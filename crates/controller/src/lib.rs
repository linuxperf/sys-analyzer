use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use sys_analyzer_shared::{
    AGENTS_PATH, AgentRecord, AgentStatus,
    grpc::{
        HeartbeatDirective, HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
        agent_registration_server::{AgentRegistration, AgentRegistrationServer},
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_LEASE_TIMEOUT_MS: u64 = 15_000;

#[derive(Clone, Copy)]
struct HeartbeatSettings {
    interval_ms: u64,
    lease_timeout: TimeDelta,
}

impl HeartbeatSettings {
    fn new(interval_ms: u64, lease_timeout_ms: u64) -> Result<Self> {
        ensure!(interval_ms > 0, "heartbeat interval must be positive");
        ensure!(
            lease_timeout_ms > interval_ms,
            "lease timeout must be longer than the heartbeat interval"
        );
        let settings = Self {
            interval_ms,
            lease_timeout: TimeDelta::from_std(Duration::from_millis(lease_timeout_ms))
                .context("lease timeout is out of range")?,
        };
        settings.lease_expires_at(Utc::now())?;
        Ok(settings)
    }

    fn lease_expires_at(self, now: DateTime<Utc>) -> Result<DateTime<Utc>, Status> {
        now.checked_add_signed(self.lease_timeout)
            .ok_or_else(|| Status::out_of_range("lease expiration is out of range"))
    }

    fn lease_timeout_ms(self) -> u64 {
        self.lease_timeout
            .num_milliseconds()
            .try_into()
            .expect("lease timeout is positive")
    }
}

#[derive(Clone)]
struct Registry {
    agents: Arc<Mutex<BTreeMap<String, RegisteredAgent>>>,
    heartbeat: HeartbeatSettings,
}

impl Registry {
    fn new(heartbeat: HeartbeatSettings) -> Self {
        Self {
            agents: Arc::default(),
            heartbeat,
        }
    }

    fn snapshot(&self) -> Vec<AgentRecord> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        agents.values().map(|agent| agent.record.clone()).collect()
    }
}

struct RegisteredAgent {
    record: AgentRecord,
    session_id: Uuid,
    registration_attempt_id: String,
    lease_expires_at: DateTime<Utc>,
}

pub struct Controller {
    rest: TcpListener,
    grpc: TcpListener,
    heartbeat: HeartbeatSettings,
}

impl Controller {
    pub async fn bind(rest_addr: SocketAddr, grpc_addr: SocketAddr) -> Result<Self> {
        Self::bind_with_heartbeat(
            rest_addr,
            grpc_addr,
            DEFAULT_HEARTBEAT_INTERVAL_MS,
            DEFAULT_LEASE_TIMEOUT_MS,
        )
        .await
    }

    pub async fn bind_with_heartbeat(
        rest_addr: SocketAddr,
        grpc_addr: SocketAddr,
        heartbeat_interval_ms: u64,
        lease_timeout_ms: u64,
    ) -> Result<Self> {
        let heartbeat = HeartbeatSettings::new(heartbeat_interval_ms, lease_timeout_ms)?;
        let rest = TcpListener::bind(rest_addr)
            .await
            .with_context(|| format!("bind REST listener at {rest_addr}"))?;
        let grpc = TcpListener::bind(grpc_addr)
            .await
            .with_context(|| format!("bind gRPC listener at {grpc_addr}"))?;
        Ok(Self {
            rest,
            grpc,
            heartbeat,
        })
    }

    pub fn rest_addr(&self) -> io::Result<SocketAddr> {
        self.rest.local_addr()
    }

    pub fn grpc_addr(&self) -> io::Result<SocketAddr> {
        self.grpc.local_addr()
    }

    pub async fn serve(self) -> Result<()> {
        let registry = Registry::new(self.heartbeat);
        let app = Router::new()
            .route("/health", get(health))
            .route(AGENTS_PATH, get(list_agents))
            .with_state(registry.clone());

        let rest = async { axum::serve(self.rest, app).await.context("serve REST API") };
        let grpc = async {
            Server::builder()
                .add_service(AgentRegistrationServer::new(registry))
                .serve_with_incoming(TcpListenerStream::new(self.grpc))
                .await
                .context("serve gRPC API")
        };
        tokio::try_join!(rest, grpc)?;
        Ok(())
    }
}

#[tonic::async_trait]
impl AgentRegistration for Registry {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let request = request.into_inner();
        if request.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("agent_id must not be blank"));
        }

        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let registered = match agents.entry(request.agent_id.clone()) {
            Entry::Occupied(entry)
                if !request.registration_attempt_id.is_empty()
                    && request.registration_attempt_id == entry.get().registration_attempt_id =>
            {
                entry.into_mut()
            }
            Entry::Occupied(_) => {
                return Err(Status::already_exists(format!(
                    "Agent {:?} is already online",
                    request.agent_id
                )));
            }
            Entry::Vacant(entry) => {
                let registered_at = Utc::now();
                entry.insert(RegisteredAgent {
                    record: AgentRecord {
                        agent_id: request.agent_id,
                        hostname: request.hostname,
                        pid: request.pid,
                        version: request.version,
                        labels: request.labels,
                        metadata: request.metadata,
                        status: AgentStatus::Online,
                        registered_at,
                        last_heartbeat_at: None,
                    },
                    session_id: Uuid::new_v4(),
                    registration_attempt_id: request.registration_attempt_id,
                    lease_expires_at: self.heartbeat.lease_expires_at(registered_at)?,
                })
            }
        };

        Ok(Response::new(RegisterResponse {
            session_id: registered.session_id.to_string(),
            heartbeat_interval_ms: self.heartbeat.interval_ms,
            lease_timeout_ms: self.heartbeat.lease_timeout_ms(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let Some(agent) = agents
            .get_mut(&request.agent_id)
            .filter(|agent| agent.record.status == AgentStatus::Online)
        else {
            return Ok(Response::new(HeartbeatResponse {
                directive: HeartbeatDirective::Reregister.into(),
                ..Default::default()
            }));
        };

        // The Controller owns the lease clock. Agent clocks may differ from it.
        let received_at = Utc::now();
        agent.lease_expires_at = self.heartbeat.lease_expires_at(received_at)?;
        agent.record.last_heartbeat_at = Some(received_at);
        Ok(Response::new(HeartbeatResponse {
            directive: HeartbeatDirective::Continue.into(),
            lease_expires_at_unix_ms: agent.lease_expires_at.timestamp_millis(),
        }))
    }
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn list_agents(State(registry): State<Registry>) -> Json<Vec<AgentRecord>> {
    Json(registry.snapshot())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Registry {
        Registry::new(
            HeartbeatSettings::new(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS)
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn registration_returns_a_session_and_valid_lease_settings() {
        let response = registry()
            .register(Request::new(RegisterRequest {
                agent_id: "test-agent".to_owned(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            Uuid::parse_str(&response.session_id)
                .unwrap()
                .get_version_num(),
            4
        );
        assert!(response.heartbeat_interval_ms > 0);
        assert!(response.lease_timeout_ms > response.heartbeat_interval_ms);
    }

    #[tokio::test]
    async fn duplicate_registration_returns_already_exists() {
        let registry = registry();
        let request = RegisterRequest {
            agent_id: "test-agent".to_owned(),
            ..Default::default()
        };
        registry
            .register(Request::new(request.clone()))
            .await
            .unwrap();

        let error = registry.register(Request::new(request)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::AlreadyExists);
    }
}
