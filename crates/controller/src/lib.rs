use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::get};
use chrono::Utc;
use serde::Serialize;
use sys_analyzer_shared::{
    AGENTS_PATH, AgentRecord, AgentStatus,
    grpc::{
        RegisterRequest, RegisterResponse,
        agent_registration_server::{AgentRegistration, AgentRegistrationServer},
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

const HEARTBEAT_INTERVAL_MS: u64 = 5_000;
const LEASE_TIMEOUT_MS: u64 = 15_000;

#[derive(Clone, Default)]
struct Registry {
    agents: Arc<Mutex<BTreeMap<String, RegisteredAgent>>>,
}

impl Registry {
    fn snapshot(&self) -> Vec<AgentRecord> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        agents.values().map(|agent| agent.record.clone()).collect()
    }
}

struct RegisteredAgent {
    record: AgentRecord,
    session_id: Uuid,
}

pub struct Controller {
    rest: TcpListener,
    grpc: TcpListener,
}

impl Controller {
    pub async fn bind(rest_addr: SocketAddr, grpc_addr: SocketAddr) -> Result<Self> {
        let rest = TcpListener::bind(rest_addr)
            .await
            .with_context(|| format!("bind REST listener at {rest_addr}"))?;
        let grpc = TcpListener::bind(grpc_addr)
            .await
            .with_context(|| format!("bind gRPC listener at {grpc_addr}"))?;
        Ok(Self { rest, grpc })
    }

    pub fn rest_addr(&self) -> io::Result<SocketAddr> {
        self.rest.local_addr()
    }

    pub fn grpc_addr(&self) -> io::Result<SocketAddr> {
        self.grpc.local_addr()
    }

    pub async fn serve(self) -> Result<()> {
        let registry = Registry::default();
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
        let Entry::Vacant(entry) = agents.entry(request.agent_id.clone()) else {
            return Err(Status::already_exists(format!(
                "Agent {:?} is already online",
                request.agent_id
            )));
        };

        let registered = entry.insert(RegisteredAgent {
            record: AgentRecord {
                agent_id: request.agent_id,
                hostname: request.hostname,
                pid: request.pid,
                version: request.version,
                labels: request.labels,
                metadata: request.metadata,
                status: AgentStatus::Online,
                registered_at: Utc::now(),
            },
            session_id: Uuid::new_v4(),
        });

        Ok(Response::new(RegisterResponse {
            session_id: registered.session_id.to_string(),
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
            lease_timeout_ms: LEASE_TIMEOUT_MS,
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

    #[tokio::test]
    async fn registration_returns_a_session_and_valid_lease_settings() {
        let response = Registry::default()
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
        let registry = Registry::default();
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
