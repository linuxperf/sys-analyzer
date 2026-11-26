use std::{io, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::get};
use chrono::Utc;
use serde::Serialize;
use sys_analyzer_shared::{
    AGENTS_PATH, AgentRecord,
    grpc::{
        DeregisterRequest, DeregisterResponse, HeartbeatDirective, HeartbeatRequest,
        HeartbeatResponse, RegisterRequest, RegisterResponse,
        agent_registration_server::{AgentRegistration, AgentRegistrationServer},
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

mod registry;

use registry::{Heartbeat, HeartbeatSettings, Registration, Registry, RegistryError};

pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_LEASE_TIMEOUT_MS: u64 = 15_000;

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
        heartbeat
            .lease_expires_at(Utc::now())
            .map_err(registry_status)?;
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
        let registry = Arc::new(Registry::new(self.heartbeat, Utc::now));
        let app = Router::new()
            .route("/health", get(health))
            .route(AGENTS_PATH, get(list_agents))
            .with_state(registry.clone());

        let rest = async { axum::serve(self.rest, app).await.context("serve REST API") };
        let grpc = async {
            Server::builder()
                .add_service(AgentRegistrationServer::new(GrpcRegistry(registry)))
                .serve_with_incoming(TcpListenerStream::new(self.grpc))
                .await
                .context("serve gRPC API")
        };
        tokio::try_join!(rest, grpc)?;
        Ok(())
    }
}

struct GrpcRegistry(Arc<Registry>);

#[tonic::async_trait]
impl AgentRegistration for GrpcRegistry {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let request = request.into_inner();
        let session = self
            .0
            .register(Registration {
                agent_id: request.agent_id,
                hostname: request.hostname,
                pid: request.pid,
                version: request.version,
                labels: request.labels,
                metadata: request.metadata,
                attempt_id: request.registration_attempt_id,
            })
            .map_err(registry_status)?;

        Ok(Response::new(RegisterResponse {
            session_id: session.id.to_string(),
            heartbeat_interval_ms: session.heartbeat_interval_ms,
            lease_timeout_ms: session.lease_timeout_ms,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let heartbeat = self
            .0
            .heartbeat(&request.into_inner().agent_id)
            .map_err(registry_status)?;
        let response = match heartbeat {
            Heartbeat::Renewed { lease_expires_at } => HeartbeatResponse {
                directive: HeartbeatDirective::Continue.into(),
                lease_expires_at_unix_ms: lease_expires_at.timestamp_millis(),
            },
            Heartbeat::Reregister => HeartbeatResponse {
                directive: HeartbeatDirective::Reregister.into(),
                ..Default::default()
            },
        };
        Ok(Response::new(response))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let request = request.into_inner();
        self.0
            .deregister(&request.agent_id, &request.session_id)
            .map_err(registry_status)?;
        println!(
            "Agent {:?} deregistered ({:?})",
            request.agent_id, request.reason
        );
        Ok(Response::new(DeregisterResponse {}))
    }
}

fn registry_status(error: RegistryError) -> Status {
    match error {
        RegistryError::BlankAgentId => Status::invalid_argument("agent_id must not be blank"),
        RegistryError::AlreadyRegistered(agent_id) => {
            Status::already_exists(format!("Agent {agent_id:?} is already online"))
        }
        RegistryError::LeaseExpirationOutOfRange => {
            Status::out_of_range("lease expiration is out of range")
        }
        RegistryError::InvalidSessionId => Status::invalid_argument("session_id must be a UUID"),
        RegistryError::UnknownSession(agent_id) => {
            Status::not_found(format!("No matching session for Agent {agent_id:?}"))
        }
    }
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn list_agents(State(registry): State<Arc<Registry>>) -> Json<Vec<AgentRecord>> {
    Json(registry.snapshot())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn grpc_registry() -> GrpcRegistry {
        let heartbeat =
            HeartbeatSettings::new(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS)
                .unwrap();
        GrpcRegistry(Arc::new(Registry::new(heartbeat, Utc::now)))
    }

    #[tokio::test]
    async fn registration_returns_a_session_and_valid_lease_settings() {
        let response = grpc_registry()
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
        let registry = grpc_registry();
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
