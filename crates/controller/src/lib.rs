use std::{future, io, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use sys_analyzer_shared::{
    AGENTS_PATH, AgentDetail, AgentRecord, AgentStatus, ErrorBody,
    grpc::{
        DeregisterRequest, DeregisterResponse, HeartbeatDirective, HeartbeatRequest,
        HeartbeatResponse, RegisterRequest, RegisterResponse,
        agent_registration_server::{AgentRegistration, AgentRegistrationServer},
    },
};
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

mod registry;

use registry::{
    Heartbeat, HeartbeatSettings, ManagementAction, ManagementError, Registration, Registry,
    RegistryError,
};

pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_LEASE_TIMEOUT_MS: u64 = 15_000;
pub const DEFAULT_OFFLINE_RETENTION_MS: u64 = 60_000;
pub const DEFAULT_SWEEP_INTERVAL_MS: u64 = 1_000;

pub struct LifecycleSettings {
    pub heartbeat_interval_ms: u64,
    pub lease_timeout_ms: u64,
    pub offline_retention_ms: u64,
    pub sweep_interval_ms: u64,
}

impl Default for LifecycleSettings {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            lease_timeout_ms: DEFAULT_LEASE_TIMEOUT_MS,
            offline_retention_ms: DEFAULT_OFFLINE_RETENTION_MS,
            sweep_interval_ms: DEFAULT_SWEEP_INTERVAL_MS,
        }
    }
}

pub struct Controller {
    rest: TcpListener,
    grpc: TcpListener,
    heartbeat: HeartbeatSettings,
    offline_retention: TimeDelta,
    sweep_interval: Duration,
}

impl Controller {
    pub async fn bind(rest_addr: SocketAddr, grpc_addr: SocketAddr) -> Result<Self> {
        Self::bind_with_lifecycle(rest_addr, grpc_addr, LifecycleSettings::default()).await
    }

    pub async fn bind_with_heartbeat(
        rest_addr: SocketAddr,
        grpc_addr: SocketAddr,
        heartbeat_interval_ms: u64,
        lease_timeout_ms: u64,
    ) -> Result<Self> {
        Self::bind_with_lifecycle(
            rest_addr,
            grpc_addr,
            LifecycleSettings {
                heartbeat_interval_ms,
                lease_timeout_ms,
                ..LifecycleSettings::default()
            },
        )
        .await
    }

    pub async fn bind_with_lifecycle(
        rest_addr: SocketAddr,
        grpc_addr: SocketAddr,
        settings: LifecycleSettings,
    ) -> Result<Self> {
        let heartbeat =
            HeartbeatSettings::new(settings.heartbeat_interval_ms, settings.lease_timeout_ms)?;
        heartbeat
            .lease_expires_at(Utc::now())
            .map_err(registry_status)?;
        ensure!(
            settings.offline_retention_ms > 0,
            "offline retention must be positive"
        );
        ensure!(
            settings.sweep_interval_ms > 0,
            "sweep interval must be positive"
        );
        let offline_retention =
            TimeDelta::from_std(Duration::from_millis(settings.offline_retention_ms))
                .context("offline retention is out of range")?;
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
            offline_retention,
            sweep_interval: Duration::from_millis(settings.sweep_interval_ms),
        })
    }

    pub fn rest_addr(&self) -> io::Result<SocketAddr> {
        self.rest.local_addr()
    }

    pub fn grpc_addr(&self) -> io::Result<SocketAddr> {
        self.grpc.local_addr()
    }

    pub async fn serve(self) -> Result<()> {
        self.serve_with_shutdown(future::pending()).await
    }

    pub async fn serve_with_shutdown(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        let registry = Arc::new(Registry::new(
            self.heartbeat,
            self.offline_retention,
            Utc::now,
        ));
        let app = Router::new()
            .route("/health", get(health))
            .route(AGENTS_PATH, get(list_agents))
            .route("/agents/{agent_id}", get(agent_detail).delete(remove_agent))
            .route("/agents/{agent_id}/disable", post(disable_agent))
            .route("/agents/{agent_id}/enable", post(enable_agent))
            .with_state(registry.clone());

        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let rest_shutdown = wait_for_shutdown(shutdown_receiver.clone());
        let grpc_shutdown = wait_for_shutdown(shutdown_receiver.clone());
        let sweep_shutdown = wait_for_shutdown(shutdown_receiver);

        let sweep_registry = Arc::clone(&registry);
        let sweep = async move {
            let mut ticks = tokio::time::interval(self.sweep_interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tokio::pin!(sweep_shutdown);
            loop {
                tokio::select! {
                    () = &mut sweep_shutdown => return Ok(()),
                    _ = ticks.tick() => sweep_registry.sweep(),
                }
            }
        };

        let rest = async {
            axum::serve(self.rest, app)
                .with_graceful_shutdown(rest_shutdown)
                .await
                .context("serve REST API")
        };
        let grpc = async {
            Server::builder()
                .add_service(AgentRegistrationServer::new(GrpcRegistry(registry)))
                .serve_with_incoming_shutdown(TcpListenerStream::new(self.grpc), grpc_shutdown)
                .await
                .context("serve gRPC API")
        };
        let stop = async move {
            shutdown.await;
            drop(shutdown_sender);
            Ok(())
        };
        tokio::try_join!(rest, grpc, sweep, stop)?;
        Ok(())
    }
}

async fn wait_for_shutdown(mut listener: watch::Receiver<()>) {
    // The sender is dropped once shutdown begins, waking current and future waiters.
    let _ = listener.changed().await;
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
        let request = request.into_inner();
        let heartbeat = self
            .0
            .heartbeat(&request.agent_id, request.session_id.as_deref())
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
            Heartbeat::Stop => HeartbeatResponse {
                directive: HeartbeatDirective::Stop.into(),
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
        RegistryError::AgentDisabled(agent_id) => {
            Status::permission_denied(format!("Agent {agent_id:?} is disabled"))
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

#[derive(Deserialize)]
struct AgentListQuery {
    status: Option<AgentStatus>,
}

async fn list_agents(
    State(registry): State<Arc<Registry>>,
    Query(query): Query<AgentListQuery>,
) -> Json<Vec<AgentRecord>> {
    Json(registry.snapshot(query.status))
}

type ApiError = (StatusCode, Json<ErrorBody>);

async fn agent_detail(
    State(registry): State<Arc<Registry>>,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentDetail>, ApiError> {
    registry
        .detail(&agent_id)
        .map(Json)
        .ok_or_else(|| management_error(&agent_id, ManagementError::Missing))
}

async fn disable_agent(
    State(registry): State<Arc<Registry>>,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentDetail>, ApiError> {
    registry
        .manage(&agent_id, ManagementAction::Disable)
        .map(Json)
        .map_err(|error| management_error(&agent_id, error))
}

async fn enable_agent(
    State(registry): State<Arc<Registry>>,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentDetail>, ApiError> {
    registry
        .manage(&agent_id, ManagementAction::Enable)
        .map(Json)
        .map_err(|error| management_error(&agent_id, error))
}

async fn remove_agent(
    State(registry): State<Arc<Registry>>,
    Path(agent_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    registry
        .remove(&agent_id)
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|error| management_error(&agent_id, error))
}

fn management_error(agent_id: &str, error: ManagementError) -> ApiError {
    let (status, error) = match error {
        ManagementError::Missing => (
            StatusCode::NOT_FOUND,
            format!("Agent {agent_id:?} was not found"),
        ),
        ManagementError::AlreadyDisabled => (
            StatusCode::CONFLICT,
            format!("Agent {agent_id:?} is already disabled"),
        ),
        ManagementError::NotDisabled => (
            StatusCode::CONFLICT,
            format!("Agent {agent_id:?} is not disabled"),
        ),
    };
    (status, Json(ErrorBody { error }))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn grpc_registry() -> GrpcRegistry {
        let heartbeat =
            HeartbeatSettings::new(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS)
                .unwrap();
        GrpcRegistry(Arc::new(Registry::new(
            heartbeat,
            TimeDelta::seconds(60),
            Utc::now,
        )))
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
