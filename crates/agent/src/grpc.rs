use std::{error::Error, io, iter, time::Duration};

use anyhow::{Result, anyhow};
use chrono::Utc;
use sys_analyzer_shared::{
    grpc::{
        DeregisterRequest, HeartbeatDirective, HeartbeatRequest, HeartbeatResponse,
        RegisterRequest, agent_registration_client::AgentRegistrationClient,
    },
    parse_controller_url,
};
use tonic::{
    Code, Response, Status,
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

use super::{Controller, Heartbeat, Registration, RpcError, Session};

pub(super) struct GrpcController {
    client: AgentRegistrationClient<Channel>,
}

impl GrpcController {
    pub(super) fn new(address: &str) -> Result<Self> {
        let endpoint = Endpoint::from(parse_controller_url(address)?)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5));
        Ok(Self {
            client: AgentRegistrationClient::new(endpoint.connect_lazy()),
        })
    }
}

impl Controller for GrpcController {
    async fn register(
        &mut self,
        agent: &Registration,
        attempt_id: Uuid,
    ) -> Result<Session, RpcError> {
        let response = self
            .client
            .register(RegisterRequest {
                agent_id: agent.agent_id.clone(),
                hostname: agent.hostname.clone(),
                pid: agent.pid,
                version: agent.version.to_owned(),
                labels: agent.labels.clone(),
                metadata: agent.metadata.clone(),
                registration_attempt_id: attempt_id.to_string(),
            })
            .await
            .map_err(|status| {
                classify_error(status, || format!("register Agent {:?}", agent.agent_id))
            })?
            .into_inner();

        Ok(Session {
            id: response.session_id,
            heartbeat_interval: Duration::from_millis(response.heartbeat_interval_ms),
        })
    }

    async fn heartbeat(&mut self, agent_id: &str, session_id: &str) -> Result<Heartbeat, RpcError> {
        let response = self
            .client
            .heartbeat(HeartbeatRequest {
                agent_id: agent_id.to_owned(),
                sent_at_unix_ms: Utc::now().timestamp_millis(),
                session_id: Some(session_id.to_owned()),
            })
            .await;

        heartbeat_outcome(agent_id, response)
    }

    async fn deregister(
        &mut self,
        agent_id: &str,
        session_id: &str,
        reason: &str,
    ) -> Result<(), RpcError> {
        match self
            .client
            .deregister(DeregisterRequest {
                agent_id: agent_id.to_owned(),
                reason: reason.to_owned(),
                session_id: session_id.to_owned(),
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == Code::NotFound => Ok(()),
            Err(status) => Err(classify_error(status, || {
                format!("deregister Agent {agent_id:?}")
            })),
        }
    }
}

fn heartbeat_outcome(
    agent_id: &str,
    response: Result<Response<HeartbeatResponse>, Status>,
) -> Result<Heartbeat, RpcError> {
    let directive = match response {
        Ok(response) => response.into_inner().directive,
        // Older Controllers signal a missing registration with NotFound.
        Err(status) if status.code() == Code::NotFound => return Ok(Heartbeat::Reregister),
        Err(status) => {
            return Err(classify_error(status, || {
                format!("send heartbeat for Agent {agent_id:?}")
            }));
        }
    };

    match HeartbeatDirective::try_from(directive) {
        Ok(HeartbeatDirective::Continue) => Ok(Heartbeat::Continue),
        Ok(HeartbeatDirective::Reregister) => Ok(Heartbeat::Reregister),
        Ok(HeartbeatDirective::Stop) => Ok(Heartbeat::Stop),
        _ => Err(RpcError::Fatal(anyhow!(
            "Controller returned unexpected heartbeat directive {directive} for Agent {agent_id:?}"
        ))),
    }
}

fn classify_error(status: Status, context: impl FnOnce() -> String) -> RpcError {
    if is_retryable_rpc_error(&status) {
        RpcError::Retryable(status.into())
    } else {
        RpcError::Fatal(anyhow::Error::new(status).context(context()))
    }
}

fn is_retryable_rpc_error(status: &Status) -> bool {
    // Tonic reports a local RPC timeout as Cancelled.
    matches!(
        status.code(),
        Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable
    ) || iter::successors(status.source(), |&source| source.source()).any(|source| {
        // Tonic can report local socket failures as Unknown or Internal.
        source.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_not_found_heartbeat_requests_reregistration() {
        let outcome = heartbeat_outcome("test-agent", Err(Status::not_found("unknown Agent")));
        assert!(matches!(outcome, Ok(Heartbeat::Reregister)));
    }

    #[test]
    fn unexpected_heartbeat_directive_is_fatal() {
        let response = Response::new(HeartbeatResponse {
            directive: 42,
            ..Default::default()
        });
        let Err(RpcError::Fatal(error)) = heartbeat_outcome("test-agent", Ok(response)) else {
            panic!("unexpected Controller directive should be fatal");
        };
        assert_eq!(
            error.to_string(),
            "Controller returned unexpected heartbeat directive 42 for Agent \"test-agent\""
        );
    }
}
