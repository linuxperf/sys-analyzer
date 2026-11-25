use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use sys_analyzer_shared::{
    grpc::{RegisterRequest, RegisterResponse, agent_registration_client::AgentRegistrationClient},
    parse_controller_url,
};
use tonic::transport::Endpoint;
use uuid::Uuid;

pub fn registration_request(
    agent_id: Option<String>,
    labels: Vec<String>,
    metadata: BTreeMap<String, String>,
) -> RegisterRequest {
    RegisterRequest {
        agent_id: agent_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        hostname: rustix::system::uname()
            .nodename()
            .to_string_lossy()
            .into_owned(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        labels,
        metadata,
    }
}

pub async fn register(controller: &str, request: RegisterRequest) -> Result<RegisterResponse> {
    let channel = Endpoint::from(parse_controller_url(controller)?)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .with_context(|| format!("connect to Controller at {controller}"))?;
    let response = AgentRegistrationClient::new(channel)
        .register(request)
        .await?;
    Ok(response.into_inner())
}
