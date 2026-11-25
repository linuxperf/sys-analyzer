use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use sys_analyzer_shared::{
    grpc::{
        HeartbeatDirective, HeartbeatRequest, RegisterRequest, RegisterResponse,
        agent_registration_client::AgentRegistrationClient,
    },
    parse_controller_url,
};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tonic::{
    Code,
    transport::{Channel, Endpoint},
};
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
    let (_, session) = register_with_client(controller, request).await?;
    Ok(session)
}

async fn register_with_client(
    controller: &str,
    request: RegisterRequest,
) -> Result<(AgentRegistrationClient<Channel>, RegisterResponse)> {
    let channel = Endpoint::from(parse_controller_url(controller)?)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .with_context(|| format!("connect to Controller at {controller}"))?;
    let mut client = AgentRegistrationClient::new(channel);
    let response = client.register(request).await?.into_inner();
    Ok((client, response))
}

pub async fn run(controller: &str, request: RegisterRequest) -> Result<()> {
    let agent_id = request.agent_id.clone();
    let (mut client, session) = register_with_client(controller, request)
        .await
        .with_context(|| format!("register Agent {agent_id:?}"))?;
    ensure!(
        session.heartbeat_interval_ms > 0,
        "Controller returned a zero heartbeat interval"
    );
    let period = Duration::from_millis(session.heartbeat_interval_ms);
    let mut ticks = interval_at(Instant::now() + period, period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    println!(
        "Agent {agent_id:?} registered with session {}",
        session.session_id
    );

    loop {
        ticks.tick().await;
        let response = client
            .heartbeat(HeartbeatRequest {
                agent_id: agent_id.clone(),
                sent_at_unix_ms: Utc::now().timestamp_millis(),
            })
            .await;
        let response = match response {
            Ok(response) => response.into_inner(),
            // Tonic reports a local RPC timeout as Cancelled.
            Err(error)
                if matches!(
                    error.code(),
                    Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable
                ) =>
            {
                eprintln!("Heartbeat for Agent {agent_id:?} failed and will retry ({error})");
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("send heartbeat for Agent {agent_id:?}"));
            }
        };
        ensure!(
            response.directive == i32::from(HeartbeatDirective::Continue),
            "Controller returned unexpected heartbeat directive {} for Agent {agent_id:?}",
            response.directive
        );
    }
}
