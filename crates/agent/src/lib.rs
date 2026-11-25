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
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep};
use tonic::{Code, transport::Endpoint};
use uuid::Uuid;

const CONTROLLER_RETRY_DELAY: Duration = Duration::from_secs(3);

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
    let channel = controller_endpoint(controller)?
        .connect()
        .await
        .with_context(|| format!("connect to Controller at {controller}"))?;
    let response = AgentRegistrationClient::new(channel)
        .register(request)
        .await?;
    Ok(response.into_inner())
}

fn controller_endpoint(controller: &str) -> Result<Endpoint> {
    Ok(Endpoint::from(parse_controller_url(controller)?)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5)))
}

pub async fn run(controller: &str, request: RegisterRequest) -> Result<()> {
    let agent_id = request.agent_id.clone();
    let endpoint =
        controller_endpoint(controller).with_context(|| format!("register Agent {agent_id:?}"))?;
    let mut client = AgentRegistrationClient::new(endpoint.connect_lazy());
    let session = loop {
        match client.register(request.clone()).await {
            Ok(response) => break response.into_inner(),
            Err(error) if is_retryable_rpc_error(error.code()) => {
                eprintln!(
                    "Registration for Agent {agent_id:?} at {controller} failed and will retry ({error})"
                );
                sleep(CONTROLLER_RETRY_DELAY).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("register Agent {agent_id:?}"));
            }
        }
    };
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
            Err(error) if is_retryable_rpc_error(error.code()) => {
                eprintln!("Heartbeat for Agent {agent_id:?} failed and will retry ({error})");
                ticks.reset_after(CONTROLLER_RETRY_DELAY);
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

fn is_retryable_rpc_error(code: Code) -> bool {
    // Tonic reports a local RPC timeout as Cancelled.
    matches!(
        code,
        Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable
    )
}
