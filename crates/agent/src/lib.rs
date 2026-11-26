use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep};
use uuid::Uuid;

mod grpc;

#[cfg(test)]
mod tests;

const CONTROLLER_RETRY_DELAY: Duration = Duration::from_secs(3);

struct Registration {
    agent_id: String,
    hostname: String,
    pid: u32,
    version: &'static str,
    labels: Vec<String>,
    metadata: BTreeMap<String, String>,
}

struct Session {
    id: String,
    heartbeat_interval: Duration,
}

enum Heartbeat {
    Continue,
    Reregister,
}

enum RpcError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

trait Controller {
    async fn register(
        &mut self,
        agent: &Registration,
        attempt_id: Uuid,
    ) -> Result<Session, RpcError>;

    async fn heartbeat(&mut self, agent_id: &str) -> Result<Heartbeat, RpcError>;
}

pub async fn run(
    controller_addr: &str,
    agent_id: Option<String>,
    labels: Vec<String>,
    metadata: BTreeMap<String, String>,
) -> Result<()> {
    let agent = Registration {
        agent_id: agent_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        hostname: rustix::system::uname()
            .nodename()
            .to_string_lossy()
            .into_owned(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION"),
        labels,
        metadata,
    };
    let mut controller = grpc::GrpcController::new(controller_addr)
        .with_context(|| format!("register Agent {:?}", agent.agent_id))?;
    maintain_registration(&mut controller, controller_addr, &agent).await
}

async fn maintain_registration(
    controller: &mut impl Controller,
    controller_addr: &str,
    agent: &Registration,
) -> Result<()> {
    let agent_id = &agent.agent_id;

    loop {
        // Retries of this registration can replay a committed Controller response.
        let attempt_id = Uuid::new_v4();
        let session = loop {
            match controller.register(agent, attempt_id).await {
                Ok(session) => break session,
                Err(RpcError::Retryable(error)) => {
                    eprintln!(
                        "Registration for Agent {agent_id:?} at {controller_addr} failed and will retry ({error})"
                    );
                    sleep(CONTROLLER_RETRY_DELAY).await;
                }
                Err(RpcError::Fatal(error)) => return Err(error),
            }
        };

        let period = session.heartbeat_interval;
        ensure!(
            !period.is_zero(),
            "Controller returned a zero heartbeat interval"
        );
        println!("Agent {agent_id:?} registered with session {}", session.id);

        let mut ticks = interval_at(Instant::now() + period, period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            match controller.heartbeat(agent_id).await {
                Ok(Heartbeat::Continue) => {}
                Ok(Heartbeat::Reregister) => break,
                Err(RpcError::Retryable(error)) => {
                    eprintln!("Heartbeat for Agent {agent_id:?} failed and will retry ({error})");
                    ticks.reset_after(CONTROLLER_RETRY_DELAY);
                }
                Err(RpcError::Fatal(error)) => return Err(error),
            }
        }
    }
}
