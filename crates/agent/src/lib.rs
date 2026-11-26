use std::{
    collections::BTreeMap,
    pin::{Pin, pin},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use tokio::{
    signal::unix::{SignalKind, signal},
    time::{Instant, MissedTickBehavior, interval_at, sleep, timeout},
};
use uuid::Uuid;

mod grpc;

#[cfg(test)]
mod tests;

const CONTROLLER_RETRY_DELAY: Duration = Duration::from_secs(3);
const SHUTDOWN_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const DEREGISTER_RETRY_DELAY: Duration = Duration::from_millis(100);

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
    Stop,
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

    async fn heartbeat(&mut self, agent_id: &str, session_id: &str) -> Result<Heartbeat, RpcError>;

    async fn deregister(
        &mut self,
        agent_id: &str,
        session_id: &str,
        reason: &str,
    ) -> Result<(), RpcError>;
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
    let mut sigint = signal(SignalKind::interrupt()).context("listen for SIGINT")?;
    let mut sigterm = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    let shutdown = async {
        tokio::select! {
            _ = sigint.recv() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        }
    };
    maintain_registration(&mut controller, controller_addr, &agent, shutdown).await
}

enum AttemptEnd {
    Reregister,
    Stop,
    Shutdown {
        reason: &'static str,
        session: Option<Session>,
    },
}

async fn maintain_registration(
    controller: &mut impl Controller,
    controller_addr: &str,
    agent: &Registration,
    shutdown: impl Future<Output = &'static str>,
) -> Result<()> {
    let mut shutdown = pin!(shutdown);
    loop {
        let attempt_id = Uuid::new_v4();
        match run_attempt(
            controller,
            controller_addr,
            agent,
            attempt_id,
            shutdown.as_mut(),
        )
        .await?
        {
            AttemptEnd::Reregister => {}
            AttemptEnd::Stop => return Ok(()),
            AttemptEnd::Shutdown { reason, session } => {
                shutdown_registration(controller, agent, attempt_id, session, reason).await;
                return Ok(());
            }
        }
    }
}

async fn run_attempt(
    controller: &mut impl Controller,
    controller_addr: &str,
    agent: &Registration,
    attempt_id: Uuid,
    mut shutdown: Pin<&mut impl Future<Output = &'static str>>,
) -> Result<AttemptEnd> {
    let agent_id = &agent.agent_id;
    let session = loop {
        // Await an in-flight Register so shutdown can use its session ID. Retrying
        // with the same attempt ID can retrieve a response already committed.
        match controller.register(agent, attempt_id).await {
            Ok(session) => break session,
            Err(RpcError::Retryable(error)) => {
                eprintln!(
                    "Registration for Agent {agent_id:?} at {controller_addr} failed and will retry ({error})"
                );
                tokio::select! {
                    biased;
                    reason = &mut shutdown => {
                        return Ok(AttemptEnd::Shutdown { reason, session: None });
                    }
                    _ = sleep(CONTROLLER_RETRY_DELAY) => {}
                }
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
        let heartbeat = tokio::select! {
            biased;
            reason = &mut shutdown => {
                return Ok(AttemptEnd::Shutdown { reason, session: Some(session) });
            }
            result = async {
                ticks.tick().await;
                controller.heartbeat(agent_id, &session.id).await
            } => result,
        };
        match heartbeat {
            Ok(Heartbeat::Continue) => {}
            Ok(Heartbeat::Reregister) => return Ok(AttemptEnd::Reregister),
            Ok(Heartbeat::Stop) => {
                println!("Controller requested stop for Agent {agent_id:?}");
                return Ok(AttemptEnd::Stop);
            }
            Err(RpcError::Retryable(error)) => {
                eprintln!("Heartbeat for Agent {agent_id:?} failed and will retry ({error})");
                ticks.reset_after(CONTROLLER_RETRY_DELAY);
            }
            Err(RpcError::Fatal(error)) => return Err(error),
        }
    }
}

async fn shutdown_registration(
    controller: &mut impl Controller,
    agent: &Registration,
    attempt_id: Uuid,
    session: Option<Session>,
    reason: &str,
) {
    let agent_id = &agent.agent_id;
    let cleanup = async {
        let session = match session {
            Some(session) => session,
            // A retryable registration error may mean only the response was lost.
            None => match controller.register(agent, attempt_id).await {
                Ok(session) => session,
                Err(RpcError::Retryable(error) | RpcError::Fatal(error)) => return Err(error),
            },
        };

        loop {
            match controller.deregister(agent_id, &session.id, reason).await {
                Ok(()) => return Ok(()),
                Err(RpcError::Retryable(_)) => sleep(DEREGISTER_RETRY_DELAY).await,
                Err(RpcError::Fatal(error)) => return Err(error),
            }
        }
    };
    if let Err(error) = timeout(SHUTDOWN_CLEANUP_TIMEOUT, cleanup)
        .await
        .context("shutdown cleanup timed out")
        .and_then(std::convert::identity)
    {
        eprintln!("Deregistration for Agent {agent_id:?} failed ({error:#})");
    }
}
