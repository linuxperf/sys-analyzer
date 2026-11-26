use std::collections::VecDeque;

use anyhow::anyhow;

use super::*;

#[derive(Debug)]
enum Request {
    Register(Uuid),
    Heartbeat,
}

struct ScriptedController {
    start: Instant,
    registrations: VecDeque<Result<Session, RpcError>>,
    heartbeats: VecDeque<Result<Heartbeat, RpcError>>,
    requests: Vec<(u128, Request)>,
}

impl ScriptedController {
    fn new(
        registrations: impl IntoIterator<Item = Result<Session, RpcError>>,
        heartbeats: impl IntoIterator<Item = Result<Heartbeat, RpcError>>,
    ) -> Self {
        Self {
            start: Instant::now(),
            registrations: registrations.into_iter().collect(),
            heartbeats: heartbeats.into_iter().collect(),
            requests: Vec::new(),
        }
    }
}

impl Controller for ScriptedController {
    async fn register(
        &mut self,
        agent: &Registration,
        attempt_id: Uuid,
    ) -> Result<Session, RpcError> {
        assert_eq!(agent.agent_id, "test-agent");
        self.requests.push((
            self.start.elapsed().as_millis(),
            Request::Register(attempt_id),
        ));
        self.registrations
            .pop_front()
            .expect("unexpected registration")
    }

    async fn heartbeat(&mut self, agent_id: &str) -> Result<Heartbeat, RpcError> {
        assert_eq!(agent_id, "test-agent");
        self.requests
            .push((self.start.elapsed().as_millis(), Request::Heartbeat));
        self.heartbeats.pop_front().expect("unexpected heartbeat")
    }

    async fn deregister(
        &mut self,
        _agent_id: &str,
        _session_id: &str,
        _reason: &str,
    ) -> Result<()> {
        panic!("unexpected deregistration")
    }
}

fn agent() -> Registration {
    Registration {
        agent_id: "test-agent".to_owned(),
        hostname: "localhost".to_owned(),
        pid: 1,
        version: env!("CARGO_PKG_VERSION"),
        labels: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

#[tokio::test(start_paused = true)]
async fn retries_registration_and_heartbeats_then_uses_the_new_session_interval() {
    let mut controller = ScriptedController::new(
        [
            Err(RpcError::Retryable(anyhow!("lost registration reply"))),
            Ok(Session {
                id: "original-session".to_owned(),
                heartbeat_interval: Duration::from_millis(100),
            }),
            Ok(Session {
                id: "new-session".to_owned(),
                heartbeat_interval: Duration::from_millis(50),
            }),
        ],
        [
            Ok(Heartbeat::Continue),
            Err(RpcError::Retryable(anyhow!("heartbeat timed out"))),
            Ok(Heartbeat::Reregister),
            Err(RpcError::Fatal(anyhow!("terminal heartbeat error"))),
        ],
    );

    let error = maintain_registration(
        &mut controller,
        "controller",
        &agent(),
        std::future::pending(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "terminal heartbeat error");
    let [
        (0, Request::Register(first)),
        (3000, Request::Register(retry)),
        (3100, Request::Heartbeat),
        (3200, Request::Heartbeat),
        (6200, Request::Heartbeat),
        (6200, Request::Register(new_attempt)),
        (6250, Request::Heartbeat),
    ] = controller.requests.as_slice()
    else {
        panic!("unexpected requests: {:?}", controller.requests);
    };
    assert_eq!(first, retry);
    assert_ne!(first, new_attempt);
}

#[tokio::test(start_paused = true)]
async fn fatal_registration_error_stops_before_heartbeat() {
    let mut controller =
        ScriptedController::new([Err(RpcError::Fatal(anyhow!("registration rejected")))], []);

    let error = maintain_registration(
        &mut controller,
        "controller",
        &agent(),
        std::future::pending(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "registration rejected");
    assert!(matches!(
        controller.requests.as_slice(),
        [(0, Request::Register(_))]
    ));
}

#[tokio::test(start_paused = true)]
async fn zero_heartbeat_interval_stops_before_heartbeat() {
    let mut controller = ScriptedController::new(
        [Ok(Session {
            id: "invalid-session".to_owned(),
            heartbeat_interval: Duration::ZERO,
        })],
        [],
    );

    let error = maintain_registration(
        &mut controller,
        "controller",
        &agent(),
        std::future::pending(),
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Controller returned a zero heartbeat interval"
    );
    assert!(matches!(
        controller.requests.as_slice(),
        [(0, Request::Register(_))]
    ));
}
