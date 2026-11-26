use std::collections::VecDeque;

use anyhow::anyhow;

use super::*;

#[derive(Debug, PartialEq, Eq)]
enum Request {
    Register(Uuid),
    Heartbeat(String),
    Deregister { session_id: String, reason: String },
}

enum Deregister {
    Success,
    Retry,
    NoReply,
}

struct ScriptedController {
    start: Instant,
    registrations: VecDeque<Result<Session, RpcError>>,
    heartbeats: VecDeque<Result<Heartbeat, RpcError>>,
    deregistrations: VecDeque<Deregister>,
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
            deregistrations: VecDeque::new(),
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

    async fn heartbeat(&mut self, agent_id: &str, session_id: &str) -> Result<Heartbeat, RpcError> {
        assert_eq!(agent_id, "test-agent");
        self.requests.push((
            self.start.elapsed().as_millis(),
            Request::Heartbeat(session_id.to_owned()),
        ));
        self.heartbeats.pop_front().expect("unexpected heartbeat")
    }

    async fn deregister(
        &mut self,
        agent_id: &str,
        session_id: &str,
        reason: &str,
    ) -> Result<(), RpcError> {
        assert_eq!(agent_id, "test-agent");
        self.requests.push((
            self.start.elapsed().as_millis(),
            Request::Deregister {
                session_id: session_id.to_owned(),
                reason: reason.to_owned(),
            },
        ));
        match self
            .deregistrations
            .pop_front()
            .expect("unexpected deregistration")
        {
            Deregister::Success => Ok(()),
            Deregister::Retry => Err(RpcError::Retryable(anyhow!("connection reset"))),
            Deregister::NoReply => std::future::pending().await,
        }
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
        (3100, Request::Heartbeat(first_heartbeat)),
        (3200, Request::Heartbeat(second_heartbeat)),
        (6200, Request::Heartbeat(retry_heartbeat)),
        (6200, Request::Register(new_attempt)),
        (6250, Request::Heartbeat(new_heartbeat)),
    ] = controller.requests.as_slice()
    else {
        panic!("unexpected requests: {:?}", controller.requests);
    };
    assert_eq!(first, retry);
    assert_ne!(first, new_attempt);
    assert_eq!(first_heartbeat, "original-session");
    assert_eq!(second_heartbeat, "original-session");
    assert_eq!(retry_heartbeat, "original-session");
    assert_eq!(new_heartbeat, "new-session");
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

#[tokio::test(start_paused = true)]
async fn shutdown_replays_the_uncertain_attempt_and_deregisters_its_session() {
    let mut controller = ScriptedController::new(
        [
            Err(RpcError::Retryable(anyhow!("lost registration reply"))),
            Ok(Session {
                id: "replayed-session".to_owned(),
                heartbeat_interval: Duration::from_secs(1),
            }),
        ],
        [],
    );
    controller
        .deregistrations
        .extend([Deregister::Retry, Deregister::Success]);

    maintain_registration(&mut controller, "controller", &agent(), async {
        sleep(Duration::from_millis(50)).await;
        "SIGTERM"
    })
    .await
    .unwrap();

    let [
        (0, Request::Register(first)),
        (50, Request::Register(replay)),
        (50, deregister),
        (150, retry),
    ] = controller.requests.as_slice()
    else {
        panic!("unexpected requests: {:?}", controller.requests);
    };
    assert_eq!(first, replay);
    let request = Request::Deregister {
        session_id: "replayed-session".to_owned(),
        reason: "SIGTERM".to_owned(),
    };
    assert_eq!(deregister, &request);
    assert_eq!(retry, &request);
}

#[tokio::test(start_paused = true)]
async fn shutdown_of_a_registered_session_bounds_an_unanswered_deregistration() {
    let mut controller = ScriptedController::new(
        [Ok(Session {
            id: "active-session".to_owned(),
            heartbeat_interval: Duration::from_secs(1),
        })],
        [],
    );
    controller.deregistrations.push_back(Deregister::NoReply);

    maintain_registration(&mut controller, "controller", &agent(), async {
        sleep(Duration::from_millis(50)).await;
        "SIGINT"
    })
    .await
    .unwrap();

    assert_eq!(controller.start.elapsed(), Duration::from_millis(2050));
    assert!(matches!(controller.requests[0], (0, Request::Register(_))));
    assert_eq!(
        controller.requests[1..],
        [(
            50,
            Request::Deregister {
                session_id: "active-session".to_owned(),
                reason: "SIGINT".to_owned(),
            }
        )]
    );
}
