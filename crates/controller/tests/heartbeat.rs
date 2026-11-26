use std::time::Duration;

use chrono::Utc;
use sys_analyzer_controller::Controller;
use sys_analyzer_shared::grpc::{
    HeartbeatDirective, HeartbeatRequest, RegisterRequest,
    agent_registration_client::AgentRegistrationClient,
};
use tonic::{
    Code,
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

async fn start_controller() -> AgentRegistrationClient<Channel> {
    let loopback = "127.0.0.1:0".parse().unwrap();
    let controller = Controller::bind_with_heartbeat(loopback, loopback, 25, 1_000)
        .await
        .unwrap();
    let endpoint = Endpoint::from_shared(format!("http://{}", controller.grpc_addr().unwrap()))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5));
    tokio::spawn(controller.serve());
    AgentRegistrationClient::connect(endpoint).await.unwrap()
}

#[tokio::test]
async fn registered_agent_heartbeats_renew_the_configured_lease() {
    let mut client = start_controller().await;
    let registration = client
        .register(RegisterRequest {
            agent_id: "test-agent".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registration.heartbeat_interval_ms, 25);
    assert_eq!(registration.lease_timeout_ms, 1_000);

    let mut previous_expiry = 0;
    for (sent_at_unix_ms, session_id) in [(-1, None), (i64::MAX, Some(registration.session_id))] {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let before = Utc::now().timestamp_millis();
        let response = client
            .heartbeat(HeartbeatRequest {
                agent_id: "test-agent".to_owned(),
                sent_at_unix_ms,
                session_id,
            })
            .await
            .unwrap()
            .into_inner();
        let after = Utc::now().timestamp_millis();

        assert_eq!(response.directive(), HeartbeatDirective::Continue);
        assert!((before + 1_000..=after + 1_000).contains(&response.lease_expires_at_unix_ms));
        assert!(response.lease_expires_at_unix_ms > previous_expiry);
        previous_expiry = response.lease_expires_at_unix_ms;
    }
}

#[tokio::test]
async fn unknown_agent_heartbeat_requests_reregistration() {
    let mut client = start_controller().await;
    let response = client
        .heartbeat(HeartbeatRequest {
            agent_id: "unknown-agent".to_owned(),
            sent_at_unix_ms: Utc::now().timestamp_millis(),
            session_id: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.directive(), HeartbeatDirective::Reregister);
    assert_eq!(response.lease_expires_at_unix_ms, 0);
}

#[tokio::test]
async fn replaying_registration_returns_the_original_session() {
    let mut client = start_controller().await;
    let request = RegisterRequest {
        agent_id: "test-agent".to_owned(),
        registration_attempt_id: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let original = client.register(request.clone()).await.unwrap().into_inner();
    let replay = client.register(request.clone()).await.unwrap().into_inner();
    assert_eq!(replay, original);

    let conflict = client
        .register(RegisterRequest {
            registration_attempt_id: Uuid::new_v4().to_string(),
            ..request
        })
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), Code::AlreadyExists);
}
