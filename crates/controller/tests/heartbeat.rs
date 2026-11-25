use std::time::Duration;

use chrono::Utc;
use sys_analyzer_controller::Controller;
use sys_analyzer_shared::grpc::{
    HeartbeatDirective, HeartbeatRequest, RegisterRequest,
    agent_registration_client::AgentRegistrationClient,
};
use tonic::transport::Endpoint;

#[tokio::test]
async fn registered_agent_heartbeats_renew_the_configured_lease() {
    let loopback = "127.0.0.1:0".parse().unwrap();
    let controller = Controller::bind_with_heartbeat(loopback, loopback, 25, 1_000)
        .await
        .unwrap();
    let endpoint = Endpoint::from_shared(format!("http://{}", controller.grpc_addr().unwrap()))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5));
    let server = tokio::spawn(controller.serve());
    let mut client = AgentRegistrationClient::connect(endpoint).await.unwrap();
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
    for sent_at_unix_ms in [-1, i64::MAX] {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let before = Utc::now().timestamp_millis();
        let response = client
            .heartbeat(HeartbeatRequest {
                agent_id: "test-agent".to_owned(),
                sent_at_unix_ms,
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

    server.abort();
}
