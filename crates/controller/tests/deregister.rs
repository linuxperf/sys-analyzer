use std::time::Duration;

use sys_analyzer_controller::Controller;
use sys_analyzer_shared::grpc::{
    DeregisterRequest, RegisterRequest, agent_registration_client::AgentRegistrationClient,
};
use tonic::{Code, transport::Endpoint};

#[tokio::test]
async fn delayed_deregistration_cannot_remove_a_replacement_session() {
    let loopback = "127.0.0.1:0".parse().unwrap();
    let controller = Controller::bind(loopback, loopback).await.unwrap();
    let endpoint = Endpoint::from_shared(format!("http://{}", controller.grpc_addr().unwrap()))
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5));
    tokio::spawn(controller.serve());
    let mut client = AgentRegistrationClient::connect(endpoint).await.unwrap();

    let registration = RegisterRequest {
        agent_id: "test-agent".to_owned(),
        ..Default::default()
    };
    let original = client
        .register(registration.clone())
        .await
        .unwrap()
        .into_inner();
    let departure = DeregisterRequest {
        agent_id: registration.agent_id.clone(),
        reason: "SIGTERM".to_owned(),
        session_id: original.session_id,
    };
    client.deregister(departure.clone()).await.unwrap();

    let replacement = client.register(registration).await.unwrap().into_inner();
    assert_ne!(replacement.session_id, departure.session_id);
    let error = client.deregister(departure.clone()).await.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);

    client
        .deregister(DeregisterRequest {
            session_id: replacement.session_id,
            ..departure
        })
        .await
        .unwrap();
}
