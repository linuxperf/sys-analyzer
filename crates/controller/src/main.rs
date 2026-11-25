use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use sys_analyzer_controller::{
    Controller, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS,
};
use sys_analyzer_shared::{DEFAULT_GRPC_ADDR, DEFAULT_REST_ADDR};

#[derive(Parser)]
#[command(version, about = "Serve the sys-analyzer control plane")]
struct Args {
    #[arg(long, env = "SYS_ANALYZER_REST_ADDR", default_value = DEFAULT_REST_ADDR)]
    rest_addr: SocketAddr,
    #[arg(long, env = "SYS_ANALYZER_GRPC_ADDR", default_value = DEFAULT_GRPC_ADDR)]
    grpc_addr: SocketAddr,
    /// Interval between Agent heartbeats in milliseconds.
    #[arg(long, env = "SYS_ANALYZER_HEARTBEAT_INTERVAL_MS", default_value_t = DEFAULT_HEARTBEAT_INTERVAL_MS)]
    heartbeat_interval_ms: u64,
    /// Time from registration or the most recent heartbeat until lease expiration in milliseconds.
    #[arg(long, env = "SYS_ANALYZER_LEASE_TIMEOUT_MS", default_value_t = DEFAULT_LEASE_TIMEOUT_MS)]
    lease_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let controller = Controller::bind_with_heartbeat(
        args.rest_addr,
        args.grpc_addr,
        args.heartbeat_interval_ms,
        args.lease_timeout_ms,
    )
    .await?;
    println!(
        "Controller listening on REST {} and gRPC {}",
        controller.rest_addr()?,
        controller.grpc_addr()?
    );
    controller.serve().await
}
