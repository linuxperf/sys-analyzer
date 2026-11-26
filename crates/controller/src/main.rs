use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use sys_analyzer_controller::{
    Controller, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS,
    DEFAULT_OFFLINE_RETENTION_MS, DEFAULT_SWEEP_INTERVAL_MS, LifecycleSettings,
};
use sys_analyzer_shared::{DEFAULT_GRPC_ADDR, DEFAULT_REST_ADDR};
use tokio::signal::unix::{SignalKind, signal};

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
    /// Time to keep an Offline Agent record in milliseconds.
    #[arg(long, env = "SYS_ANALYZER_OFFLINE_RETENTION_MS", default_value_t = DEFAULT_OFFLINE_RETENTION_MS)]
    offline_retention_ms: u64,
    /// Interval between Agent lifecycle sweeps in milliseconds.
    #[arg(long, env = "SYS_ANALYZER_SWEEP_INTERVAL_MS", default_value_t = DEFAULT_SWEEP_INTERVAL_MS)]
    sweep_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let controller = Controller::bind_with_lifecycle(
        args.rest_addr,
        args.grpc_addr,
        LifecycleSettings {
            heartbeat_interval_ms: args.heartbeat_interval_ms,
            lease_timeout_ms: args.lease_timeout_ms,
            offline_retention_ms: args.offline_retention_ms,
            sweep_interval_ms: args.sweep_interval_ms,
        },
    )
    .await?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    println!(
        "Controller listening on REST {} and gRPC {}",
        controller.rest_addr()?,
        controller.grpc_addr()?
    );
    controller
        .serve_with_shutdown(async {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
            }
        })
        .await
}
