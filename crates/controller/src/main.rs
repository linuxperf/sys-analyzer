use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use sys_analyzer_controller::Controller;
use sys_analyzer_shared::{DEFAULT_GRPC_ADDR, DEFAULT_REST_ADDR};

#[derive(Parser)]
#[command(version, about = "Serve the sys-analyzer control plane")]
struct Args {
    #[arg(long, env = "SYS_ANALYZER_REST_ADDR", default_value = DEFAULT_REST_ADDR)]
    rest_addr: SocketAddr,
    #[arg(long, env = "SYS_ANALYZER_GRPC_ADDR", default_value = DEFAULT_GRPC_ADDR)]
    grpc_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let controller = Controller::bind(args.rest_addr, args.grpc_addr).await?;
    println!(
        "Controller listening on REST {} and gRPC {}",
        controller.rest_addr()?,
        controller.grpc_addr()?
    );
    controller.serve().await
}
