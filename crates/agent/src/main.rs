use std::future::pending;

use anyhow::{Context, Result};
use clap::Parser;
use sys_analyzer_agent::{register, registration_request};
use sys_analyzer_shared::DEFAULT_GRPC_ADDR;

#[derive(Parser)]
#[command(version, about = "Register a sys-analyzer Agent with the Controller")]
struct Args {
    #[arg(long, env = "SYS_ANALYZER_GRPC_URL", default_value_t = format!("http://{DEFAULT_GRPC_ADDR}"))]
    controller: String,
    #[arg(long, env = "SYS_ANALYZER_AGENT_ID", value_parser = clap::builder::NonEmptyStringValueParser::new())]
    agent_id: Option<String>,
    #[arg(long = "label")]
    labels: Vec<String>,
    #[arg(long = "metadata", value_name = "KEY=VALUE", value_parser = parse_metadata)]
    metadata: Vec<(String, String)>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let request = registration_request(
        args.agent_id,
        args.labels,
        args.metadata.into_iter().collect(),
    );
    let agent_id = request.agent_id.clone();
    let session = register(&args.controller, request)
        .await
        .with_context(|| format!("register Agent {agent_id:?}"))?;
    println!(
        "Agent {agent_id:?} registered with session {}",
        session.session_id
    );
    pending::<()>().await;
    Ok(())
}

fn parse_metadata(entry: &str) -> Result<(String, String), String> {
    match entry.split_once('=') {
        Some((key, value)) if !key.trim().is_empty() => Ok((key.to_owned(), value.to_owned())),
        _ => Err("expected KEY=VALUE with a non-empty key".to_owned()),
    }
}
