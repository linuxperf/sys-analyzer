use anyhow::Result;
use clap::Parser;
use sys_analyzer_agent::run;
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
    run(
        &args.controller,
        args.agent_id,
        args.labels,
        args.metadata.into_iter().collect(),
    )
    .await
}

fn parse_metadata(entry: &str) -> Result<(String, String), String> {
    match entry.split_once('=') {
        Some((key, value)) if !key.trim().is_empty() => Ok((key.to_owned(), value.to_owned())),
        _ => Err("expected KEY=VALUE with a non-empty key".to_owned()),
    }
}
