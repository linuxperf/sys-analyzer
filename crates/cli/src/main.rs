use std::{
    io::{self, Write},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use http::Uri;
use http_body_util::{BodyExt, Empty};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use sys_analyzer_shared::{AGENTS_PATH, AgentRecord, DEFAULT_REST_ADDR, parse_controller_url};
use unicode_width::UnicodeWidthStr;

#[derive(Parser)]
#[command(version, about = "Query the sys-analyzer Controller")]
struct Args {
    #[arg(long, global = true, env = "SYS_ANALYZER_REST_URL", default_value_t = format!("http://{DEFAULT_REST_ADDR}"))]
    controller: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List registered Agents
    List {
        #[arg(short, long, value_enum, default_value_t = Output::Table)]
        output: Output,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Table,
    Json,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let Command::List { output } = args.command;
    let agents = list_agents(&args.controller)
        .await
        .with_context(|| format!("query Controller at {}", args.controller))?;

    let mut writer = io::stdout().lock();
    match output {
        Output::Table => write_table(&mut writer, &agents)?,
        Output::Json => {
            serde_json::to_writer_pretty(&mut writer, &agents)?;
            writeln!(writer)?;
        }
    }
    writer.flush()?;
    Ok(())
}

async fn list_agents(controller: &str) -> Result<Vec<AgentRecord>> {
    let mut parts = parse_controller_url(controller)?.into_parts();
    parts.path_and_query = Some(http::uri::PathAndQuery::from_static(AGENTS_PATH));
    let uri = Uri::from_parts(parts)?;
    let client = Client::builder(TokioExecutor::new()).build_http::<Empty<bytes::Bytes>>();
    let body = tokio::time::timeout(Duration::from_secs(5), async {
        let response = client.get(uri).await.context("HTTP request failed")?;
        ensure!(
            response.status().is_success(),
            "Controller returned HTTP {}",
            response.status()
        );
        response
            .into_body()
            .collect()
            .await
            .context("read Controller response")
    })
    .await
    .context("Controller request timed out")??
    .to_bytes();
    serde_json::from_slice(&body).context("invalid Agent list from Controller")
}

fn write_table(writer: &mut impl Write, agents: &[AgentRecord]) -> Result<()> {
    let mut rows = vec![
        [
            "Agent ID",
            "Hostname",
            "PID",
            "Version",
            "Status",
            "Registered at (UTC)",
            "Last heartbeat (UTC)",
            "Labels",
            "Metadata",
        ]
        .map(str::to_owned),
    ];
    for agent in agents {
        rows.push([
            escape_control(&agent.agent_id),
            escape_control(&agent.hostname),
            agent.pid.to_string(),
            escape_control(&agent.version),
            format!("{:?}", agent.status),
            agent.registered_at.to_rfc3339(),
            agent
                .last_heartbeat_at
                .map_or_else(|| "-".to_owned(), |time| time.to_rfc3339()),
            escape_control(&serde_json::to_string(&agent.labels)?),
            escape_control(&serde_json::to_string(&agent.metadata)?),
        ]);
    }
    let mut widths = rows[0].each_ref().map(|cell| cell.width());
    for row in &rows[1..] {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.width());
        }
    }
    for row in rows {
        for (index, (cell, width)) in row.iter().zip(widths).enumerate() {
            if index + 1 == row.len() {
                writeln!(writer, "{cell}")?;
            } else {
                let padding = width - cell.width() + 2;
                write!(writer, "{cell}{:padding$}", "")?;
            }
        }
    }
    Ok(())
}

fn escape_control(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_control() {
            escaped.extend(ch.escape_default());
        } else {
            escaped.push(ch);
        }
    }
    escaped
}
