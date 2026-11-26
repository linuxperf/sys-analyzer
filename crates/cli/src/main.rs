use std::{
    collections::BTreeMap,
    io::{self, Write},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use chrono::DateTime;
use clap::{Parser, Subcommand, ValueEnum};
use http::{Uri, uri::PathAndQuery};
use http_body_util::{BodyExt, Empty};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Serialize, de::DeserializeOwned};
use sys_analyzer_shared::{
    AGENTS_PATH, AgentDetail, AgentRecord, AgentStatus, DEFAULT_REST_ADDR, parse_controller_url,
};
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
        #[arg(long, value_enum, ignore_case = true)]
        status: Option<StatusFilter>,
        #[arg(short, long, value_enum, default_value_t = Output::Table)]
        output: Output,
    },
    /// Show a registered Agent and its lease
    Show {
        agent_id: String,
        #[arg(short, long, value_enum, default_value_t = Output::Table)]
        output: Output,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum StatusFilter {
    Online,
    Offline,
    Disabled,
}

#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Table,
    Json,
}

#[derive(Serialize)]
struct AgentListRow<'a> {
    agent_id: &'a str,
    hostname: &'a str,
    pid: u32,
    version: &'a str,
    labels: &'a [String],
    metadata: &'a BTreeMap<String, String>,
    status: AgentStatus,
    registered_at: i64,
    last_heartbeat_at: Option<i64>,
}

#[derive(Serialize)]
struct AgentDetailRow<'a> {
    #[serde(flatten)]
    agent: AgentListRow<'a>,
    lease_expires_at: i64,
}

impl<'a> From<&'a AgentRecord> for AgentListRow<'a> {
    fn from(agent: &'a AgentRecord) -> Self {
        Self {
            agent_id: &agent.agent_id,
            hostname: &agent.hostname,
            pid: agent.pid,
            version: &agent.version,
            labels: &agent.labels,
            metadata: &agent.metadata,
            status: agent.status,
            registered_at: agent.registered_at.timestamp(),
            last_heartbeat_at: agent.last_heartbeat_at.as_ref().map(DateTime::timestamp),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let stdout = io::stdout();
    match args.command {
        Command::List { status, output } => {
            let path = match status {
                Some(status) => {
                    let value = status.to_possible_value().expect("valid status filter");
                    PathAndQuery::try_from(format!("{AGENTS_PATH}?status={}", value.get_name()))?
                }
                None => PathAndQuery::from_static(AGENTS_PATH),
            };
            let agents: Vec<AgentRecord> = get_controller(&args.controller, path)
                .await
                .with_context(|| format!("query Controller at {}", args.controller))?;
            write_list(&mut stdout.lock(), &agents, output)?;
        }
        Command::Show { agent_id, output } => {
            let encoded_id = utf8_percent_encode(&agent_id, NON_ALPHANUMERIC);
            let path = PathAndQuery::try_from(format!("{AGENTS_PATH}/{encoded_id}"))?;
            let detail = get_controller(&args.controller, path)
                .await
                .with_context(|| {
                    format!("query Agent {agent_id:?} at Controller {}", args.controller)
                })?;
            write_detail(&mut stdout.lock(), &detail, output)?;
        }
    }
    stdout.lock().flush()?;
    Ok(())
}

async fn get_controller<T: DeserializeOwned>(controller: &str, path: PathAndQuery) -> Result<T> {
    let mut parts = parse_controller_url(controller)?.into_parts();
    parts.path_and_query = Some(path);
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
    serde_json::from_slice(&body).context("invalid Agent data from Controller")
}

fn write_list(writer: &mut impl Write, agents: &[AgentRecord], output: Output) -> Result<()> {
    let rows: Vec<_> = agents.iter().map(AgentListRow::from).collect();
    match output {
        Output::Table => write_table(writer, &rows),
        Output::Json => write_json(writer, &rows),
    }
}

fn write_detail(writer: &mut impl Write, detail: &AgentDetail, output: Output) -> Result<()> {
    let row = AgentDetailRow {
        agent: AgentListRow::from(&detail.record),
        lease_expires_at: detail.lease_expires_at.timestamp(),
    };
    match output {
        Output::Table => {
            write_table(writer, &[row.agent])?;
            writeln!(writer, "Lease expires at (UTC)  {}", row.lease_expires_at)?;
            Ok(())
        }
        Output::Json => write_json(writer, &row),
    }
}

fn write_json(writer: &mut impl Write, records: &impl Serialize) -> Result<()> {
    serde_json::to_writer_pretty(&mut *writer, records)?;
    writeln!(writer)?;
    Ok(())
}

fn write_table(writer: &mut impl Write, agents: &[AgentListRow<'_>]) -> Result<()> {
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
            escape_control(agent.agent_id),
            escape_control(agent.hostname),
            agent.pid.to_string(),
            escape_control(agent.version),
            format!("{:?}", agent.status),
            agent.registered_at.to_string(),
            agent
                .last_heartbeat_at
                .map_or_else(|| "-".to_owned(), |seconds| seconds.to_string()),
            escape_control(&serde_json::to_string(agent.labels)?),
            escape_control(&serde_json::to_string(agent.metadata)?),
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn list_formats_share_unix_seconds_and_preserve_missing_heartbeats() {
        let agent = AgentRecord {
            agent_id: "test-agent".to_owned(),
            hostname: "localhost".to_owned(),
            pid: 1,
            version: "0.1.0".to_owned(),
            labels: Vec::new(),
            metadata: Default::default(),
            status: AgentStatus::Online,
            registered_at: "2023-12-31T23:59:59.9Z".parse().unwrap(),
            last_heartbeat_at: Some("2024-01-01T00:00:00.1Z".parse().unwrap()),
        };
        let awaiting_heartbeat = AgentRecord {
            agent_id: "awaiting-heartbeat".to_owned(),
            registered_at: "1969-12-31T23:59:59.9Z".parse().unwrap(),
            last_heartbeat_at: None,
            ..agent.clone()
        };
        let agents = [agent, awaiting_heartbeat];

        let mut table = Vec::new();
        write_list(&mut table, &agents, Output::Table).unwrap();
        let table = String::from_utf8(table).unwrap();
        let mut times = table
            .lines()
            .skip(1)
            .map(|row| row.split_whitespace().skip(5).take(2).collect::<Vec<_>>());
        assert_eq!(times.next().unwrap(), ["1704067199", "1704067200"]);
        assert_eq!(times.next().unwrap(), ["-1", "-"]);

        let mut json = Vec::new();
        write_list(&mut json, &agents, Output::Json).unwrap();
        let records: Vec<Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["registered_at"], json!(1704067199));
        assert_eq!(records[0]["last_heartbeat_at"], json!(1704067200));
        assert_eq!(records[1]["registered_at"], json!(-1));
        assert_eq!(records[1].get("last_heartbeat_at"), Some(&Value::Null));
    }
}
