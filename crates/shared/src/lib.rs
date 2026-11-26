use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use http::Uri;
use serde::{Deserialize, Serialize};

pub const DEFAULT_REST_ADDR: &str = "127.0.0.1:8080";
pub const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:50051";
pub const AGENTS_PATH: &str = "/agents";

pub fn parse_controller_url(url: &str) -> Result<Uri> {
    let uri: Uri = url.parse().context("invalid Controller URL")?;
    ensure!(
        uri.scheme_str() == Some("http")
            && uri
                .authority()
                .is_some_and(|authority| !authority.as_str().contains('@'))
            && uri.path() == "/"
            && uri.query().is_none()
            && !url.contains('#'),
        "expected a Controller URL in the form http://host:port"
    );
    Ok(uri)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    #[serde(alias = "online")]
    Online,
    #[serde(alias = "offline")]
    Offline,
    #[serde(alias = "disabled")]
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: String,
    pub hostname: String,
    pub pid: u32,
    pub version: String,
    pub labels: Vec<String>,
    pub metadata: BTreeMap<String, String>,
    pub status: AgentStatus,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDetail {
    #[serde(flatten)]
    pub record: AgentRecord,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorBody {
    pub error: String,
}

pub mod grpc {
    tonic::include_proto!("sys_analyzer.control.v1");
}
