use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http::{Method, Request, Uri, uri::PathAndQuery};
use http_body_util::{BodyExt, Empty};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::de::DeserializeOwned;
use sys_analyzer_shared::{
    AGENTS_PATH, AgentDetail, AgentRecord, AgentStatus, ErrorBody, parse_controller_url,
};

pub(super) enum AgentOperation {
    Show,
    Disable,
    Enable,
}

pub(super) struct Controller<'a> {
    address: &'a str,
}

impl<'a> Controller<'a> {
    pub(super) fn new(address: &'a str) -> Self {
        Self { address }
    }

    pub(super) async fn list(&self, status: Option<AgentStatus>) -> Result<Vec<AgentRecord>> {
        let path = match status {
            Some(status) => {
                let query = match status {
                    AgentStatus::Online => "online",
                    AgentStatus::Offline => "offline",
                    AgentStatus::Disabled => "disabled",
                };
                PathAndQuery::try_from(format!("{AGENTS_PATH}?status={query}"))?
            }
            None => PathAndQuery::from_static(AGENTS_PATH),
        };
        self.request_json(Method::GET, path)
            .await
            .with_context(|| format!("query Controller at {}", self.address))
    }

    pub(super) async fn detail(
        &self,
        agent_id: &str,
        operation: AgentOperation,
    ) -> Result<AgentDetail> {
        let base = agent_path(agent_id);
        let (method, path, operation) = match operation {
            AgentOperation::Show => (Method::GET, base, "query"),
            AgentOperation::Disable => (Method::POST, format!("{base}/disable"), "disable"),
            AgentOperation::Enable => (Method::POST, format!("{base}/enable"), "enable"),
        };
        self.request_json(method, PathAndQuery::try_from(path)?)
            .await
            .with_context(|| {
                format!(
                    "{operation} Agent {agent_id:?} at Controller {}",
                    self.address
                )
            })
    }

    pub(super) async fn remove(&self, agent_id: &str) -> Result<()> {
        let path = PathAndQuery::try_from(agent_path(agent_id))?;
        self.request(Method::DELETE, path)
            .await
            .with_context(|| format!("remove Agent {agent_id:?} at Controller {}", self.address))
            .map(|_| ())
    }

    async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: PathAndQuery,
    ) -> Result<T> {
        let body = self.request(method, path).await?;
        serde_json::from_slice(&body).context("invalid Agent data from Controller")
    }

    async fn request(&self, method: Method, path: PathAndQuery) -> Result<Bytes> {
        let mut parts = parse_controller_url(self.address)?.into_parts();
        parts.path_and_query = Some(path);
        let uri = Uri::from_parts(parts)?;
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Empty::new())?;
        let client = Client::builder(TokioExecutor::new()).build_http::<Empty<Bytes>>();
        tokio::time::timeout(Duration::from_secs(5), async {
            let response = client
                .request(request)
                .await
                .context("HTTP request failed")?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .context("read Controller response")?
                .to_bytes();
            if !status.is_success() {
                if let Ok(ErrorBody { error }) = serde_json::from_slice(&body) {
                    bail!("Controller returned HTTP {status}: {error}");
                }
                bail!("Controller returned HTTP {status}");
            }
            Ok(body)
        })
        .await
        .context("Controller request timed out")?
    }
}

fn agent_path(agent_id: &str) -> String {
    let encoded_id = utf8_percent_encode(agent_id, NON_ALPHANUMERIC);
    format!("{AGENTS_PATH}/{encoded_id}")
}
