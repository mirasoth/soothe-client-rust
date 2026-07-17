//! Session bootstrap helpers.

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::client::Client;
use crate::errors::{Error, Result};
use crate::Config;

/// Options for `bootstrap_loop_session`.
#[derive(Debug, Clone, Default)]
pub struct BootstrapOptions {
    /// Resume an existing loop id (skip loop_new).
    pub resume_loop_id: Option<String>,
    /// Workspace path (`client_workspace`).
    pub workspace: Option<String>,
    /// User id.
    pub user_id: Option<String>,
    /// Client workspace id.
    pub client_workspace_id: Option<String>,
    /// Stream delivery mode.
    pub stream_delivery: String,
    /// Ephemeral loop flag.
    pub is_ephemeral: bool,
}

impl BootstrapOptions {
    /// Default adaptive delivery.
    pub fn new() -> Self {
        Self {
            stream_delivery: "adaptive".into(),
            ..Default::default()
        }
    }
}

/// Connect with bounded retries (cold-start races).
pub async fn connect_with_retries(
    client: &Client,
    max_retries: u32,
    retry_delay: Duration,
) -> Result<()> {
    let max = if max_retries == 0 { 40 } else { max_retries };
    let delay = if retry_delay.is_zero() {
        Duration::from_millis(250)
    } else {
        retry_delay
    };
    let mut last = None;
    for attempt in 1..=max {
        match client.connect().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e);
                if attempt < max {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| Error::msg("connect failed")))
}

/// Run loop_new|reattach → subscribe. Assumes handshake already done in `connect`.
pub async fn bootstrap_loop_session(
    client: &Client,
    opts: BootstrapOptions,
    cfg: Option<&Config>,
) -> Result<Map<String, Value>> {
    let default_cfg = Config::default();
    let cfg = cfg.unwrap_or(&default_cfg);
    let delivery = if matches!(
        opts.stream_delivery.as_str(),
        "batch" | "adaptive" | "streaming"
    ) {
        opts.stream_delivery.as_str()
    } else {
        "adaptive"
    };

    let loop_id = if let Some(resume) = opts
        .resume_loop_id
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        resume
    } else {
        let mut params = Map::new();
        if let Some(ws) = &opts.workspace {
            if !ws.is_empty() {
                params.insert("client_workspace".into(), json!(ws));
            }
        }
        if let Some(uid) = &opts.user_id {
            if !uid.is_empty() {
                params.insert("user_id".into(), json!(uid));
            }
        }
        if let Some(wsid) = &opts.client_workspace_id {
            if !wsid.is_empty() {
                params.insert("client_workspace_id".into(), json!(wsid));
            }
        }
        if opts.is_ephemeral {
            params.insert("is_ephemeral".into(), json!(true));
        }
        let resp = client
            .request("loop_new", params, cfg.loop_status_timeout)
            .await
            .map_err(|e| Error::msg(format!("loop_new: {e}")))?;
        let lid = resp
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if lid.is_empty() {
            return Err(Error::msg("loop_new response missing loop_id"));
        }
        lid
    };

    client
        .loop_subscribe(&loop_id, delivery)
        .await
        .map_err(|e| Error::msg(format!("loop_subscribe: {e}")))?;

    let mut out = Map::new();
    out.insert("type".into(), json!("session_ready"));
    out.insert("loop_id".into(), json!(loop_id));
    out.insert("success".into(), json!(true));
    out.insert("autopilot_mode".into(), json!("solo"));
    Ok(out)
}
