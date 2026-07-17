//! Scripting helpers for one-shot RPCs and daemon probes.

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::client::Client;
use crate::errors::{Error, Result};
use crate::session::connect_with_retries;

/// Resolve WebSocket URL from env (`SOOTHE_WS_URL` / `SOOTHE_DAEMON_URL`).
pub fn websocket_url_from_env() -> String {
    crate::config::load_config_from_env().daemon_url
}

/// Check daemon status via an already-connected client.
pub async fn check_daemon_status(client: &Client) -> Result<Map<String, Value>> {
    client.fetch_daemon_status().await
}

/// True when daemon answers with `readiness_state == ready`.
pub async fn is_daemon_live(
    ws_url: &str,
    connect_timeout: Duration,
    wait_for_ready: bool,
    ready_timeout: Duration,
) -> bool {
    let client = Client::new(ws_url);
    let connect = async { connect_with_retries(&client, 5, Duration::from_millis(200)).await };
    if tokio::time::timeout(connect_timeout, connect)
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_none()
    {
        return false;
    }
    if !wait_for_ready {
        let _ = client.close().await;
        return true;
    }
    let deadline = tokio::time::Instant::now() + ready_timeout;
    let mut ok = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(status) = client.fetch_daemon_status().await {
            let ready = status.get("readiness_state").and_then(|v| v.as_str()) == Some("ready")
                || status.get("running").and_then(|v| v.as_bool()) == Some(true);
            if ready {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = client.close().await;
    ok
}

/// One-shot protocol-1 RPC; errors returned as `{"error": "..."}`.
pub async fn protocol1_rpc(
    ws_url: &str,
    method: &str,
    params: Map<String, Value>,
    mode: &str,
    rpc_timeout: Duration,
) -> Map<String, Value> {
    let client = Client::new(ws_url);
    if let Err(e) = connect_with_retries(&client, 5, Duration::from_millis(200)).await {
        let mut m = Map::new();
        m.insert("error".into(), json!(e.to_string()));
        return m;
    }
    let out = match mode {
        "notify" => match client.notify(method, params).await {
            Ok(()) => Map::new(),
            Err(e) => {
                let mut m = Map::new();
                m.insert("error".into(), json!(e.to_string()));
                m
            }
        },
        "subscribe" => match client.subscribe(method, params, rpc_timeout).await {
            Ok(id) => {
                let mut m = Map::new();
                m.insert("subscription_id".into(), json!(id));
                m
            }
            Err(e) => {
                let mut m = Map::new();
                m.insert("error".into(), json!(e.to_string()));
                m
            }
        },
        _ => match client.request(method, params, rpc_timeout).await {
            Ok(r) => r,
            Err(e) => {
                let mut m = Map::new();
                m.insert("error".into(), json!(e.to_string()));
                m
            }
        },
    };
    let _ = client.close().await;
    out
}

/// Request daemon shutdown.
pub async fn request_daemon_shutdown(client: &Client) -> Result<()> {
    let resp = client
        .request("daemon_shutdown", Map::new(), Duration::from_secs(10))
        .await?;
    let status = resp.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status != "acknowledged" && !status.is_empty() {
        return Err(Error::msg(format!("unexpected shutdown status: {status}")));
    }
    Ok(())
}

/// Request config reload.
pub async fn request_daemon_config_reload(client: &Client) -> Result<Map<String, Value>> {
    client
        .request("config_reload", Map::new(), Duration::from_secs(5))
        .await
}

/// Fetch skills catalog.
pub async fn fetch_skills_catalog(client: &Client) -> Result<Map<String, Value>> {
    client.list_skills().await
}

/// Fetch a config section.
pub async fn fetch_config_section(client: &Client, section: &str) -> Result<Map<String, Value>> {
    let mut params = Map::new();
    if !section.is_empty() {
        params.insert("section".into(), json!(section));
    }
    client
        .request("config_get", params, Duration::from_secs(10))
        .await
}

/// Fetch loop history.
pub async fn fetch_loop_history(client: &Client, loop_id: &str) -> Result<Map<String, Value>> {
    client.loop_history_fetch(loop_id).await
}

/// Fetch loop cards.
pub async fn fetch_loop_cards(client: &Client, loop_id: &str) -> Result<Map<String, Value>> {
    client.loop_cards_fetch(loop_id).await
}
