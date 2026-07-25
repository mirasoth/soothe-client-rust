//! Shared helpers for integration test binaries (`#[path = "common/mod.rs"]`).

#![allow(dead_code)]

use std::env;
use std::future::Future;
use std::time::Duration;

use soothe_client::is_daemon_live;
use soothe_client::session::connect_with_retries;
use soothe_client::Client;
use tempfile::TempDir;

/// Hard ceiling for every live integration test body.
pub const TEST_DEADLINE: Duration = Duration::from_secs(180);

/// Daemon WebSocket URL from env.
pub fn daemon_url() -> String {
    env::var("SOOTHE_WS_URL")
        .or_else(|_| env::var("SOOTHE_DAEMON_URL"))
        .unwrap_or_else(|_| "ws://127.0.0.1:8765".into())
}

pub fn integration_flag() -> Option<bool> {
    match env::var("SOOTHE_INTEGRATION") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            if matches!(v.as_str(), "0" | "false" | "no") {
                Some(false)
            } else if matches!(v.as_str(), "1" | "true" | "yes") {
                Some(true)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

/// Returns `None` when the test should return early (skip).
pub async fn skip_if_no_daemon_inner(url: String) -> Option<String> {
    if integration_flag() == Some(false) {
        eprintln!("skip: SOOTHE_INTEGRATION disabled");
        return None;
    }
    // Bound the live probe so a wedged daemon cannot hang skip logic.
    let probe =
        async { is_daemon_live(&url, Duration::from_secs(3), true, Duration::from_secs(5)).await };
    let live = tokio::time::timeout(Duration::from_secs(10), probe)
        .await
        .unwrap_or(false);
    if integration_flag() != Some(true) && !live {
        eprintln!("skip: daemon not live at {url}");
        return None;
    }
    if integration_flag() == Some(true) {
        assert!(live, "daemon not live at {url}");
    }
    Some(url)
}

/// Connect a client; handshake readiness is established during `connect`.
pub async fn connect_ready(url: &str) -> Client {
    let client = Client::new(url);
    tokio::time::timeout(Duration::from_secs(20), async {
        connect_with_retries(&client, 8, Duration::from_millis(150)).await
    })
    .await
    .expect("connect timed out")
    .expect("connect");
    // No-op when handshake already complete; bounded otherwise.
    let _ = tokio::time::timeout(
        Duration::from_secs(10),
        client.wait_for_daemon_ready(Duration::from_secs(10)),
    )
    .await;
    assert!(
        client.is_connected() && client.is_handshake_complete(),
        "expected connected + handshake complete"
    );
    client
}

/// Run `fut` under the suite's hard 180s ceiling.
pub async fn with_deadline<F, T>(fut: F) -> T
where
    F: Future<Output = T>,
{
    match tokio::time::timeout(TEST_DEADLINE, fut).await {
        Ok(v) => v,
        Err(_) => panic!("integration test exceeded {:?}", TEST_DEADLINE),
    }
}

/// Temp workspace directory for loop/job isolation.
pub fn temp_workspace() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

/// Extract loop_id string from a response map.
pub fn loop_id_from(resp: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    resp.get("loop_id")
        .or_else(|| resp.get("id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Extract job_id from create/status responses.
pub fn job_id_from(resp: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    resp.get("job_id")
        .or_else(|| resp.get("id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}
