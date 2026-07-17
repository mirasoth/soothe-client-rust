//! Live integration tests against a running soothe-daemon.
//!
//! Env:
//! - `SOOTHE_WS_URL` (default `ws://127.0.0.1:8765`)
//! - `SOOTHE_INTEGRATION=1` → fail if daemon down; `=0` → skip; unset → skip if unreachable

use std::env;
use std::time::Duration;

use soothe_client::appkit::{
    ConnectionPool, DaemonSession, DaemonSessionOptions, EventClassifier, InMemorySessionStore,
    InputOpts, QueryGate, SendTurnOptions, TurnRunner,
};
use soothe_client::session::connect_with_retries;
use soothe_client::{is_daemon_live, AsyncCommandClient, Client, TEXT_COMPLETION};
use tempfile::tempdir;

fn daemon_url() -> String {
    env::var("SOOTHE_WS_URL")
        .or_else(|_| env::var("SOOTHE_DAEMON_URL"))
        .unwrap_or_else(|_| "ws://127.0.0.1:8765".into())
}

fn integration_flag() -> Option<bool> {
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

async fn require_daemon() -> String {
    let url = daemon_url();
    match integration_flag() {
        Some(false) => {
            panic!("skip: SOOTHE_INTEGRATION disabled"); // use catch in each test via check
        }
        Some(true) => {
            assert!(
                is_daemon_live(&url, Duration::from_secs(5), true, Duration::from_secs(10)).await,
                "daemon not live at {url}"
            );
        }
        None => {
            if !is_daemon_live(&url, Duration::from_secs(3), true, Duration::from_secs(5)).await {
                eprintln!("skip: daemon not live at {url}");
                panic!("SKIP_NO_DAEMON");
            }
        }
    }
    url
}

macro_rules! skip_if_no_daemon {
    ($url:expr) => {{
        if integration_flag() == Some(false) {
            eprintln!("skip: SOOTHE_INTEGRATION disabled");
            return;
        }
        let url = $url;
        if integration_flag() != Some(true)
            && !is_daemon_live(&url, Duration::from_secs(3), true, Duration::from_secs(5)).await
        {
            eprintln!("skip: daemon not live at {url}");
            return;
        }
        if integration_flag() == Some(true) {
            assert!(
                is_daemon_live(&url, Duration::from_secs(5), true, Duration::from_secs(10)).await,
                "daemon not live at {url}"
            );
        }
        url
    }};
}

#[tokio::test]
async fn integration_connect_and_status() {
    let url = skip_if_no_daemon!(daemon_url());
    let client = Client::new(&url);
    connect_with_retries(&client, 10, Duration::from_millis(200))
        .await
        .expect("connect");
    assert!(client.is_connected());
    let status = client.fetch_daemon_status().await.expect("status");
    assert!(
        status.get("readiness_state").and_then(|v| v.as_str()) == Some("ready")
            || status.get("running").and_then(|v| v.as_bool()) == Some(true)
    );
    client.close().await.ok();
}

#[tokio::test]
async fn integration_helpers_live() {
    let url = skip_if_no_daemon!(daemon_url());
    assert!(is_daemon_live(&url, Duration::from_secs(5), true, Duration::from_secs(10)).await);
}

#[tokio::test]
async fn integration_loop_bootstrap_and_list() {
    let url = skip_if_no_daemon!(daemon_url());
    let dir = tempdir().unwrap();
    let opts = DaemonSessionOptions {
        workspace: Some(dir.path().display().to_string()),
        ..Default::default()
    };
    let session = DaemonSession::new(&url, Some(opts));
    let ready = session.connect(None).await.expect("connect");
    assert!(ready.get("loop_id").and_then(|v| v.as_str()).is_some());
    let loops = session.list_loops(5).await.expect("list");
    assert!(!loops.is_empty());
    session.close().await.ok();
}

#[tokio::test]
async fn integration_daemon_session_turn() {
    let url = skip_if_no_daemon!(daemon_url());
    let dir = tempdir().unwrap();
    let opts = DaemonSessionOptions {
        workspace: Some(dir.path().display().to_string()),
        ..Default::default()
    };
    let session = DaemonSession::new(&url, Some(opts));
    session.connect(None).await.expect("connect");
    session
        .send_turn(
            "Reply with exactly: pong",
            Some(SendTurnOptions {
                intent_hint: Some(TEXT_COMPLETION.into()),
                ..Default::default()
            }),
        )
        .await
        .expect("send");
    let chunks = session
        .iter_turn_chunks(Some(Duration::from_secs(90)))
        .await
        .expect("chunks");
    assert!(
        !chunks.is_empty() || !session.last_turn_end_state.lock().await.is_empty(),
        "expected turn output or terminal state"
    );
    session.close().await.ok();
}

#[tokio::test]
async fn integration_jobs() {
    let url = skip_if_no_daemon!(daemon_url());
    let dir = tempdir().unwrap();
    let client = AsyncCommandClient::new(&url);
    let created = client
        .job_create(
            "Echo: integration smoke — done",
            Some(&dir.path().display().to_string()),
        )
        .await
        .expect("job_create");
    let job_id = created
        .get("job_id")
        .or_else(|| created.get("id"))
        .and_then(|v| v.as_str())
        .expect("job_id")
        .to_string();
    let _ = client.job_status(&job_id).await.expect("status");
    let _ = client.job_cancel(&job_id).await.expect("cancel");
}

#[tokio::test]
async fn integration_pool_turn_runner() {
    let url = skip_if_no_daemon!(daemon_url());
    let dir = tempdir().unwrap();
    let workspace = dir.path().display().to_string();
    let store = std::sync::Arc::new(InMemorySessionStore::new());
    let pool = std::sync::Arc::new(ConnectionPool::new(&url, store.clone(), None));
    let runner = TurnRunner::new(
        pool.clone(),
        QueryGate::new(),
        EventClassifier::new(),
        store,
        Some(soothe_client::appkit::TurnConfig {
            query_timeout: Duration::from_secs(90),
            idle_timeout: Duration::from_secs(15),
            on_idle_timeout: soothe_client::appkit::TimeoutPolicy::SoftComplete,
            on_query_timeout: soothe_client::appkit::TimeoutPolicy::SoftComplete,
            ..Default::default()
        }),
    );
    let text = runner
        .execute(
            "rust-int-session",
            "Reply with exactly: pool-ok",
            "rust-user",
            &workspace,
            None,
            Some(InputOpts {
                intent_hint: Some(TEXT_COMPLETION.into()),
                ..Default::default()
            }),
        )
        .await
        .expect("execute");
    eprintln!("pool turn text={text:?}");
    pool.stop().await;
}

// silence unused require_daemon
#[allow(dead_code)]
async fn _unused() {
    let _ = require_daemon().await;
}
