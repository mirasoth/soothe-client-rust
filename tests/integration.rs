//! Live integration tests against a running soothe-daemon (Go parity).
//!
//! Env:
//! - `SOOTHE_WS_URL` / `SOOTHE_DAEMON_URL` (default `ws://127.0.0.1:8765`)
//! - `SOOTHE_INTEGRATION=1` → fail if daemon down; `=0` → skip; unset → skip if unreachable
//!
//! Every test body is capped at [`common::TEST_DEADLINE`] (180s).

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use serde_json::{json, Map};
use soothe_client::appkit::{
    ConnectionPool, DaemonSession, DaemonSessionOptions, EventClassifier, InMemoryLoopSessionStore,
    InputOpts, QueryGate, SendTurnOptions, TimeoutPolicy, TurnConfig, TurnRunner,
};
use soothe_client::helpers::fetch_config_section;
use soothe_client::new_request_id;
use soothe_client::{AsyncCommandClient, Client, TEXT_COMPLETION};

macro_rules! skip_if_no_daemon {
    () => {{
        let Some(url) = common::skip_if_no_daemon_inner(common::daemon_url()).await else {
            return;
        };
        url
    }};
}

// ---------------------------------------------------------------------------
// Core connectivity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_connect_and_status() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        assert!(client.is_connected());
        assert!(client.is_handshake_complete());
        let status = client.fetch_daemon_status().await.expect("status");
        assert!(
            status.get("readiness_state").and_then(|v| v.as_str()) == Some("ready")
                || status.get("running").and_then(|v| v.as_bool()) == Some(true)
                || !status.is_empty()
        );
        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_helpers_live() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        assert!(
            soothe_client::is_daemon_live(
                &url,
                Duration::from_secs(5),
                true,
                Duration::from_secs(5)
            )
            .await
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Loop management
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_loop_new_list_get() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        let dir = common::temp_workspace();
        let mut params = Map::new();
        params.insert("workspace".into(), json!(dir.path().display().to_string()));
        let created = client.loop_new(params).await.expect("loop_new");
        let loop_id = common::loop_id_from(&created).expect("loop_id");
        eprintln!("loop_new -> {loop_id}");

        let listed = client.loop_list(20).await.expect("loop_list");
        assert!(
            listed.get("loops").and_then(|v| v.as_array()).is_some() || !listed.is_empty(),
            "expected loops in list: {listed:?}"
        );

        let got = client.loop_get(&loop_id).await.expect("loop_get");
        eprintln!("loop_get -> {got:?}");
        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_loop_tree_cards_history_messages_state() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        let dir = common::temp_workspace();
        let mut params = Map::new();
        params.insert("workspace".into(), json!(dir.path().display().to_string()));
        let created = client.loop_new(params).await.expect("loop_new");
        let loop_id = common::loop_id_from(&created).expect("loop_id");

        match client.loop_tree(&loop_id, Some("json")).await {
            Ok(v) => eprintln!("loop_tree ok: keys={:?}", v.keys().collect::<Vec<_>>()),
            Err(e) => eprintln!("loop_tree soft-fail: {e}"),
        }
        match client.loop_history_fetch(&loop_id).await {
            Ok(v) => eprintln!("loop_history ok: {v:?}"),
            Err(e) => eprintln!("loop_history soft-fail: {e}"),
        }
        match client.loop_messages(&loop_id, 10, 0).await {
            Ok(v) => eprintln!("loop_messages ok: {v:?}"),
            Err(e) => eprintln!("loop_messages soft-fail: {e}"),
        }
        match client.loop_state_get(&loop_id).await {
            Ok(v) => eprintln!("loop_state_get ok: {v:?}"),
            Err(e) => eprintln!("loop_state_get soft-fail: {e}"),
        }

        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_loop_bootstrap_and_list() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
        let opts = DaemonSessionOptions {
            workspace: Some(dir.path().display().to_string()),
            ..Default::default()
        };
        let session = DaemonSession::new(&url, Some(opts));
        let ready = session.connect(None).await.expect("connect");
        assert!(common::loop_id_from(&ready).is_some() || !session.loop_id().await.is_empty());
        let loops = session.list_loops(5).await.expect("list");
        assert!(!loops.is_empty());
        session.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_daemon_session_turn() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
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
            .iter_turn_chunks(Some(Duration::from_secs(60)))
            .await
            .expect("chunks");
        assert!(
            !chunks.is_empty() || !session.last_turn_end_state.lock().await.is_empty(),
            "expected turn output or terminal state"
        );
        session.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_daemon_session_detach() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
        let opts = DaemonSessionOptions {
            workspace: Some(dir.path().display().to_string()),
            ..Default::default()
        };
        let session = DaemonSession::new(&url, Some(opts));
        session.connect(None).await.expect("connect");
        session.detach().await.expect("detach");
        session.close().await.ok();
    })
    .await;
}

// ---------------------------------------------------------------------------
// Catalog / config / MCP
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_skills_and_models() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        // Soft-fail: skills/models catalog can be slow or unimplemented on some daemons.
        match tokio::time::timeout(Duration::from_secs(20), client.list_skills()).await {
            Ok(Ok(skills)) => {
                eprintln!("skills keys={:?}", skills.keys().collect::<Vec<_>>());
            }
            Ok(Err(e)) => eprintln!("list_skills soft-fail: {e}"),
            Err(_) => eprintln!("list_skills soft-fail: timed out"),
        }
        match tokio::time::timeout(Duration::from_secs(20), client.list_models()).await {
            Ok(Ok(models)) => {
                eprintln!("models keys={:?}", models.keys().collect::<Vec<_>>());
            }
            Ok(Err(e)) => eprintln!("list_models soft-fail: {e}"),
            Err(_) => eprintln!("list_models soft-fail: timed out"),
        }
        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_config_get() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        match fetch_config_section(&client, "agent").await {
            Ok(v) => {
                eprintln!("config agent keys={:?}", v.keys().collect::<Vec<_>>());
                assert!(!v.is_empty());
            }
            Err(e) => eprintln!("config_get soft-fail: {e}"),
        }
        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_mcp_status() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        match client.mcp_status().await {
            Ok(v) => eprintln!("mcp_status: {v:?}"),
            Err(e) => eprintln!("mcp_status soft-fail: {e}"),
        }
        client.close().await.ok();
    })
    .await;
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_jobs_create_status_cancel() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
        let client = AsyncCommandClient::new(&url).with_timeout(Duration::from_secs(20));
        let created = client
            .job_create(
                "Echo: rust integration smoke — done",
                Some(&dir.path().display().to_string()),
            )
            .await
            .expect("job_create");
        let job_id = common::job_id_from(&created).expect("job_id");
        let _ = client.job_status(&job_id).await.expect("status");
        let _ = client.job_cancel(&job_id).await.expect("cancel");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_jobs_pause_resume_dag_guidance() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
        let client = AsyncCommandClient::new(&url).with_timeout(Duration::from_secs(15));
        let created = match client
            .job_create(
                "Count slowly then finish",
                Some(&dir.path().display().to_string()),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("job_create soft-fail: {e}");
                return;
            }
        };
        let Some(job_id) = common::job_id_from(&created) else {
            eprintln!("no job_id in {created:?}");
            return;
        };

        match client.job_pause(&job_id).await {
            Ok(v) => eprintln!("job_pause: {v:?}"),
            Err(e) => eprintln!("job_pause soft-fail: {e}"),
        }
        match client.job_resume(&job_id).await {
            Ok(v) => eprintln!("job_resume: {v:?}"),
            Err(e) => eprintln!("job_resume soft-fail: {e}"),
        }
        match client.job_dag(&job_id).await {
            Ok(v) => eprintln!("job_dag: {v:?}"),
            Err(e) => eprintln!("job_dag soft-fail: {e}"),
        }
        match client
            .job_guidance(&job_id, "Prefer a short answer", None)
            .await
        {
            Ok(v) => eprintln!("job_guidance: {v:?}"),
            Err(e) => eprintln!("job_guidance soft-fail: {e}"),
        }
        let _ = client.job_cancel(&job_id).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Cron + Autopilot
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_cron_add_list_show_cancel() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = AsyncCommandClient::new(&url).with_timeout(Duration::from_secs(15));
        let added = match client
            .cron_add("every day at 03:00 say hello from rust-int", Some(0))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("cron_add soft-fail: {e}");
                return;
            }
        };
        eprintln!("cron_add: {added:?}");
        let job_id = added
            .get("job")
            .and_then(|j| j.get("job_id").or_else(|| j.get("id")))
            .and_then(|v| v.as_str())
            .or_else(|| added.get("job_id").and_then(|v| v.as_str()))
            .map(|s| s.to_string());

        match client.cron_list(None).await {
            Ok(v) => eprintln!("cron_list: {v:?}"),
            Err(e) => eprintln!("cron_list soft-fail: {e}"),
        }
        if let Some(id) = job_id {
            match client.cron_show(&id).await {
                Ok(v) => eprintln!("cron_show: {v:?}"),
                Err(e) => eprintln!("cron_show soft-fail: {e}"),
            }
            match client.cron_cancel(&id).await {
                Ok(v) => eprintln!("cron_cancel: {v:?}"),
                Err(e) => eprintln!("cron_cancel soft-fail: {e}"),
            }
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_autopilot_status_list_subscribe() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let cmd = AsyncCommandClient::new(&url).with_timeout(Duration::from_secs(15));
        match cmd.autopilot_status().await {
            Ok(v) => eprintln!("autopilot_status: {v:?}"),
            Err(e) => eprintln!("autopilot_status soft-fail: {e}"),
        }
        match cmd.autopilot_list_goals().await {
            Ok(v) => eprintln!("autopilot_list_goals: {v:?}"),
            Err(e) => eprintln!("autopilot_list_goals soft-fail: {e}"),
        }
        match cmd.autopilot_list_jobs().await {
            Ok(v) => eprintln!("autopilot_list_jobs: {v:?}"),
            Err(e) => eprintln!("autopilot_list_jobs soft-fail: {e}"),
        }

        let client = common::connect_ready(&url).await;
        match client.autopilot_subscribe().await {
            Ok(sub) => {
                eprintln!("autopilot_subscribe: {sub}");
                let _ = client.autopilot_unsubscribe(&sub).await;
            }
            Err(e) => eprintln!("autopilot_subscribe soft-fail: {e}"),
        }
        client.close().await.ok();
    })
    .await;
}

// ---------------------------------------------------------------------------
// Fire-and-forget send_* + pool
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_send_methods_smoke() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = common::connect_ready(&url).await;
        let dir = common::temp_workspace();
        let mut params = Map::new();
        params.insert("workspace".into(), json!(dir.path().display().to_string()));
        let created = client.loop_new(params).await.expect("loop_new");
        let loop_id = common::loop_id_from(&created).expect("loop_id");
        let rid = new_request_id();

        client
            .send_daemon_status(&[&rid])
            .await
            .expect("send_daemon_status");
        client
            .send_skills_list(&[&rid])
            .await
            .expect("send_skills_list");
        client
            .send_models_list(&[&rid])
            .await
            .expect("send_models_list");
        client
            .send_mcp_status(&[&rid])
            .await
            .expect("send_mcp_status");
        client
            .send_loop_messages(&loop_id, 5, 0, false, &[&rid])
            .await
            .expect("send_loop_messages");
        client
            .send_loop_state_get(&loop_id, &[&rid])
            .await
            .expect("send_loop_state_get");
        client.close().await.ok();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_pool_turn_runner() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let dir = common::temp_workspace();
        let workspace = dir.path().display().to_string();
        let store = std::sync::Arc::new(InMemoryLoopSessionStore::new());
        let pool = std::sync::Arc::new(ConnectionPool::new(&url, store.clone(), None));
        let runner = TurnRunner::new(
            pool.clone(),
            std::sync::Arc::new(QueryGate::new()),
            EventClassifier::with_defaults(),
            store,
            Some(TurnConfig {
                query_timeout: Duration::from_secs(60),
                idle_timeout: Duration::from_secs(15),
                on_idle_timeout: TimeoutPolicy::SoftComplete,
                on_query_timeout: TimeoutPolicy::SoftComplete,
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
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_handshake_accessors() {
    common::with_deadline(async {
        let url = skip_if_no_daemon!();
        let client = Client::new(&url);
        assert!(!client.is_handshake_complete());
        soothe_client::session::connect_with_retries(&client, 8, Duration::from_millis(150))
            .await
            .expect("connect");
        let _ = client.wait_for_daemon_ready(Duration::from_secs(10)).await;
        assert!(client.is_handshake_complete());
        let state = client.readiness_state().await;
        eprintln!("readiness_state={state:?}");
        client.close().await.ok();
    })
    .await;
}
