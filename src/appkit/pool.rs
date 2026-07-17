//! Connection pool mapping sessions to daemon loops.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Map;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::errors::{Error, Result};
use crate::session::{bootstrap_loop_session, connect_with_retries, BootstrapOptions};

use super::session_store::{SessionRecord, SessionStore};

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Max concurrent pooled connections.
    pub pool_size: usize,
    /// Query timeout default.
    pub query_timeout: Duration,
    /// Connect timeout.
    pub connection_timeout: Duration,
    /// Max idle time before recycle on acquire.
    pub max_idle_time: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 1000,
            query_timeout: Duration::from_secs(30 * 60),
            connection_timeout: Duration::from_secs(30),
            max_idle_time: Duration::from_secs(10 * 60),
        }
    }
}

/// Pool stats snapshot.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Active checked-out connections.
    pub active: usize,
    /// Idle connections in pool.
    pub idle: usize,
}

/// A pooled connection bound to a session.
pub struct PooledConn {
    /// Slot id.
    pub slot_id: String,
    /// Session id.
    pub session_id: String,
    /// Loop id.
    pub loop_id: String,
    /// Underlying client.
    pub client: Client,
    last_used: Instant,
}

impl PooledConn {
    /// Whether connected.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Loop id.
    pub fn get_loop_id(&self) -> &str {
        &self.loop_id
    }
}

struct PoolInner {
    idle: HashMap<String, PooledConn>,
    active: HashMap<String, PooledConn>,
}

/// Maps chat sessions to daemon loops.
pub struct ConnectionPool<S: SessionStore> {
    daemon_url: String,
    store: Arc<S>,
    cfg: PoolConfig,
    inner: Mutex<PoolInner>,
}

impl<S: SessionStore + 'static> ConnectionPool<S> {
    /// Create a pool.
    pub fn new(daemon_url: impl Into<String>, store: Arc<S>, cfg: Option<PoolConfig>) -> Self {
        Self {
            daemon_url: daemon_url.into(),
            store,
            cfg: cfg.unwrap_or_default(),
            inner: Mutex::new(PoolInner {
                idle: HashMap::new(),
                active: HashMap::new(),
            }),
        }
    }

    /// Snapshot stats.
    pub async fn stats(&self) -> PoolStats {
        let inner = self.inner.lock().await;
        PoolStats {
            active: inner.active.len(),
            idle: inner.idle.len(),
        }
    }

    /// Acquire (or bootstrap) a connection for a session.
    pub async fn acquire(
        &self,
        session_id: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<PooledConn> {
        {
            let mut inner = self.inner.lock().await;
            if let Some(mut conn) = inner.idle.remove(session_id) {
                if conn.last_used.elapsed() > self.cfg.max_idle_time
                    || !conn.client.is_connection_alive()
                {
                    let _ = conn.client.close().await;
                } else {
                    conn.last_used = Instant::now();
                    let out = PooledConn {
                        slot_id: conn.slot_id.clone(),
                        session_id: conn.session_id.clone(),
                        loop_id: conn.loop_id.clone(),
                        client: conn.client.clone(),
                        last_used: conn.last_used,
                    };
                    inner.active.insert(session_id.to_string(), conn);
                    return Ok(out);
                }
            }
            if inner.active.len() + inner.idle.len() >= self.cfg.pool_size {
                return Err(Error::msg("pool exhausted"));
            }
        }

        // Ensure session record exists.
        if self.store.get_session(session_id).await.is_none() {
            self.store
                .create_session(SessionRecord {
                    session_id: session_id.to_string(),
                    workspace_id: workspace_id.to_string(),
                    user_id: user_id.to_string(),
                    ..Default::default()
                })
                .await;
        }

        let client = Client::new(&self.daemon_url);
        connect_with_retries(&client, 40, Duration::from_millis(250)).await?;

        let loop_id = if let Some(existing) = self.store.get_loop_id_for_session(session_id).await {
            match client.reattach_and_probe(&existing).await {
                Ok(()) => existing,
                Err(_) => {
                    let mut boot = BootstrapOptions::new();
                    boot.workspace = Some(workspace_id.to_string());
                    boot.user_id = Some(user_id.to_string());
                    let ready = bootstrap_loop_session(&client, boot, None).await?;
                    ready
                        .get("loop_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                }
            }
        } else {
            let mut boot = BootstrapOptions::new();
            boot.workspace = Some(workspace_id.to_string());
            boot.user_id = Some(user_id.to_string());
            let ready = bootstrap_loop_session(&client, boot, None).await?;
            ready
                .get("loop_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };

        if loop_id.is_empty() {
            return Err(Error::msg("failed to bootstrap pooled loop"));
        }
        self.store.set_loop_id(session_id, &loop_id).await;
        self.store.update_last_used(session_id).await;

        let conn = PooledConn {
            slot_id: format!("{session_id}:{loop_id}"),
            session_id: session_id.to_string(),
            loop_id,
            client,
            last_used: Instant::now(),
        };
        let out = PooledConn {
            slot_id: conn.slot_id.clone(),
            session_id: conn.session_id.clone(),
            loop_id: conn.loop_id.clone(),
            client: conn.client.clone(),
            last_used: conn.last_used,
        };
        self.inner
            .lock()
            .await
            .active
            .insert(session_id.to_string(), conn);
        Ok(out)
    }

    /// Release session connection back to idle.
    pub async fn release(&self, session_id: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(mut conn) = inner.active.remove(session_id) {
            conn.last_used = Instant::now();
            inner.idle.insert(session_id.to_string(), conn);
        }
    }

    /// Reset session (close + clear loop binding).
    pub async fn reset_session(&self, session_id: &str) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            if let Some(conn) = inner.active.remove(session_id) {
                let _ = conn.client.close().await;
            }
            if let Some(conn) = inner.idle.remove(session_id) {
                let _ = conn.client.close().await;
            }
        }
        self.store.increment_reset_count(session_id).await;
        self.store.set_loop_id(session_id, "").await;
        Ok(())
    }

    /// Stop pool and close all connections.
    pub async fn stop(&self) {
        let mut inner = self.inner.lock().await;
        let mut conns: Vec<PooledConn> = inner.active.drain().map(|(_, c)| c).collect();
        conns.extend(inner.idle.drain().map(|(_, c)| c));
        drop(inner);
        for conn in conns {
            let _ = conn.client.close().await;
        }
    }
}

/// Helper to build flat loop_input dict (coerced by callers to notification).
pub fn input_message_for_loop(
    text: &str,
    loop_id: &str,
    attachments: Option<serde_json::Value>,
    opts: Option<&super::turn_runner::InputOpts>,
) -> Map<String, serde_json::Value> {
    use serde_json::json;
    let mut m = Map::new();
    m.insert("type".into(), json!("loop_input"));
    m.insert("content".into(), json!(text));
    m.insert("loop_id".into(), json!(loop_id));
    if let Some(a) = attachments {
        m.insert("attachments".into(), a);
    }
    if let Some(o) = opts {
        if let Some(h) = &o.intent_hint {
            m.insert("intent_hint".into(), json!(h));
        }
        if let Some(s) = &o.preferred_subagent {
            m.insert("preferred_subagent".into(), json!(s));
        }
        if let Some(schema) = &o.response_schema {
            m.insert("response_schema".into(), schema.clone());
        }
    }
    m
}
