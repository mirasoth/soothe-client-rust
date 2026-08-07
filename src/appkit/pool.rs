//! Connection pool mapping application sessions to daemon loops (Go `ConnectionPool` parity).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::client::Client;
use crate::errors::{Error, Result};
use crate::protocol::new_notification;
use crate::session::{bootstrap_loop_session, BootstrapOptions};

use super::loop_session_store::{LoopSessionEntry, LoopSessionStore};

/// Returned when no idle pool slot is immediately available.
#[derive(Debug, Clone, thiserror::Error)]
#[error("appkit: connection pool exhausted")]
pub struct ErrPoolExhausted;

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Max concurrent pooled connections (pre-seeded idle slots).
    pub pool_size: usize,
    /// Max idle time before recycle on `acquire`.
    pub max_idle_time: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 1000,
            max_idle_time: Duration::from_secs(10 * 60),
        }
    }
}

/// Pool stats snapshot.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Active checked-out connections keyed by session id.
    pub active: usize,
    /// Idle slots waiting in the pool channel.
    pub idle: usize,
}

/// One connection slot in the pool.
pub struct PooledConn {
    /// Internal slot id.
    pub slot_id: u32,
    /// Application session id (`app_key` in Go).
    pub session_id: String,
    /// Bound StrangeLoop id.
    pub loop_id: Mutex<String>,
    /// Workspace id used at bootstrap.
    pub workspace_id: String,
    /// Underlying protocol client.
    pub client: Client,
    /// Forwarded event stream (`ReceiveMessages` → mpsc); `None` until `start_reader`.
    pub event_rx: Mutex<Option<tokio::sync::mpsc::Receiver<Value>>>,
    /// Whether the forwarder task is still running.
    pub reader_live: AtomicBool,
    /// Last time this slot was acquired.
    pub last_used: Mutex<Instant>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
}

impl PooledConn {
    /// Loop id (Go `getLoopID`).
    pub async fn get_loop_id(&self) -> String {
        self.loop_id.lock().await.clone()
    }

    /// Whether the underlying client is connected and has not signalled disconnect.
    pub async fn is_connected(&self) -> bool {
        self.client.is_connected() && !Self::client_disconnect_notified(&self.client).await
    }

    /// Whether `ReceiveMessages` is still feeding `event_rx`.
    pub async fn event_stream_live(&self) -> bool {
        let has_rx = self.event_rx.lock().await.is_some();
        has_rx && self.reader_live.load(Ordering::SeqCst)
    }

    async fn client_disconnect_notified(client: &Client) -> bool {
        client.disconnect_cause().await.is_some() || !client.is_connection_alive()
    }
}

/// Handle to an acquired pooled connection (shared across async tasks).
pub type PooledConnHandle = Arc<PooledConn>;

type ClientFactory = Arc<dyn Fn(&str) -> Client + Send + Sync>;
type BootstrapFn = Arc<
    dyn Fn(Client, String, String, String) -> Pin<Box<dyn Future<Output = Result<String>> + Send>>
        + Send
        + Sync,
>;

struct IdleSlot {
    client: Client,
}

struct PoolState {
    active_slots: HashMap<String, PooledConnHandle>,
    next_slot_id: u32,
    idle_rx: tokio::sync::mpsc::Receiver<IdleSlot>,
    idle_tx: tokio::sync::mpsc::Sender<IdleSlot>,
    idle_count: usize,
}

/// Manages daemon connections, one active slot per session id.
pub struct ConnectionPool<S: LoopSessionStore> {
    daemon_url: String,
    store: Arc<S>,
    cfg: PoolConfig,
    factory: ClientFactory,
    bootstrap: BootstrapFn,
    state: Mutex<PoolState>,
}

impl<S: LoopSessionStore + 'static> ConnectionPool<S> {
    /// Construct a pool for `daemon_url`. `cfg` defaults via [`PoolConfig::default`].
    pub fn new(daemon_url: impl Into<String>, store: Arc<S>, cfg: Option<PoolConfig>) -> Self {
        let cfg = cfg.unwrap_or_default();
        let pool_size = if cfg.pool_size == 0 {
            PoolConfig::default().pool_size
        } else {
            cfg.pool_size
        };
        let daemon_url = daemon_url.into();
        let factory: ClientFactory = Arc::new(|url| Client::new(url));
        let bootstrap = default_bootstrap_fn();
        let (idle_tx, idle_rx) = tokio::sync::mpsc::channel(pool_size);
        for _ in 0..pool_size {
            let _ = idle_tx.try_send(IdleSlot {
                client: factory(&daemon_url),
            });
        }
        Self {
            daemon_url,
            store,
            cfg: PoolConfig { pool_size, ..cfg },
            factory,
            bootstrap,
            state: Mutex::new(PoolState {
                active_slots: HashMap::new(),
                next_slot_id: 1,
                idle_rx,
                idle_tx,
                idle_count: pool_size,
            }),
        }
    }

    /// Override the client factory (logging/metrics wrappers, test fakes).
    pub fn with_client_factory(mut self, f: Box<dyn Fn(&str) -> Client + Send + Sync>) -> Self {
        self.factory = Arc::from(f);
        self
    }

    /// Override loop bootstrap (`loop_new` + subscribe).
    pub fn with_bootstrap(
        mut self,
        f: impl Fn(
                Client,
                String,
                String,
                String,
            ) -> Pin<Box<dyn Future<Output = Result<String>> + Send>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.bootstrap = Arc::new(f);
        self
    }

    /// Snapshot active and idle slot counts.
    pub async fn stats(&self) -> PoolStats {
        let state = self.state.lock().await;
        PoolStats {
            active: state.active_slots.len(),
            idle: state.idle_count,
        }
    }

    /// Acquire a live connection for `session_id`, bootstrapping or reattaching as needed.
    ///
    /// The caller must call [`Self::release`] when the session connection should be torn down
    /// (TurnRunner keeps the slot active across turns on the same session).
    pub async fn acquire(
        &self,
        session_id: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<PooledConnHandle> {
        // 1. Reuse active connection when still live.
        if let Some(existing) = {
            let state = self.state.lock().await;
            state.active_slots.get(session_id).cloned()
        } {
            let disconnect = PooledConn::client_disconnect_notified(&existing.client).await
                || !existing.is_connected().await
                || !existing.event_stream_live().await;
            if disconnect {
                tracing::warn!(
                    session_id,
                    "previous connection dropped or event stream dead, releasing for fresh bootstrap"
                );
                self.release(session_id).await;
            } else {
                let idle_too_long = self.cfg.max_idle_time > Duration::ZERO
                    && existing.last_used.lock().await.elapsed() > self.cfg.max_idle_time;
                if idle_too_long {
                    tracing::warn!(
                        session_id,
                        max_idle = ?self.cfg.max_idle_time,
                        "session idle beyond max, releasing"
                    );
                    self.release(session_id).await;
                } else {
                    *existing.last_used.lock().await = Instant::now();
                    self.store.update_last_used(session_id).await;
                    return Ok(existing);
                }
            }
        }

        // 2. Pull a slot from the pool (non-blocking — Go `default` branch).
        let idle = {
            let mut state = self.state.lock().await;
            match state.idle_rx.try_recv() {
                Ok(slot) => {
                    state.idle_count = state.idle_count.saturating_sub(1);
                    slot
                }
                Err(_) => return Err(Error::from(ErrPoolExhausted)),
            }
        };

        let slot_id = {
            let mut state = self.state.lock().await;
            let id = state.next_slot_id;
            state.next_slot_id += 1;
            id
        };

        let conn = Arc::new(PooledConn {
            slot_id,
            session_id: session_id.to_string(),
            loop_id: Mutex::new(String::new()),
            workspace_id: workspace_id.to_string(),
            client: idle.client,
            event_rx: Mutex::new(None),
            reader_live: AtomicBool::new(false),
            last_used: Mutex::new(Instant::now()),
            reader_task: Mutex::new(None),
        });

        let (stored_loop_id, has_loop) = self.loop_id_for(session_id).await;
        let loop_id = if !has_loop || stored_loop_id.is_empty() {
            conn.client
                .connect()
                .await
                .map_err(|e| Error::msg(format!("connect: {e}")))?;
            let loop_id = match self
                .bootstrap_new(Arc::clone(&conn), session_id, workspace_id, user_id)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    self.release(session_id).await;
                    return Err(Error::msg(format!("bootstrap new loop: {e}")));
                }
            };
            if let Err(e) = self
                .persist_new_session(session_id, workspace_id, user_id, &loop_id)
                .await
            {
                tracing::warn!(session_id, error = %e, "create session failed");
            }
            loop_id
        } else if self
            .resume_and_reattach(Arc::clone(&conn), &stored_loop_id)
            .await
            .is_err()
        {
            tracing::warn!(session_id, "reattach failed, bootstrapping fresh");
            if let Err(e) = conn.client.connect().await {
                self.release(session_id).await;
                return Err(Error::msg(format!("connect after reattach fail: {e}")));
            }
            let loop_id = match self
                .bootstrap_new(Arc::clone(&conn), session_id, workspace_id, user_id)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    self.release(session_id).await;
                    return Err(Error::msg(format!("bootstrap after reattach fail: {e}")));
                }
            };
            if let Err(e) = self
                .persist_new_session(session_id, workspace_id, user_id, &loop_id)
                .await
            {
                tracing::warn!(session_id, error = %e, "create session after bootstrap failed");
            }
            loop_id
        } else {
            stored_loop_id
        };

        *conn.loop_id.lock().await = loop_id.clone();
        *conn.last_used.lock().await = Instant::now();
        self.store.update_last_used(session_id).await;

        {
            let mut state = self.state.lock().await;
            state
                .active_slots
                .insert(session_id.to_string(), Arc::clone(&conn));
        }

        tracing::info!(slot_id, session_id, loop_id, "acquired pool slot");
        Ok(conn)
    }

    /// Tear down the connection for `session_id` and return a fresh slot to the pool.
    pub async fn release(&self, session_id: &str) {
        let conn = {
            let mut state = self.state.lock().await;
            state.active_slots.remove(session_id)
        };
        let Some(conn) = conn else {
            return;
        };

        if let Some(handle) = conn.reader_task.lock().await.take() {
            handle.abort();
        }
        conn.reader_live.store(false, Ordering::SeqCst);
        *conn.event_rx.lock().await = None;
        let _ = conn.client.close().await;

        tracing::info!(slot_id = conn.slot_id, session_id, "released pool slot");

        let new_slot = IdleSlot {
            client: (self.factory)(&self.daemon_url),
        };
        let mut state = self.state.lock().await;
        if state.idle_tx.try_send(new_slot).is_ok() {
            state.idle_count += 1;
        } else {
            tracing::warn!(session_id, "pool full when returning slot");
        }
    }

    /// Tear down the connection so the next `acquire` bootstraps fresh.
    pub async fn reset_session(&self, session_id: &str) {
        self.release(session_id).await;
        tracing::info!(
            session_id,
            "reset session — next message will create new loop"
        );
    }

    /// Gracefully shut down all active and idle connections.
    pub async fn stop(&self) {
        let active: Vec<PooledConnHandle> = {
            let mut state = self.state.lock().await;
            state.active_slots.drain().map(|(_, c)| c).collect()
        };
        for conn in active {
            if let Some(handle) = conn.reader_task.lock().await.take() {
                handle.abort();
            }
            let _ = conn.client.close().await;
        }
        loop {
            let slot = {
                let mut state = self.state.lock().await;
                match state.idle_rx.try_recv() {
                    Ok(s) => {
                        state.idle_count = state.idle_count.saturating_sub(1);
                        Some(s)
                    }
                    Err(_) => None,
                }
            };
            match slot {
                Some(s) => {
                    let _ = s.client.close().await;
                }
                None => break,
            }
        }
    }

    async fn loop_id_for(&self, session_id: &str) -> (String, bool) {
        let Some(loop_id) = self.store.get_loop_id_for_session(session_id).await else {
            return (String::new(), false);
        };
        let loop_id = loop_id.trim().to_string();
        if loop_id.is_empty() || loop_id.starts_with("pending-") {
            return (String::new(), false);
        }
        (loop_id, true)
    }

    async fn persist_new_session(
        &self,
        session_id: &str,
        workspace_id: &str,
        user_id: &str,
        loop_id: &str,
    ) -> Result<()> {
        if self.store.get_session(session_id).await.is_none() {
            self.store
                .create_session(LoopSessionEntry {
                    session_id: session_id.to_string(),
                    workspace_id: workspace_id.to_string(),
                    user_id: user_id.to_string(),
                    loop_id: Some(loop_id.to_string()),
                    ..Default::default()
                })
                .await;
        } else {
            self.store.set_loop_id(session_id, loop_id).await;
        }
        Ok(())
    }

    async fn bootstrap_new(
        &self,
        conn: PooledConnHandle,
        session_id: &str,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<String> {
        let loop_id = (self.bootstrap)(
            conn.client.clone(),
            session_id.to_string(),
            workspace_id.to_string(),
            user_id.to_string(),
        )
        .await?;
        self.start_reader(&conn).await;
        Ok(loop_id)
    }

    async fn resume_and_reattach(&self, conn: PooledConnHandle, loop_id: &str) -> Result<()> {
        conn.client.connect().await?;
        conn.client.reattach_and_probe(loop_id).await?;
        self.start_reader(&conn).await;
        Ok(())
    }

    /// Launch `ReceiveMessages(256)` forwarder; detached from caller cancellation.
    async fn start_reader(&self, conn: &PooledConnHandle) {
        if let Some(handle) = conn.reader_task.lock().await.take() {
            handle.abort();
        }
        conn.reader_live.store(false, Ordering::SeqCst);

        let raw_rx = conn.client.receive_messages(256);
        let (tx, event_rx) = tokio::sync::mpsc::channel(256);
        *conn.event_rx.lock().await = Some(event_rx);
        conn.reader_live.store(true, Ordering::SeqCst);

        let reader_live_flag = Arc::clone(conn);
        let session_id = conn.session_id.clone();

        let handle = tokio::spawn(async move {
            let mut raw = raw_rx;
            while let Some(msg) = raw.recv().await {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
            reader_live_flag.reader_live.store(false, Ordering::SeqCst);
            tracing::debug!(session_id, "pool event reader stopped");
        });

        *conn.reader_task.lock().await = Some(handle);
    }
}

fn default_bootstrap_fn() -> BootstrapFn {
    Arc::new(|client, _session_id, workspace_id, user_id| {
        Box::pin(async move {
            let mut boot = BootstrapOptions::new();
            boot.workspace = Some(workspace_id);
            boot.user_id = Some(user_id);
            let ready = bootstrap_loop_session(&client, boot, None).await?;
            let loop_id = ready
                .get("loop_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if loop_id.is_empty() {
                return Err(Error::msg("bootstrap missing loop_id"));
            }
            Ok(loop_id)
        })
    })
}

impl From<ErrPoolExhausted> for Error {
    fn from(e: ErrPoolExhausted) -> Self {
        Error::msg(e.to_string())
    }
}

/// Build a protocol-1 `loop_input` notification envelope (Go `InputMessageForLoop`).
pub fn input_message_for_loop(
    text: &str,
    loop_id: &str,
    attachments: Option<Value>,
    opts: Option<&super::turn_runner::InputOpts>,
) -> Map<String, Value> {
    let mut params = Map::new();
    params.insert("content".into(), json!(text));
    if !loop_id.trim().is_empty() {
        params.insert("loop_id".into(), json!(loop_id));
    }
    if let Some(atts) = attachments {
        params.insert("attachments".into(), atts);
    }
    if let Some(o) = opts {
        if let Some(h) = o
            .intent_hint
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            params.insert("intent_hint".into(), json!(h));
        }
        if let Some(s) = o
            .preferred_subagent
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            params.insert("preferred_subagent".into(), json!(s));
        }
        if let Some(s) = o
            .intake_scope
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            params.insert("intake_scope".into(), json!(s));
        }
        if let Some(schema) = &o.response_schema {
            params.insert("response_schema".into(), schema.clone());
        }
        if let Some(n) = o
            .response_schema_name
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            params.insert("response_schema_name".into(), json!(n));
        }
        if let Some(strict) = o.response_schema_strict {
            params.insert("response_schema_strict".into(), json!(strict));
        }
    }
    let env = new_notification("loop_input", params);
    match serde_json::to_value(env) {
        Ok(Value::Object(m)) => m,
        _ => {
            let mut fallback = Map::new();
            fallback.insert("proto".into(), json!(crate::protocol::PROTO_VERSION));
            fallback.insert("type".into(), json!("notification"));
            fallback.insert("method".into(), json!("loop_input"));
            fallback.insert(
                "params".into(),
                Value::Object({
                    let mut p = Map::new();
                    p.insert("content".into(), json!(text));
                    if !loop_id.trim().is_empty() {
                        p.insert("loop_id".into(), json!(loop_id));
                    }
                    p
                }),
            );
            fallback
        }
    }
}
