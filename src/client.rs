//! Protocol-1 WebSocket transport client with mux and delivery_ack.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::errors::{
    ConnectionError, DaemonError, DisconnectCause, Error, Result, StaleLoopError, TimeoutError,
};
use crate::heartbeat::HeartbeatTracker;
use crate::protocol::{
    decode_message, expand_wire_messages, new_connection_init, new_notification, new_ping,
    new_pong, new_request, new_subscribe, new_unsubscribe, Envelope,
};
use crate::stream_terminal::{
    extract_loop_id_from_inbound, inbound_needs_delivery_ack, stale_pending_frame_label,
};

const MAX_INBOUND: usize = 20_000;
const DEFAULT_MAX_FRAME: usize = 10 * 1024 * 1024;

/// Options for Client construction.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// WebSocket URL.
    pub url: String,
    /// Max inbound queue depth before priority drop.
    pub max_inbound: usize,
    /// Max frame size hint (informational).
    pub max_frame_size: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:8765".into(),
            max_inbound: MAX_INBOUND,
            max_frame_size: DEFAULT_MAX_FRAME,
        }
    }
}

/// Options for `send_input` / loop_input notification.
#[derive(Debug, Clone, Default)]
pub struct SendInputOptions {
    /// Loop id (required for multi-loop).
    pub loop_id: Option<String>,
    /// Autonomous mode.
    pub autonomous: bool,
    /// Max iterations when autonomous.
    pub max_iterations: Option<u32>,
    /// Preferred subagent.
    pub preferred_subagent: Option<String>,
    /// Model override.
    pub model: Option<String>,
    /// Model params.
    pub model_params: Option<Value>,
    /// Router profile.
    pub router_profile: Option<String>,
    /// Attachments array.
    pub attachments: Option<Value>,
    /// Intent hint.
    pub intent_hint: Option<String>,
    /// Response schema.
    pub response_schema: Option<Value>,
    /// Response schema name.
    pub response_schema_name: Option<String>,
    /// Strict schema.
    pub response_schema_strict: Option<bool>,
    /// Clarification mode.
    pub clarification_mode: Option<String>,
    /// Clarification answer flag.
    pub clarification_answer: bool,
    /// Clarification answers payload.
    pub clarification_answers: Option<Value>,
}

type RpcWaiter = oneshot::Sender<std::result::Result<Value, DaemonError>>;
type StreamDegradedCallback = Arc<dyn Fn(u64, String) + Send + Sync>;

struct Shared {
    url: String,
    max_inbound: usize,
    write_tx: Mutex<Option<mpsc::UnboundedSender<Message>>>,
    rpc_waiters: Mutex<HashMap<String, RpcWaiter>>,
    inbound: Mutex<VecDeque<Value>>,
    inbound_notify: Notify,
    connected: AtomicBool,
    reader_alive: AtomicBool,
    disconnect_cause: Mutex<Option<DisconnectCause>>,
    disconnect_notify: Notify,
    inbound_dropped: AtomicU64,
    delivery_seq: Mutex<HashMap<String, u64>>,
    heartbeat_interval_ms: Mutex<Option<u64>>,
    stream_degraded_cb: std::sync::Mutex<Option<StreamDegradedCallback>>,
    degraded_notified: AtomicBool,
    heartbeat_tracker: std::sync::Mutex<Option<Arc<HeartbeatTracker>>>,
}

/// Long-lived protocol-1 WebSocket client.
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}

impl Client {
    /// Create a client for `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self::with_config(ClientConfig {
            url: url.into(),
            ..Default::default()
        })
    }

    /// Create with explicit config.
    pub fn with_config(cfg: ClientConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                url: cfg.url,
                max_inbound: cfg.max_inbound,
                write_tx: Mutex::new(None),
                rpc_waiters: Mutex::new(HashMap::new()),
                inbound: Mutex::new(VecDeque::new()),
                inbound_notify: Notify::new(),
                connected: AtomicBool::new(false),
                reader_alive: AtomicBool::new(false),
                disconnect_cause: Mutex::new(None),
                disconnect_notify: Notify::new(),
                inbound_dropped: AtomicU64::new(0),
                delivery_seq: Mutex::new(HashMap::new()),
                heartbeat_interval_ms: Mutex::new(None),
                stream_degraded_cb: std::sync::Mutex::new(None),
                degraded_notified: AtomicBool::new(false),
                heartbeat_tracker: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Daemon URL.
    pub fn url(&self) -> &str {
        &self.shared.url
    }

    /// Whether the socket is connected.
    pub fn is_connected(&self) -> bool {
        self.shared.connected.load(Ordering::SeqCst)
    }

    /// Whether the reader task is alive.
    pub fn is_connection_alive(&self) -> bool {
        self.shared.reader_alive.load(Ordering::SeqCst)
            && self.shared.connected.load(Ordering::SeqCst)
    }

    /// Count of dropped inbound frames under backpressure.
    pub fn inbound_dropped(&self) -> u64 {
        self.shared.inbound_dropped.load(Ordering::SeqCst)
    }

    /// Register a hook invoked on the first inbound overflow drop.
    pub fn set_stream_degraded_callback(&self, cb: Option<Arc<dyn Fn(u64, String) + Send + Sync>>) {
        *self
            .shared
            .stream_degraded_cb
            .lock()
            .expect("stream_degraded lock") = cb;
        self.shared.degraded_notified.store(false, Ordering::SeqCst);
    }

    /// Enable heartbeat tracking with the default 15s alive threshold.
    pub fn enable_heartbeat_tracking(&self) -> Arc<HeartbeatTracker> {
        self.enable_heartbeat_tracking_with_threshold(Duration::from_secs(15))
    }

    /// Enable heartbeat tracking with a custom alive threshold.
    pub fn enable_heartbeat_tracking_with_threshold(
        &self,
        threshold: Duration,
    ) -> Arc<HeartbeatTracker> {
        let tracker = Arc::new(HeartbeatTracker::with_threshold(threshold));
        *self
            .shared
            .heartbeat_tracker
            .lock()
            .expect("heartbeat lock") = Some(tracker.clone());
        tracker
    }

    /// Disable heartbeat tracking.
    pub fn disable_heartbeat_tracking(&self) {
        *self
            .shared
            .heartbeat_tracker
            .lock()
            .expect("heartbeat lock") = None;
    }

    /// Current heartbeat tracker, if enabled.
    pub fn heartbeat_tracker(&self) -> Option<Arc<HeartbeatTracker>> {
        self.shared
            .heartbeat_tracker
            .lock()
            .expect("heartbeat lock")
            .clone()
    }

    /// Whether the tracked daemon is considered alive (true if tracking disabled).
    pub fn is_daemon_alive(&self) -> bool {
        match self
            .shared
            .heartbeat_tracker
            .lock()
            .expect("heartbeat lock")
            .as_ref()
        {
            Some(t) => t.get_health().is_alive,
            None => true,
        }
    }

    /// Disconnect cause if disconnected.
    pub async fn disconnect_cause(&self) -> Option<DisconnectCause> {
        *self.shared.disconnect_cause.lock().await
    }

    /// Wait until disconnected.
    pub async fn wait_disconnected(&self) -> DisconnectCause {
        loop {
            if let Some(c) = *self.shared.disconnect_cause.lock().await {
                return c;
            }
            self.shared.disconnect_notify.notified().await;
        }
    }

    /// Dial + handshake (`connection_init` / `connection_ack` ready).
    pub async fn connect(&self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        let (ws, _) = connect_async(&self.shared.url).await.map_err(|e| {
            ConnectionError::new(
                &self.shared.url,
                1,
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
            )
        })?;
        let (mut write, mut read) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        {
            let mut slot = self.shared.write_tx.lock().await;
            *slot = Some(tx);
        }
        self.shared.connected.store(true, Ordering::SeqCst);
        self.shared.reader_alive.store(true, Ordering::SeqCst);
        *self.shared.disconnect_cause.lock().await = None;

        let shared_r2 = self.shared.clone();
        let client_for_hb = self.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = write.close().await;
        });

        tokio::spawn(async move {
            while let Some(item) = read.next().await {
                match item {
                    Ok(Message::Text(text)) => {
                        if let Ok(raw) = decode_message(&text) {
                            for msg in expand_wire_messages(raw) {
                                shared_r2.route_inbound(msg).await;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = shared_r2.send_raw(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            shared_r2.mark_disconnected(DisconnectCause::Unclean).await;
        });

        // Handshake
        self.send_envelope(new_connection_init()).await?;
        let ack = self.wait_connection_ack(Duration::from_secs(15)).await?;
        if let Some(interval) = ack
            .get("result")
            .and_then(|r| r.get("heartbeat_interval_ms"))
            .and_then(|v| v.as_u64())
        {
            *self.shared.heartbeat_interval_ms.lock().await = Some(interval);
            if interval > 0 {
                let hb_client = client_for_hb;
                tokio::spawn(async move {
                    let period = Duration::from_millis(interval.max(5_000));
                    while hb_client.is_connection_alive() {
                        sleep(period).await;
                        if !hb_client.is_connection_alive() {
                            break;
                        }
                        let _ = hb_client.send_envelope(new_ping()).await;
                    }
                });
            }
        }
        Ok(())
    }

    async fn wait_connection_ack(&self, overall: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + overall;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(TimeoutError::new("connection_ack", format!("{overall:?}")).into());
            }
            let msg = self
                .read_event_with_timeout(remaining.min(Duration::from_secs(2)))
                .await?;
            let Some(msg) = msg else {
                continue;
            };
            let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if msg_type == "status" {
                continue;
            }
            if msg_type == "error" {
                let err = parse_daemon_error(&msg);
                // Retryable readiness codes.
                if matches!(err.code, -32003..=-32001) {
                    sleep(Duration::from_millis(50)).await;
                    self.send_envelope(new_connection_init()).await?;
                    continue;
                }
                return Err(err.into());
            }
            if msg_type == "connection_ack" {
                let state = msg
                    .get("result")
                    .and_then(|r| r.get("readiness_state"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if state == "starting" || state == "warming" {
                    sleep(Duration::from_millis(50)).await;
                    self.send_envelope(new_connection_init()).await?;
                    continue;
                }
                if state == "ready" || state.is_empty() {
                    return Ok(msg);
                }
                return Err(Error::protocol(format!(
                    "connection_ack readiness_state={state}"
                )));
            }
        }
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<()> {
        if self.is_connected() {
            let _ = self.notify("disconnect", Map::new()).await;
        }
        {
            let mut tx = self.shared.write_tx.lock().await;
            *tx = None;
        }
        self.shared.mark_disconnected(DisconnectCause::Clean).await;
        Ok(())
    }

    /// Re-dial and re-handshake after a connection drop.
    ///
    /// Does not re-establish loop subscriptions; follow with
    /// [`Self::reattach_and_probe`] to resume a loop session.
    pub async fn reconnect(&self) -> Result<()> {
        {
            let mut tx = self.shared.write_tx.lock().await;
            *tx = None;
        }
        self.shared.connected.store(false, Ordering::SeqCst);
        self.shared.reader_alive.store(false, Ordering::SeqCst);
        *self.shared.disconnect_cause.lock().await = None;
        self.shared.rpc_waiters.lock().await.clear();
        self.shared.inbound.lock().await.clear();
        self.shared.degraded_notified.store(false, Ordering::SeqCst);
        self.connect().await
    }

    /// Send a raw envelope.
    pub async fn send_envelope(&self, env: Envelope) -> Result<()> {
        let text = env.to_wire_json()?;
        self.shared.send_raw(Message::Text(text.into())).await
    }

    /// Fire-and-forget notification.
    pub async fn notify(&self, method: &str, params: Map<String, Value>) -> Result<()> {
        self.send_envelope(new_notification(method, params)).await
    }

    /// RPC request correlated by id.
    pub async fn request(
        &self,
        method: &str,
        params: Map<String, Value>,
        req_timeout: Duration,
    ) -> Result<Map<String, Value>> {
        let env = new_request(method, params);
        let id = env.id.clone().unwrap_or_default();
        let (tx, rx) = oneshot::channel();
        self.shared.rpc_waiters.lock().await.insert(id.clone(), tx);
        if let Err(e) = self.send_envelope(env).await {
            self.shared.rpc_waiters.lock().await.remove(&id);
            return Err(e);
        }
        match timeout(req_timeout, rx).await {
            Ok(Ok(Ok(Value::Object(m)))) => Ok(m),
            Ok(Ok(Ok(other))) => {
                let mut m = Map::new();
                m.insert("result".into(), other);
                Ok(m)
            }
            Ok(Ok(Err(de))) => Err(de.into()),
            Ok(Err(_)) => Err(Error::protocol("rpc waiter dropped")),
            Err(_) => {
                self.shared.rpc_waiters.lock().await.remove(&id);
                Err(TimeoutError::new(method, format!("{req_timeout:?}")).into())
            }
        }
    }

    /// Legacy-shaped request map (`type` → method) like Go RequestResponse.
    pub async fn request_response(
        &self,
        payload: Map<String, Value>,
        fallback_method: &str,
        req_timeout: Duration,
    ) -> Result<Map<String, Value>> {
        let method = payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or(fallback_method)
            .to_string();
        let mut params = Map::new();
        for (k, v) in payload {
            if k == "type" || k == "request_id" {
                continue;
            }
            params.insert(k, v);
        }
        self.request(&method, params, req_timeout).await
    }

    /// Subscribe; returns subscription id.
    pub async fn subscribe(
        &self,
        method: &str,
        params: Map<String, Value>,
        req_timeout: Duration,
    ) -> Result<String> {
        let env = new_subscribe(method, params);
        let id = env.id.clone().unwrap_or_default();
        // Subscribe confirmation may arrive as next/complete; we treat send success as ok
        // and rely on subsequent events. Still register a short waiter for error.
        let (tx, rx) = oneshot::channel();
        self.shared.rpc_waiters.lock().await.insert(id.clone(), tx);
        self.send_envelope(env).await?;
        // Don't block long — many daemons only send `next` without a response.
        match timeout(req_timeout.min(Duration::from_millis(500)), rx).await {
            Ok(Ok(Err(de))) => return Err(de.into()),
            Ok(Ok(Ok(_))) => {}
            _ => {
                self.shared.rpc_waiters.lock().await.remove(&id);
            }
        }
        Ok(id)
    }

    /// Unsubscribe by id.
    pub async fn unsubscribe(&self, id: &str) -> Result<()> {
        self.send_envelope(new_unsubscribe(id)).await
    }

    /// Read next inbound app event (blocks).
    pub async fn read_event(&self) -> Result<Option<Value>> {
        loop {
            {
                let mut q = self.shared.inbound.lock().await;
                if let Some(v) = q.pop_front() {
                    return Ok(Some(v));
                }
            }
            if !self.is_connection_alive() {
                return Ok(None);
            }
            self.shared.inbound_notify.notified().await;
        }
    }

    /// Read with timeout; `None` on timeout.
    pub async fn read_event_with_timeout(&self, dur: Duration) -> Result<Option<Value>> {
        match timeout(dur, self.read_event()).await {
            Ok(r) => r,
            Err(_) => Ok(None),
        }
    }

    /// Clear pending inbound events.
    pub async fn clear_pending_events(&self) {
        self.shared.inbound.lock().await.clear();
    }

    /// Peel stale pending control frames at turn start.
    pub async fn peel_stale_pending_control_events(&self) -> Vec<String> {
        let mut labels = Vec::new();
        let mut kept = VecDeque::new();
        let mut q = self.shared.inbound.lock().await;
        while let Some(ev) = q.pop_front() {
            if let Some(label) = stale_pending_frame_label(&ev) {
                labels.push(label);
            } else {
                kept.push_back(ev);
            }
        }
        *q = kept;
        labels
    }

    /// Notify `loop_input`.
    pub async fn send_input(&self, text: &str, opts: SendInputOptions) -> Result<()> {
        let mut params = Map::new();
        params.insert("content".into(), json!(text));
        if let Some(lid) = opts.loop_id {
            params.insert("loop_id".into(), json!(lid));
        }
        if opts.autonomous {
            params.insert("autonomous".into(), json!(true));
        }
        if let Some(n) = opts.max_iterations {
            params.insert("max_iterations".into(), json!(n));
        }
        if let Some(v) = opts.preferred_subagent {
            params.insert("preferred_subagent".into(), json!(v));
        }
        if let Some(v) = opts.model {
            params.insert("model".into(), json!(v));
        }
        if let Some(v) = opts.model_params {
            params.insert("model_params".into(), v);
        }
        if let Some(v) = opts.router_profile {
            params.insert("router_profile".into(), json!(v));
        }
        if let Some(v) = opts.attachments {
            params.insert("attachments".into(), v);
        }
        if let Some(v) = opts.intent_hint {
            params.insert("intent_hint".into(), json!(v));
        }
        if let Some(v) = opts.response_schema {
            params.insert("response_schema".into(), v);
        }
        if let Some(v) = opts.response_schema_name {
            params.insert("response_schema_name".into(), json!(v));
        }
        if let Some(v) = opts.response_schema_strict {
            params.insert("response_schema_strict".into(), json!(v));
        }
        if let Some(v) = opts.clarification_mode {
            params.insert("clarification_mode".into(), json!(v));
        }
        if opts.clarification_answer {
            params.insert("clarification_answer".into(), json!(true));
        }
        if let Some(v) = opts.clarification_answers {
            params.insert("clarification_answers".into(), v);
        }
        self.notify("loop_input", params).await
    }

    /// `loop_new` RPC.
    pub async fn loop_new(&self, params: Map<String, Value>) -> Result<Map<String, Value>> {
        self.request("loop_new", params, Duration::from_secs(30))
            .await
    }

    /// `loop_list`.
    pub async fn loop_list(&self, limit: u32) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("limit".into(), json!(limit));
        self.request("loop_list", params, Duration::from_secs(15))
            .await
    }

    /// `loop_get`.
    pub async fn loop_get(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_get", params, Duration::from_secs(15))
            .await
    }

    /// `loop_reattach`.
    pub async fn loop_reattach(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_reattach", params, Duration::from_secs(15))
            .await
    }

    /// Subscribe to `loop_events`.
    pub async fn loop_subscribe(&self, loop_id: &str, stream_delivery: &str) -> Result<String> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("stream_delivery".into(), json!(stream_delivery));
        params.insert("wire_tier".into(), json!("full"));
        self.subscribe("loop_events", params, Duration::from_secs(30))
            .await
    }

    /// `loop_cards_fetch`.
    pub async fn loop_cards_fetch(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_cards_fetch", params, Duration::from_secs(30))
            .await
    }

    /// `loop_history_fetch`.
    pub async fn loop_history_fetch(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_history_fetch", params, Duration::from_secs(30))
            .await
    }

    /// `loop_messages`.
    pub async fn loop_messages(
        &self,
        loop_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("limit".into(), json!(limit));
        params.insert("offset".into(), json!(offset));
        self.request("loop_messages", params, Duration::from_secs(10))
            .await
    }

    /// `loop_state_get`.
    pub async fn loop_state_get(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_state_get", params, Duration::from_secs(30))
            .await
    }

    /// `loop_state_update`.
    pub async fn loop_state_update(
        &self,
        loop_id: &str,
        state: Map<String, Value>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("state".into(), Value::Object(state));
        self.request("loop_state_update", params, Duration::from_secs(30))
            .await
    }

    /// `loop_tree`.
    pub async fn loop_tree(
        &self,
        loop_id: &str,
        format: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if let Some(f) = format {
            if !f.is_empty() {
                params.insert("format".into(), json!(f));
            }
        }
        self.request("loop_tree", params, Duration::from_secs(15))
            .await
    }

    /// `loop_prune`.
    pub async fn loop_prune(
        &self,
        loop_id: &str,
        retention_days: Option<i32>,
        dry_run: bool,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if let Some(d) = retention_days {
            if d > 0 {
                params.insert("retention_days".into(), json!(d));
            }
        }
        if dry_run {
            params.insert("dry_run".into(), json!(true));
        }
        self.request("loop_prune", params, Duration::from_secs(30))
            .await
    }

    /// `loop_delete`.
    pub async fn loop_delete(&self, loop_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.request("loop_delete", params, Duration::from_secs(10))
            .await
    }

    /// Detach from a loop by unsubscribing (`subscription_id` from `loop_subscribe`).
    pub async fn loop_detach(&self, subscription_id: &str) -> Result<()> {
        self.unsubscribe(subscription_id).await
    }

    /// Authenticate with access/secret keys.
    pub async fn authenticate(
        &self,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("access_key".into(), json!(access_key));
        params.insert("secret_key".into(), json!(secret_key));
        self.request("auth", params, Duration::from_secs(15)).await
    }

    /// Refresh auth token.
    pub async fn refresh_auth_token(&self, refresh_token: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("refresh_token".into(), json!(refresh_token));
        self.request("auth_refresh", params, Duration::from_secs(15))
            .await
    }

    /// `job_create` on this long-lived connection.
    pub async fn job_create(
        &self,
        goal: &str,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal".into(), json!(goal));
        if let Some(ws) = workspace {
            params.insert("workspace".into(), json!(ws));
        }
        self.request("job_create", params, Duration::from_secs(30))
            .await
    }

    /// `job_status`.
    pub async fn job_status(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("job_status", params, Duration::from_secs(15))
            .await
    }

    /// `job_pause`.
    pub async fn job_pause(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("job_pause", params, Duration::from_secs(15))
            .await
    }

    /// `job_resume`.
    pub async fn job_resume(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("job_resume", params, Duration::from_secs(15))
            .await
    }

    /// `job_cancel`.
    pub async fn job_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("job_cancel", params, Duration::from_secs(15))
            .await
    }

    /// `job_dag`.
    pub async fn job_dag(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("job_dag", params, Duration::from_secs(15))
            .await
    }

    /// `job_guidance`.
    pub async fn job_guidance(
        &self,
        job_id: &str,
        content: &str,
        goal_id: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        params.insert("content".into(), json!(content));
        if let Some(g) = goal_id {
            params.insert("goal_id".into(), json!(g));
        }
        self.request("job_guidance", params, Duration::from_secs(30))
            .await
    }

    /// `autopilot_status`.
    pub async fn autopilot_status(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_status", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_submit`.
    pub async fn autopilot_submit(
        &self,
        description: &str,
        priority: i32,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("description".into(), json!(description));
        params.insert("priority".into(), json!(priority));
        if let Some(ws) = workspace {
            params.insert("workspace".into(), json!(ws));
        }
        self.request("autopilot_submit", params, Duration::from_secs(30))
            .await
    }

    /// `autopilot_list_goals`.
    pub async fn autopilot_list_goals(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_list_goals", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_get_goal`.
    pub async fn autopilot_get_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.request("autopilot_get_goal", params, Duration::from_secs(15))
            .await
    }

    /// `autopilot_cancel_goal`.
    pub async fn autopilot_cancel_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.request("autopilot_cancel_goal", params, Duration::from_secs(15))
            .await
    }

    /// `autopilot_cancel_all`.
    pub async fn autopilot_cancel_all(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_cancel_all", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_wake`.
    pub async fn autopilot_wake(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_wake", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_dream`.
    pub async fn autopilot_dream(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_dream", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_resume`.
    pub async fn autopilot_resume(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.request("autopilot_resume", params, Duration::from_secs(15))
            .await
    }

    /// `autopilot_list_jobs`.
    pub async fn autopilot_list_jobs(&self) -> Result<Map<String, Value>> {
        self.request("autopilot_list_jobs", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `autopilot_get_job`.
    pub async fn autopilot_get_job(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("autopilot_get_job", params, Duration::from_secs(15))
            .await
    }

    /// Subscribe to `autopilot_events` (long-lived worker stream).
    pub async fn autopilot_subscribe(&self) -> Result<String> {
        self.subscribe("autopilot_events", Map::new(), Duration::from_secs(15))
            .await
    }

    /// Unsubscribe from an autopilot events subscription.
    pub async fn autopilot_unsubscribe(&self, subscription_id: &str) -> Result<()> {
        self.unsubscribe(subscription_id).await
    }

    /// `cron_add`.
    pub async fn cron_add(&self, text: &str, priority: Option<i32>) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("text".into(), json!(text));
        if let Some(p) = priority {
            params.insert("priority".into(), json!(p));
        }
        self.request("cron_add", params, Duration::from_secs(15))
            .await
    }

    /// `cron_list`.
    pub async fn cron_list(&self, status: Option<&str>) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        if let Some(s) = status {
            params.insert("status".into(), json!(s));
        }
        self.request("cron_list", params, Duration::from_secs(15))
            .await
    }

    /// `cron_show`.
    pub async fn cron_show(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("cron_show", params, Duration::from_secs(15))
            .await
    }

    /// `cron_cancel`.
    pub async fn cron_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.request("cron_cancel", params, Duration::from_secs(15))
            .await
    }

    /// `memory_stats`.
    pub async fn memory_stats(&self, mode: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("mode".into(), json!(mode));
        self.request("memory_stats", params, Duration::from_secs(15))
            .await
    }

    /// `skills_list`.
    pub async fn list_skills(&self) -> Result<Map<String, Value>> {
        self.request("skills_list", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `models_list`.
    pub async fn list_models(&self) -> Result<Map<String, Value>> {
        self.request("models_list", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `mcp_status`.
    pub async fn mcp_status(&self) -> Result<Map<String, Value>> {
        self.request("mcp_status", Map::new(), Duration::from_secs(15))
            .await
    }

    /// `daemon_status`.
    pub async fn fetch_daemon_status(&self) -> Result<Map<String, Value>> {
        self.request("daemon_status", Map::new(), Duration::from_secs(10))
            .await
    }

    /// `invoke_skill` on this connection (stream socket for turn enqueue).
    pub async fn invoke_skill(&self, skill: &str, args: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("skill".into(), json!(skill));
        if !args.is_empty() {
            params.insert("args".into(), json!(args));
        }
        self.request("invoke_skill", params, Duration::from_secs(120))
            .await
    }

    /// Reattach + subscribe + `loop_get` probe.
    pub async fn reattach_and_probe(&self, loop_id: &str) -> Result<()> {
        self.loop_reattach(loop_id).await?;
        self.loop_subscribe(loop_id, "adaptive").await?;
        match self.loop_get(loop_id).await {
            Ok(_) => Ok(()),
            Err(Error::Daemon(de)) if de.code == -32200 => {
                Err(StaleLoopError::new(loop_id, Some(Box::new(de))).into())
            }
            Err(e) => Err(StaleLoopError::new(loop_id, Some(Box::new(e))).into()),
        }
    }
}

impl Shared {
    async fn send_raw(&self, msg: Message) -> Result<()> {
        let tx = self.write_tx.lock().await;
        let Some(tx) = tx.as_ref() else {
            return Err(Error::protocol("not connected"));
        };
        tx.send(msg)
            .map_err(|_| Error::protocol("write channel closed"))?;
        Ok(())
    }

    async fn mark_disconnected(&self, cause: DisconnectCause) {
        self.connected.store(false, Ordering::SeqCst);
        self.reader_alive.store(false, Ordering::SeqCst);
        {
            let mut slot = self.write_tx.lock().await;
            *slot = None;
        }
        {
            let mut c = self.disconnect_cause.lock().await;
            if c.is_none() {
                *c = Some(cause);
            }
        }
        // Fail pending RPCs.
        let mut waiters = self.rpc_waiters.lock().await;
        for (_, tx) in waiters.drain() {
            let _ = tx.send(Err(DaemonError::new(-1, "disconnected")));
        }
        self.inbound_notify.notify_waiters();
        self.disconnect_notify.notify_waiters();
    }

    async fn route_inbound(&self, msg: Value) {
        // Auto pong for app-level ping.
        if msg.get("type").and_then(|v| v.as_str()) == Some("ping") {
            let _ = self
                .send_raw(Message::Text(
                    new_pong().to_wire_json().unwrap_or_default().into(),
                ))
                .await;
            return;
        }

        if msg.get("type").and_then(|v| v.as_str()) == Some("pong") {
            if let Some(tracker) = self.heartbeat_tracker.lock().expect("hb").as_ref() {
                tracker.note_pong();
            }
            return;
        }

        // Daemon heartbeat custom events (namespace / type heuristics).
        let ns = msg
            .get("namespace")
            .or_else(|| msg.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if ns.contains("heartbeat") {
            if let Some(tracker) = self.heartbeat_tracker.lock().expect("hb").as_ref() {
                tracker.update(msg.get("data"));
            }
        }

        if inbound_needs_delivery_ack(&msg) {
            let loop_id = extract_loop_id_from_inbound(&msg);
            if !loop_id.is_empty() {
                let seq = {
                    let mut map = self.delivery_seq.lock().await;
                    let e = map.entry(loop_id.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                let mut params = Map::new();
                params.insert("loop_id".into(), json!(loop_id));
                params.insert("seq".into(), json!(seq));
                let _ = self
                    .send_raw(Message::Text(
                        new_notification("delivery_ack", params)
                            .to_wire_json()
                            .unwrap_or_default()
                            .into(),
                    ))
                    .await;
            }
        }

        let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let id = msg
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if matches!(msg_type, "response" | "error") && !id.is_empty() {
            if let Some(waiter) = self.rpc_waiters.lock().await.remove(&id) {
                let result = if msg_type == "error" {
                    Err(parse_daemon_error(&msg))
                } else {
                    Ok(msg
                        .get("result")
                        .cloned()
                        .unwrap_or(Value::Object(Map::new())))
                };
                let _ = waiter.send(result);
                return;
            }
        }

        // Push to inbound queue with priority backpressure.
        let critical = is_critical_inbound(&msg);
        let mut q = self.inbound.lock().await;
        if q.len() >= self.max_inbound {
            if critical {
                if let Some(pos) = q.iter().position(|m| !is_critical_inbound(m)) {
                    q.remove(pos);
                    self.note_inbound_drop("priority_evict");
                } else {
                    self.note_inbound_drop("queue_full_critical");
                    return;
                }
            } else {
                self.note_inbound_drop("queue_full");
                return;
            }
        }
        q.push_back(msg);
        self.inbound_notify.notify_one();
    }

    fn note_inbound_drop(&self, reason: &str) {
        let n = self.inbound_dropped.fetch_add(1, Ordering::SeqCst) + 1;
        if !self.degraded_notified.swap(true, Ordering::SeqCst) {
            if let Some(cb) = self.stream_degraded_cb.lock().expect("cb").as_ref() {
                cb(n, reason.to_string());
            }
        }
    }
}

fn is_critical_inbound(msg: &Value) -> bool {
    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
    matches!(
        t,
        "status" | "error" | "complete" | "connection_ack" | "response"
    ) || inbound_needs_delivery_ack(msg)
}

fn parse_daemon_error(msg: &Value) -> DaemonError {
    if let Some(err) = msg.get("error") {
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("daemon error")
            .to_string();
        let data = err.get("data").cloned();
        let mut de = DaemonError::new(code, message);
        if let Some(d) = data {
            de = de.with_data(d);
        }
        return de;
    }
    DaemonError::new(
        -1,
        msg.get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("daemon error"),
    )
}

/// Re-export unwrap helper for appkit.
pub use crate::protocol::unwrap_next as unwrap_next_frame;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_input_options_default() {
        let o = SendInputOptions::default();
        assert!(!o.autonomous);
        assert!(o.loop_id.is_none());
    }
}
