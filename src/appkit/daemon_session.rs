//! Dual-socket DaemonSession for one conversation with turn streaming.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::sync::Mutex;

use crate::client::{unwrap_next_frame, Client, SendInputOptions};
use crate::errors::{Error, Result};
use crate::session::{bootstrap_loop_session, connect_with_retries, BootstrapOptions};
use crate::stream_terminal::{is_turn_end_custom_data, is_turn_progress_chunk, STREAM_END};
use crate::turn_boundary::{
    frame_turn_id, is_idle_terminal_allowed, is_turn_terminal_allowed, parse_turn_generation,
    turn_ids_match,
};

use super::chunk_filter::should_drop_stream_chunk_early;
use super::observability::TurnEventStats;

/// Default post-idle drain window.
pub const DEFAULT_POST_IDLE_DRAIN: Duration = Duration::from_millis(500);

/// Filters non-actionable stream chunks before yield.
pub type EarlyDropFn = Arc<dyn Fn(&[Value], &str, &Value) -> bool + Send + Sync>;

/// Options for constructing a DaemonSession.
#[derive(Clone)]
pub struct DaemonSessionOptions {
    /// Workspace path.
    pub workspace: Option<String>,
    /// Stream delivery mode.
    pub stream_delivery: String,
    /// Post-idle drain window.
    pub post_idle_drain: Duration,
    /// Optional early-drop filter (defaults to [`should_drop_stream_chunk_early`]).
    pub early_drop_fn: Option<EarlyDropFn>,
}

impl Default for DaemonSessionOptions {
    fn default() -> Self {
        Self {
            workspace: None,
            stream_delivery: "adaptive".into(),
            post_idle_drain: DEFAULT_POST_IDLE_DRAIN,
            early_drop_fn: None,
        }
    }
}

impl std::fmt::Debug for DaemonSessionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonSessionOptions")
            .field("workspace", &self.workspace)
            .field("stream_delivery", &self.stream_delivery)
            .field("post_idle_drain", &self.post_idle_drain)
            .field(
                "early_drop_fn",
                &self.early_drop_fn.as_ref().map(|_| "<fn>"),
            )
            .finish()
    }
}

/// Options for `send_turn`.
#[derive(Debug, Clone, Default)]
pub struct SendTurnOptions {
    /// Preferred subagent.
    pub preferred_subagent: Option<String>,
    /// Forced StrangeLoop intake scope (`trivial`|`simple`|`complex`).
    pub intake_scope: Option<String>,
    /// Model override.
    pub model: Option<String>,
    /// Model params.
    pub model_params: Option<Value>,
    /// Attachments.
    pub attachments: Option<Value>,
    /// Clarification mode.
    pub clarification_mode: Option<String>,
    /// Clarification answer.
    pub clarification_answer: bool,
    /// Intent hint.
    pub intent_hint: Option<String>,
    /// Interaction mode (`agent`|`ask`).
    pub interaction_mode: Option<String>,
    /// Autopilot rail id (pins the turn to a specific autopilot rail).
    pub autopilot_rail_id: Option<String>,
}

/// One streamed turn chunk.
#[derive(Debug, Clone)]
pub struct TurnChunk {
    /// Namespace path.
    pub namespace: Value,
    /// Mode (`messages`, `custom`, …).
    pub mode: String,
    /// Payload data.
    pub data: Value,
}

/// Dual-socket session: stream + lazy RPC sidecar.
pub struct DaemonSession {
    opts: DaemonSessionOptions,
    client: Client,
    rpc_client: Client,
    rpc_connected: Mutex<bool>,
    loop_id: Mutex<String>,
    read_lock: Mutex<()>,
    early_drop_fn: EarlyDropFn,
    /// Per-turn stream filtering counters.
    pub turn_event_stats: Mutex<TurnEventStats>,
    /// Last turn end state label.
    pub last_turn_end_state: Mutex<String>,
    /// Whether cancel was observed for the last turn.
    pub last_turn_cancel_seen: Mutex<bool>,
    /// Last turn error message.
    pub last_turn_error_message: Mutex<String>,
}

impl DaemonSession {
    /// Create a session for `ws_url`.
    pub fn new(ws_url: impl Into<String>, opts: Option<DaemonSessionOptions>) -> Self {
        let ws_url = ws_url.into();
        let opts = opts.unwrap_or_default();
        let early_drop_fn = opts.early_drop_fn.clone().unwrap_or_else(|| {
            Arc::new(|ns: &[Value], mode: &str, data: &Value| {
                should_drop_stream_chunk_early(ns, mode, data)
            })
        });
        Self {
            client: Client::new(&ws_url),
            rpc_client: Client::new(&ws_url),
            rpc_connected: Mutex::new(false),
            loop_id: Mutex::new(String::new()),
            read_lock: Mutex::new(()),
            early_drop_fn,
            turn_event_stats: Mutex::new(TurnEventStats::new()),
            last_turn_end_state: Mutex::new(String::new()),
            last_turn_cancel_seen: Mutex::new(false),
            last_turn_error_message: Mutex::new(String::new()),
            opts,
        }
    }

    /// Stream socket.
    pub fn stream_client(&self) -> &Client {
        &self.client
    }

    /// RPC sidecar socket (lazy-connected for list/cards/history).
    pub fn rpc_client(&self) -> &Client {
        &self.rpc_client
    }

    /// Active loop id.
    pub async fn loop_id(&self) -> String {
        self.loop_id.lock().await.clone()
    }

    /// Connect and bootstrap a loop (or resume).
    pub async fn connect(&self, resume_loop_id: Option<&str>) -> Result<Map<String, Value>> {
        connect_with_retries(&self.client, 40, Duration::from_millis(250)).await?;
        self.bootstrap_loop(resume_loop_id).await
    }

    async fn bootstrap_loop(&self, resume_loop_id: Option<&str>) -> Result<Map<String, Value>> {
        let mut boot = BootstrapOptions::new();
        boot.resume_loop_id = resume_loop_id.map(|s| s.to_string());
        boot.workspace = self.opts.workspace.clone();
        boot.stream_delivery = self.opts.stream_delivery.clone();
        let ready = bootstrap_loop_session(&self.client, boot, None).await?;
        if let Some(lid) = ready.get("loop_id").and_then(|v| v.as_str()) {
            *self.loop_id.lock().await = lid.to_string();
        }
        Ok(ready)
    }

    /// Start a fresh loop on the stream socket.
    pub async fn new_loop(&self) -> Result<Map<String, Value>> {
        self.bootstrap_loop(None).await
    }

    /// Switch to an existing loop.
    pub async fn switch_loop(&self, loop_id: &str) -> Result<Map<String, Value>> {
        self.bootstrap_loop(Some(loop_id)).await
    }

    /// Reconnect + reattach, or fresh bootstrap if stale.
    pub async fn ensure_connected(&self) -> Result<()> {
        if self.client.is_connection_alive() {
            return Ok(());
        }
        connect_with_retries(&self.client, 40, Duration::from_millis(250)).await?;
        let lid = self.loop_id().await;
        if lid.is_empty() {
            self.bootstrap_loop(None).await?;
            return Ok(());
        }
        match self.client.reattach_and_probe(&lid).await {
            Ok(()) => Ok(()),
            Err(Error::StaleLoop(_)) | Err(_) => {
                // Close RPC sidecar; fresh bootstrap.
                let _ = self.rpc_client.close().await;
                *self.rpc_connected.lock().await = false;
                self.bootstrap_loop(None).await?;
                Ok(())
            }
        }
    }

    /// Close both sockets.
    pub async fn close(&self) -> Result<()> {
        let _ = self.client.close().await;
        let _ = self.rpc_client.close().await;
        *self.rpc_connected.lock().await = false;
        Ok(())
    }

    /// Notify disconnect (loops keep running server-side).
    pub async fn detach(&self) -> Result<()> {
        self.client.notify("disconnect", Map::new()).await
    }

    /// Send a user turn on the stream socket.
    pub async fn send_turn(&self, text: &str, opts: Option<SendTurnOptions>) -> Result<()> {
        let loop_id = self.loop_id().await;
        if loop_id.is_empty() {
            return Err(Error::msg("no active loop session"));
        }
        let opts = opts.unwrap_or_default();
        let input = SendInputOptions {
            loop_id: Some(loop_id),
            preferred_subagent: opts.preferred_subagent,
            intake_scope: opts.intake_scope,
            model: opts.model,
            model_params: opts.model_params,
            attachments: opts.attachments,
            clarification_mode: opts.clarification_mode,
            clarification_answer: opts.clarification_answer,
            intent_hint: opts.intent_hint,
            interaction_mode: opts.interaction_mode,
            autopilot_rail_id: opts.autopilot_rail_id,
            ..Default::default()
        };
        self.client.send_input(text, input).await
    }

    /// Cancel active turn via `/cancel`.
    pub async fn cancel_active_turn(&self) -> Result<()> {
        let mut params = Map::new();
        params.insert("cmd".into(), json!("/cancel"));
        self.client.notify("slash_command", params).await
    }

    async fn ensure_rpc_connected(&self) -> Result<()> {
        let mut flag = self.rpc_connected.lock().await;
        if *flag && self.rpc_client.is_connected() {
            return Ok(());
        }
        connect_with_retries(&self.rpc_client, 5, Duration::from_millis(250)).await?;
        *flag = true;
        Ok(())
    }

    /// List loops via RPC sidecar.
    pub async fn list_loops(&self, limit: u32) -> Result<Map<String, Value>> {
        self.ensure_rpc_connected().await?;
        let lim = if limit == 0 { 20 } else { limit };
        self.rpc_client.loop_list(lim).await
    }

    /// Fetch history via RPC sidecar.
    pub async fn fetch_loop_history(&self, loop_id: &str) -> Result<Map<String, Value>> {
        self.ensure_rpc_connected().await?;
        self.rpc_client.loop_history_fetch(loop_id).await
    }

    /// Invoke a skill via the RPC sidecar socket.
    ///
    /// `interaction_mode` (`agent`|`ask`) is forwarded to the daemon when set.
    pub async fn invoke_skill(
        &self,
        skill: &str,
        args: &str,
        interaction_mode: Option<&str>,
    ) -> Result<Map<String, Value>> {
        self.ensure_rpc_connected().await?;
        self.rpc_client
            .invoke_skill(skill, args, interaction_mode)
            .await
    }

    /// Hot-swap the clarification mode on the running goal.
    ///
    /// Sends `loop_set_clarification_mode` with `mode` (`auto`|`manual`) and an
    /// optional `interaction_mode` (`bypass` swaps to the bypass graph; `None`
    /// keeps the default graph). Returns `true` when the swap landed on a live
    /// goal, `false` when no goal is currently running.
    pub async fn set_clarification_mode(
        &self,
        mode: &str,
        interaction_mode: Option<&str>,
    ) -> Result<bool> {
        let loop_id = self.loop_id().await;
        if loop_id.is_empty() {
            return Ok(false);
        }
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("mode".into(), json!(mode));
        if let Some(im) = interaction_mode {
            params.insert("interaction_mode".into(), json!(im));
        }
        self.ensure_rpc_connected().await?;
        let result = self
            .rpc_client
            .request(
                "loop_set_clarification_mode",
                params,
                Duration::from_secs(5),
            )
            .await?;
        let applied = result
            .get("applied")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(applied)
    }

    /// Stream turn chunks until idle / stream.end.
    ///
    /// `max_wait` of `None` means no absolute deadline.
    pub async fn iter_turn_chunks(&self, max_wait: Option<Duration>) -> Result<Vec<TurnChunk>> {
        let _guard = self.read_lock.lock().await;
        self.iter_turn_chunks_locked(max_wait).await
    }

    async fn iter_turn_chunks_locked(&self, max_wait: Option<Duration>) -> Result<Vec<TurnChunk>> {
        *self.last_turn_end_state.lock().await = String::new();
        *self.last_turn_error_message.lock().await = String::new();
        *self.last_turn_cancel_seen.lock().await = false;
        *self.turn_event_stats.lock().await = TurnEventStats::new();

        let inbound_baseline = self.client.inbound_dropped();
        let mut out = Vec::new();
        let mut query_started = false;
        let mut expected_loop_id = self.loop_id().await;
        let mut expected_turn_id: Option<String> = None;
        let mut turn_progress_seen = false;
        let mut cancel_seen = false;
        let absolute_deadline = max_wait.map(|d| tokio::time::Instant::now() + d);

        let _ = self.client.peel_stale_pending_control_events().await;

        loop {
            if let Some(deadline) = absolute_deadline {
                if tokio::time::Instant::now() > deadline {
                    let err = format!(
                        "turn timed out after {:?} (loop={})",
                        max_wait.unwrap_or_default(),
                        expected_loop_id
                    );
                    *self.last_turn_error_message.lock().await = err.clone();
                    self.finalize_inbound_dropped(inbound_baseline).await;
                    return Err(Error::msg(err));
                }
            }

            let ev = match self
                .client
                .read_event_with_timeout(Duration::from_millis(250))
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    self.finalize_inbound_dropped(inbound_baseline).await;
                    return Err(e);
                }
            };
            let Some(ev) = ev else {
                if query_started && !self.client.is_connection_alive() {
                    *self.last_turn_end_state.lock().await = "connection_lost".into();
                    self.finalize_inbound_dropped(inbound_baseline).await;
                    return Err(Error::msg("daemon connection lost"));
                }
                // Idle timeout on read — keep waiting unless absolute deadline.
                continue;
            };

            let mut frame = ev;
            let mut event_type = frame
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if event_type == "next" {
                frame = unwrap_next_frame(&frame);
                event_type = frame
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
            }

            let event_loop_id = frame
                .get("loop_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !expected_loop_id.is_empty()
                && !event_loop_id.is_empty()
                && event_loop_id != expected_loop_id
            {
                continue;
            }

            let ev_turn = frame_turn_id(Some(&frame));
            let status_state = if event_type == "status" {
                frame.get("state").and_then(|v| v.as_str()).unwrap_or("")
            } else {
                ""
            };
            let is_running_status = status_state == "running";
            let is_terminal_status = status_state == "idle" || status_state == "stopped";
            if let Some(ref expected) = expected_turn_id {
                if (event_type == "event" || event_type == "status") && !is_running_status {
                    if is_terminal_status {
                        if let Some(ref tid) = ev_turn {
                            if !turn_ids_match(Some(expected.as_str()), Some(tid.as_str())) {
                                continue;
                            }
                        }
                    } else if !turn_ids_match(Some(expected.as_str()), ev_turn.as_deref()) {
                        continue;
                    }
                }
            }

            if event_type == "error" {
                let msg = frame
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .or_else(|| frame.get("message").and_then(|m| m.as_str()))
                    .unwrap_or("daemon error")
                    .to_string();
                *self.last_turn_error_message.lock().await = msg.clone();
                self.finalize_inbound_dropped(inbound_baseline).await;
                return Err(Error::msg(msg));
            }

            if event_type == "status" {
                if let Some(lid) = frame.get("loop_id").and_then(|v| v.as_str()) {
                    if !lid.is_empty() {
                        *self.loop_id.lock().await = lid.to_string();
                        expected_loop_id = lid.to_string();
                    }
                }
                match status_state {
                    "running" => {
                        query_started = true;
                        if let Some(status_turn) = frame_turn_id(Some(&frame)) {
                            let new_gen = parse_turn_generation(Some(&status_turn));
                            let old_gen = parse_turn_generation(expected_turn_id.as_deref());
                            if expected_turn_id.is_none()
                                || (new_gen.is_some()
                                    && (old_gen.is_none() || new_gen.unwrap() >= old_gen.unwrap()))
                            {
                                if expected_turn_id.as_ref().is_some_and(|e| e != &status_turn) {
                                    turn_progress_seen = false;
                                }
                                expected_turn_id = Some(status_turn);
                            }
                        }
                    }
                    "stopped" if query_started => {
                        let stop_turn = frame_turn_id(Some(&frame));
                        if expected_turn_id.is_some()
                            && !turn_ids_match(expected_turn_id.as_deref(), stop_turn.as_deref())
                        {
                            continue;
                        }
                        *self.last_turn_end_state.lock().await = status_state.into();
                        self.drain_after_idle(&expected_loop_id, &mut out).await;
                        self.finalize_inbound_dropped(inbound_baseline).await;
                        return Ok(out);
                    }
                    "idle" if query_started => {
                        let idle_turn = frame_turn_id(Some(&frame));
                        if !is_idle_terminal_allowed(
                            expected_turn_id.as_deref(),
                            idle_turn.as_deref(),
                            query_started,
                            turn_progress_seen,
                            cancel_seen,
                        ) {
                            continue;
                        }
                        *self.last_turn_end_state.lock().await = status_state.into();
                        self.drain_after_idle(&expected_loop_id, &mut out).await;
                        self.finalize_inbound_dropped(inbound_baseline).await;
                        return Ok(out);
                    }
                    _ => {}
                }
                continue;
            }

            if event_type == "command_response" {
                let content = frame.get("content").and_then(|v| v.as_str()).unwrap_or("");
                if content.contains("Cancellation requested") {
                    cancel_seen = true;
                    *self.last_turn_cancel_seen.lock().await = true;
                }
                continue;
            }

            if event_type != "event" {
                continue;
            }

            let data = frame.get("data").cloned().unwrap_or(Value::Null);
            let namespace = frame
                .get("namespace")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            let mode = frame
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            {
                let mut stats = self.turn_event_stats.lock().await;
                stats.total += 1;
                match mode.as_str() {
                    "messages" => stats.messages += 1,
                    "updates" => stats.updates += 1,
                    "custom" => stats.custom += 1,
                    _ => {}
                }
            }

            let ns_slice: Vec<Value> = match &namespace {
                Value::Array(a) => a.clone(),
                _ => vec![],
            };
            if (self.early_drop_fn)(&ns_slice, &mode, &data) {
                self.turn_event_stats.lock().await.filtered_early += 1;
                continue;
            }

            if mode == "custom" && is_turn_end_custom_data(&data) {
                let data_turn = frame_turn_id(Some(&data)).or_else(|| ev_turn.clone());
                if !is_turn_terminal_allowed(
                    expected_turn_id.as_deref(),
                    data_turn.as_deref(),
                    query_started,
                    turn_progress_seen,
                ) {
                    self.turn_event_stats.lock().await.skipped += 1;
                    continue;
                }
            }

            if is_turn_progress_chunk(&mode, &data) {
                turn_progress_seen = true;
                if mode == "messages" {
                    self.turn_event_stats.lock().await.text_chunks += 1;
                }
            }

            // Tool call / result counting (namespace-based heuristic).
            let ns_str: String = ns_slice
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(".");
            if ns_str.contains("tool") {
                if ns_str.contains("result") {
                    self.turn_event_stats.lock().await.tool_results += 1;
                } else {
                    self.turn_event_stats.lock().await.tool_calls += 1;
                }
            }

            out.push(TurnChunk {
                namespace,
                mode: mode.clone(),
                data: data.clone(),
            });

            if mode == "custom" && is_turn_end_custom_data(&data) {
                let custom_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
                *self.last_turn_end_state.lock().await = if custom_type == STREAM_END {
                    "stream_end".into()
                } else {
                    "completed".into()
                };
                self.drain_after_idle(&expected_loop_id, &mut out).await;
                self.finalize_inbound_dropped(inbound_baseline).await;
                return Ok(out);
            }
        }
    }

    /// Record inbound frames dropped during this turn (delta from baseline).
    async fn finalize_inbound_dropped(&self, baseline: u64) {
        let now = self.client.inbound_dropped();
        self.turn_event_stats.lock().await.inbound_dropped = now.saturating_sub(baseline) as usize;
    }

    async fn drain_after_idle(&self, expected_loop_id: &str, out: &mut Vec<TurnChunk>) {
        let deadline = tokio::time::Instant::now() + self.opts.post_idle_drain;
        while tokio::time::Instant::now() < deadline {
            let ev = match self
                .client
                .read_event_with_timeout(Duration::from_millis(250))
                .await
            {
                Ok(Some(e)) => e,
                _ => return,
            };
            let mut frame = ev;
            let mut event_type = frame
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if event_type == "next" {
                frame = unwrap_next_frame(&frame);
                event_type = frame
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            let event_loop_id = frame.get("loop_id").and_then(|v| v.as_str()).unwrap_or("");
            if !expected_loop_id.is_empty()
                && !event_loop_id.is_empty()
                && event_loop_id != expected_loop_id
            {
                continue;
            }
            if event_type == "error" {
                return;
            }
            if event_type == "status" {
                if let Some(lid) = frame.get("loop_id").and_then(|v| v.as_str()) {
                    if !lid.is_empty() {
                        *self.loop_id.lock().await = lid.to_string();
                    }
                }
                continue;
            }
            if event_type != "event" {
                continue;
            }
            let data = frame.get("data").cloned().unwrap_or(Value::Null);
            let namespace = frame
                .get("namespace")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            let mode = frame
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ns_slice: Vec<Value> = match &namespace {
                Value::Array(a) => a.clone(),
                _ => vec![],
            };
            {
                let mut stats = self.turn_event_stats.lock().await;
                stats.total += 1;
                match mode.as_str() {
                    "messages" => stats.messages += 1,
                    "updates" => stats.updates += 1,
                    "custom" => stats.custom += 1,
                    _ => {}
                }
            }
            if (self.early_drop_fn)(&ns_slice, &mode, &data) {
                self.turn_event_stats.lock().await.filtered_early += 1;
                continue;
            }
            self.turn_event_stats.lock().await.post_idle_drained += 1;
            out.push(TurnChunk {
                namespace,
                mode,
                data,
            });
        }
    }
}
