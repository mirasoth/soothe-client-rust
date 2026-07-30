//! TurnRunner: single-flight execute over ConnectionPool (Go appkit parity).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

use crate::client::{unwrap_next_frame, SendInputOptions};
use crate::errors::{Error, Result};
use crate::intent_hints::validate_loop_input_intent_hint;
use crate::stream_terminal::is_turn_end_custom_data;

use super::attachments::{compact_attachments, CompactImageOptions};
use super::broadcaster::{SseBroadcaster, SseEvent};
use super::classifier::{ChatEventTerminal, EventClassifier};
use super::loop_session_store::LoopSessionStore;
use super::pool::ConnectionPool;
use super::query_gate::{CancelFn, QueryGate, SendCancelFn};
use super::turn_boundary::{is_daemon_turn_end_event, TurnBoundary};

/// Timeout policy for idle / query / stream-close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeoutPolicy {
    /// Fail the turn.
    #[default]
    Fail,
    /// Soft-complete with whatever was collected.
    SoftComplete,
}

/// Turn runner configuration.
#[derive(Debug, Clone)]
pub struct TurnConfig {
    /// Absolute query timeout.
    pub query_timeout: Duration,
    /// Idle silence timeout (0 = off).
    pub idle_timeout: Duration,
    /// Floor idle timeout when attachments are present (0 = no floor).
    pub min_idle_timeout_with_attachments: Duration,
    /// Idle timeout policy.
    pub on_idle_timeout: TimeoutPolicy,
    /// Query timeout policy.
    pub on_query_timeout: TimeoutPolicy,
    /// Stream close policy.
    pub on_stream_close: TimeoutPolicy,
    /// Compact image attachments before send.
    pub compact_attachments_before_send: bool,
    /// Options for attachment compaction.
    pub compact_image_opts: Option<CompactImageOptions>,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30 * 60),
            idle_timeout: Duration::ZERO,
            min_idle_timeout_with_attachments: Duration::ZERO,
            on_idle_timeout: TimeoutPolicy::Fail,
            on_query_timeout: TimeoutPolicy::Fail,
            on_stream_close: TimeoutPolicy::Fail,
            compact_attachments_before_send: false,
            compact_image_opts: None,
        }
    }
}

/// Optional input knobs for TurnRunner.
#[derive(Debug, Clone, Default)]
pub struct InputOpts {
    /// Intent hint.
    pub intent_hint: Option<String>,
    /// Preferred subagent.
    pub preferred_subagent: Option<String>,
    /// Response schema.
    pub response_schema: Option<Value>,
    /// Schema name.
    pub response_schema_name: Option<String>,
    /// Strict schema.
    pub response_schema_strict: Option<bool>,
}

type OnCompleteFn = Arc<dyn Fn(&str, &str, &str, &str, i64) + Send + Sync>;
type OnErrorFn = Arc<dyn Fn(&str, &str, &Error) + Send + Sync>;
type ErrorDataFn = Arc<dyn Fn(&Error) -> Value + Send + Sync>;
type InputBuilderFn =
    Arc<dyn Fn(&str, &str, Option<&Value>, Option<&InputOpts>) -> Map<String, Value> + Send + Sync>;

/// Executes a turn against a pooled connection.
pub struct TurnRunner<S: LoopSessionStore> {
    pool: Arc<ConnectionPool<S>>,
    gate: Arc<QueryGate>,
    classifier: EventClassifier,
    store: Arc<S>,
    broadcaster: Option<Arc<SseBroadcaster>>,
    cfg: TurnConfig,
    on_complete: Option<OnCompleteFn>,
    on_error: Option<OnErrorFn>,
    error_data: Option<ErrorDataFn>,
    input_builder: Option<InputBuilderFn>,
}

impl<S: LoopSessionStore + 'static> TurnRunner<S> {
    /// Create a runner. `cfg` defaults when `None`.
    ///
    /// `gate` is shared (`Arc`) so callers can [`QueryGate::acquire`] before
    /// spawning work and then call [`Self::execute_reserved`] (Go appkit parity).
    pub fn new(
        pool: Arc<ConnectionPool<S>>,
        gate: Arc<QueryGate>,
        classifier: EventClassifier,
        store: Arc<S>,
        cfg: Option<TurnConfig>,
    ) -> Self {
        Self {
            pool,
            gate,
            classifier,
            store,
            broadcaster: None,
            cfg: cfg.unwrap_or_default(),
            on_complete: None,
            on_error: None,
            error_data: None,
            input_builder: None,
        }
    }

    /// Shared query gate (same instance used by [`Self::execute`] / [`Self::execute_reserved`]).
    pub fn gate(&self) -> &Arc<QueryGate> {
        &self.gate
    }

    /// Attach an SSE broadcaster for delta / thinking / complete / error fan-out.
    pub fn with_broadcaster(mut self, b: Arc<SseBroadcaster>) -> Self {
        self.broadcaster = Some(b);
        self
    }

    /// Override the loop_input payload builder.
    pub fn with_input_builder(mut self, f: InputBuilderFn) -> Self {
        self.input_builder = Some(f);
        self
    }

    /// Completion hook (`app_key`, `loop_id`, `content`, `completion_event`, `elapsed_ms`).
    pub fn with_on_complete(mut self, f: OnCompleteFn) -> Self {
        self.on_complete = Some(f);
        self
    }

    /// Error hook.
    pub fn with_on_error(mut self, f: OnErrorFn) -> Self {
        self.on_error = Some(f);
        self
    }

    /// Formatter for SSE `query_error` payloads.
    pub fn with_error_data(mut self, f: ErrorDataFn) -> Self {
        self.error_data = Some(f);
        self
    }

    /// Execute one turn; returns concatenated assistant text on success.
    pub async fn execute(
        &self,
        session_id: &str,
        message: &str,
        user_id: &str,
        workspace_id: &str,
        attachments: Option<Value>,
        opts: Option<InputOpts>,
    ) -> Result<String> {
        self.validate_opts(opts.as_ref())?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let cancel_fn: CancelFn = Arc::new(move || {
            cancel_flag.store(true, Ordering::SeqCst);
        });
        self.gate
            .acquire(session_id, cancel_fn, None)
            .map_err(|_| Error::msg("query busy"))?;
        let result = self
            .run_turn(
                session_id,
                message,
                user_id,
                workspace_id,
                attachments,
                opts,
                cancelled,
            )
            .await;
        self.gate.release(session_id);
        result
    }

    /// Run a turn when the caller already reserved the gate via [`QueryGate::acquire`].
    ///
    /// Releases the gate on all exit paths (including `validate_opts` failure), matching
    /// Go `ExecuteReserved`.
    pub async fn execute_reserved(
        &self,
        session_id: &str,
        message: &str,
        user_id: &str,
        workspace_id: &str,
        attachments: Option<Value>,
        opts: Option<InputOpts>,
    ) -> Result<String> {
        if !self.gate.is_active(session_id) {
            return Err(Error::msg(format!(
                "appkit: ExecuteReserved requires an active QueryGate reservation for {session_id}"
            )));
        }
        if let Err(e) = self.validate_opts(opts.as_ref()) {
            self.gate.release(session_id);
            return Err(e);
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let cancel_fn: CancelFn = Arc::new(move || {
            cancel_flag.store(true, Ordering::SeqCst);
        });
        self.gate.replace_cancel(session_id, cancel_fn);
        let result = self
            .run_turn(
                session_id,
                message,
                user_id,
                workspace_id,
                attachments,
                opts,
                cancelled,
            )
            .await;
        self.gate.release(session_id);
        result
    }

    fn validate_opts(&self, opts: Option<&InputOpts>) -> Result<()> {
        if let Some(hint) = opts.and_then(|o| o.intent_hint.as_deref()) {
            if let Some(msg) = validate_loop_input_intent_hint(hint) {
                return Err(Error::msg(msg));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_turn(
        &self,
        session_id: &str,
        message: &str,
        user_id: &str,
        workspace_id: &str,
        attachments: Option<Value>,
        opts: Option<InputOpts>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<String> {
        let conn = self.pool.acquire(session_id, workspace_id, user_id).await?;
        let loop_id = conn.get_loop_id().await;

        let loop_id_for_cancel = loop_id.clone();
        let client_for_cancel = conn.client.clone();
        let send_cancel: SendCancelFn = Arc::new(move || {
            let client = client_for_cancel.clone();
            let loop_id = loop_id_for_cancel.clone();
            Box::pin(async move { client.command_cancel(&loop_id).await.map(|_| ()) })
        });
        self.gate.set_send_cancel(session_id, send_cancel);

        let has_attachments = attachments
            .as_ref()
            .map(|v| v.as_array().map(|a| !a.is_empty()).unwrap_or(true))
            .unwrap_or(false);

        let mut atts = attachments;
        if self.cfg.compact_attachments_before_send {
            if let Some(Value::Array(arr)) = atts.take() {
                let maps: Vec<Map<String, Value>> = arr
                    .into_iter()
                    .filter_map(|v| v.as_object().cloned())
                    .collect();
                let compacted = compact_attachments(&maps, self.cfg.compact_image_opts.as_ref());
                atts = Some(Value::Array(
                    compacted.into_iter().map(Value::Object).collect(),
                ));
            }
        }

        // Pre-send settle-drain of pooled leftovers (Go 0.4.8).
        {
            let mut rx_guard = conn.event_rx.lock().await;
            if let Some(rx) = rx_guard.as_mut() {
                drain_event_ch(rx, Duration::from_millis(5)).await;
            } else {
                let err = Error::msg(format!(
                    "missing event stream for session {session_id} (loop {loop_id})"
                ));
                self.fail_turn(session_id, &loop_id, &err).await;
                return Err(err);
            }
        }

        let input_opts = SendInputOptions {
            loop_id: Some(loop_id.clone()),
            intent_hint: opts.as_ref().and_then(|o| o.intent_hint.clone()),
            preferred_subagent: opts.as_ref().and_then(|o| o.preferred_subagent.clone()),
            response_schema: opts.as_ref().and_then(|o| o.response_schema.clone()),
            response_schema_name: opts.as_ref().and_then(|o| o.response_schema_name.clone()),
            response_schema_strict: opts.as_ref().and_then(|o| o.response_schema_strict),
            attachments: atts.clone(),
            ..Default::default()
        };

        if let Some(builder) = &self.input_builder {
            let flat = builder(message, &loop_id, atts.as_ref(), opts.as_ref());
            let mut params = Map::new();
            for (k, v) in flat {
                if k != "type" && k != "proto" && k != "method" {
                    // Custom builders may return either flat params or a full envelope.
                    if k == "params" {
                        if let Value::Object(inner) = v {
                            params.extend(inner);
                            continue;
                        }
                    }
                    params.insert(k, v);
                }
            }
            if let Err(e) = conn.client.notify("loop_input", params).await {
                let err = Error::msg(format!("send message: {e}"));
                self.fail_turn(session_id, &loop_id, &err).await;
                return Err(err);
            }
        } else if let Err(e) = conn.client.send_input(message, input_opts).await {
            self.fail_turn(session_id, &loop_id, &e).await;
            return Err(e);
        }

        self.store
            .append_message(session_id, json!({"role":"user","content": message}))
            .await;

        let started_at = Instant::now();
        let deadline = started_at + self.cfg.query_timeout;
        let idle_for_turn = idle_timeout_for_turn(&self.cfg, has_attachments);
        let mut last_event = Instant::now();
        let mut collected = String::new();
        let mut boundary = TurnBoundary::default();
        let mut armed = false;

        let result = loop {
            if cancelled.load(Ordering::SeqCst) {
                let err = Error::msg("query cancelled");
                self.fail_turn(session_id, &loop_id, &err).await;
                break Err(err);
            }
            if Instant::now() > deadline {
                let _ = conn.client.command_cancel(&loop_id).await;
                break self
                    .finish_timeout(
                        session_id,
                        &loop_id,
                        &collected,
                        started_at,
                        "query_timeout",
                        self.cfg.on_query_timeout,
                    )
                    .await;
            }
            // Idle silence is only meaningful after the turn is armed (first
            // non-stale event). Counting from query send treats LLM first-token
            // latency as "idle" and SoftCompletes empty replies under load.
            if armed && !idle_for_turn.is_zero() && last_event.elapsed() > idle_for_turn {
                let _ = conn.client.command_cancel(&loop_id).await;
                break self
                    .finish_timeout(
                        session_id,
                        &loop_id,
                        &collected,
                        started_at,
                        "idle_timeout",
                        self.cfg.on_idle_timeout,
                    )
                    .await;
            }

            let wait = if !armed || idle_for_turn.is_zero() {
                Duration::from_millis(500)
            } else {
                idle_for_turn
                    .saturating_sub(last_event.elapsed())
                    .min(Duration::from_millis(500))
                    .max(Duration::from_millis(1))
            };

            let ev = {
                let mut rx_guard = conn.event_rx.lock().await;
                let Some(rx) = rx_guard.as_mut() else {
                    break Err(Error::msg("event stream missing"));
                };
                match tokio::time::timeout(wait, rx.recv()).await {
                    Ok(Some(v)) => Some(v),
                    Ok(None) => None,
                    Err(_) => continue,
                }
            };

            let Some(ev) = ev else {
                if !conn.event_stream_live().await || !conn.client.is_connection_alive() {
                    if self.cfg.on_stream_close == TimeoutPolicy::SoftComplete
                        && !collected.trim().is_empty()
                    {
                        break self
                            .complete_turn(
                                session_id,
                                &loop_id,
                                &collected,
                                started_at,
                                "stream_closed",
                            )
                            .await;
                    }
                    let err = Error::msg("event stream closed");
                    self.fail_turn(session_id, &loop_id, &err).await;
                    break Err(err);
                }
                continue;
            };

            if !armed {
                if is_stale_turn_end_event(&ev) {
                    // Keepalive / prior-turn idle — do not arm or start idle clock.
                    continue;
                }
                if is_status_running_event(&ev) {
                    let _ = feed_boundary(&mut boundary, &ev);
                    // Daemon accepted the turn — arm idle from here so pre-accept
                    // wait does not burn the silence budget, but post-accept hangs
                    // still SoftComplete / Fail via idle_timeout.
                    armed = true;
                    last_event = Instant::now();
                    continue;
                }
                armed = true;
                last_event = Instant::now();
            }

            let ended = feed_boundary(&mut boundary, &ev);
            if ended.is_some() && !armed {
                boundary = TurnBoundary::default();
                continue;
            }

            let event_result = self.classifier.classify(&ev, &collected);
            // Heartbeats / empty catalog events must not postpone idle_timeout —
            // otherwise StrangeLoop planner churn holds the single-flight gate forever.
            if advances_idle_clock(&event_result) {
                last_event = Instant::now();
            }

            if event_result.terminal == ChatEventTerminal::FailedComplete {
                let err = Error::msg(
                    event_result
                        .error
                        .unwrap_or_else(|| "process event failed".into()),
                );
                self.fail_turn(session_id, &loop_id, &err).await;
                break Err(err);
            }

            if !event_result.thinking_step.trim().is_empty() {
                self.broadcast_thinking_step(session_id, &event_result.thinking_step);
            }

            if !event_result.content.is_empty() {
                let delta = if event_result.content.starts_with(&collected) {
                    let d = event_result.content[collected.len()..].to_string();
                    collected = event_result.content.clone();
                    d
                } else {
                    collected.push_str(&event_result.content);
                    event_result.content.clone()
                };
                if !delta.is_empty() {
                    self.broadcast_delta(session_id, &delta);
                }
            }

            if let Some(final_text) = self
                .classifier
                .resolve_deliverable_final_content(&event_result, &collected)
            {
                if !is_daemon_turn_end_event(&event_result.completion_event) {
                    collected = final_text;
                    let completion = event_result.completion_event;
                    break self
                        .complete_turn(session_id, &loop_id, &collected, started_at, &completion)
                        .await;
                }
            }

            if let Some(reason) = ended {
                if collected.trim().is_empty() {
                    let err =
                        Error::msg(format!("turn ended ({reason}) with no assistant content"));
                    self.fail_turn(session_id, &loop_id, &err).await;
                    break Err(err);
                }
                break self
                    .complete_turn(session_id, &loop_id, &collected, started_at, reason)
                    .await;
            }
        };

        // Post-turn non-blocking drain.
        {
            let mut rx_guard = conn.event_rx.lock().await;
            if let Some(rx) = rx_guard.as_mut() {
                drain_event_ch(rx, Duration::ZERO).await;
            }
        }

        // Keep pooled connection for session reuse (Go Release is explicit).
        let _ = conn;
        result
    }

    async fn finish_timeout(
        &self,
        session_id: &str,
        loop_id: &str,
        content: &str,
        started_at: Instant,
        completion_event: &str,
        policy: TimeoutPolicy,
    ) -> Result<String> {
        match policy {
            // Soft-complete even with empty content so idle/query timeouts always
            // release the QueryGate (callers map completion_event → chat.done codes).
            TimeoutPolicy::SoftComplete => {
                self.complete_turn(session_id, loop_id, content, started_at, completion_event)
                    .await
            }
            _ => {
                let err = Error::msg(completion_event);
                self.fail_turn(session_id, loop_id, &err).await;
                Err(err)
            }
        }
    }

    async fn complete_turn(
        &self,
        session_id: &str,
        loop_id: &str,
        content: &str,
        started_at: Instant,
        completion_event: &str,
    ) -> Result<String> {
        let elapsed_ms = started_at.elapsed().as_millis() as i64;
        self.store
            .append_message(
                session_id,
                json!({
                    "role": "assistant",
                    "content": content,
                    "status": "completed",
                    "completion_event": completion_event,
                    "deliverable": true,
                    "duration_ms": elapsed_ms,
                }),
            )
            .await;
        self.broadcast_complete(session_id, content);
        if let Some(hook) = &self.on_complete {
            hook(session_id, loop_id, content, completion_event, elapsed_ms);
        }
        Ok(content.to_string())
    }

    async fn fail_turn(&self, session_id: &str, loop_id: &str, err: &Error) {
        self.store
            .append_message(
                session_id,
                json!({
                    "role": "error",
                    "status": "failed",
                    "error_message": err.to_string(),
                }),
            )
            .await;
        self.broadcast_error(session_id, err);
        if let Some(hook) = &self.on_error {
            hook(session_id, loop_id, err);
        }
    }

    fn broadcast_delta(&self, app_key: &str, delta: &str) {
        if let Some(b) = &self.broadcaster {
            b.broadcast(
                app_key,
                SseEvent {
                    event_type: "delta".into(),
                    data: json!(delta),
                },
            );
        }
    }

    fn broadcast_thinking_step(&self, app_key: &str, step: &str) {
        if let Some(b) = &self.broadcaster {
            b.broadcast(
                app_key,
                SseEvent {
                    event_type: "thinking_step".into(),
                    data: json!(format!("{step}\n")),
                },
            );
        }
    }

    fn broadcast_complete(&self, app_key: &str, content: &str) {
        if let Some(b) = &self.broadcaster {
            b.broadcast(
                app_key,
                SseEvent {
                    event_type: "complete".into(),
                    data: json!(content),
                },
            );
        }
    }

    fn broadcast_error(&self, app_key: &str, err: &Error) {
        if let Some(b) = &self.broadcaster {
            let data = if let Some(fmt) = &self.error_data {
                fmt(err)
            } else {
                json!(err.to_string())
            };
            b.broadcast(
                app_key,
                SseEvent {
                    event_type: "query_error".into(),
                    data,
                },
            );
        }
    }
}

fn idle_timeout_for_turn(cfg: &TurnConfig, has_attachments: bool) -> Duration {
    let idle = cfg.idle_timeout;
    if idle.is_zero() {
        return Duration::ZERO;
    }
    if has_attachments
        && !cfg.min_idle_timeout_with_attachments.is_zero()
        && idle < cfg.min_idle_timeout_with_attachments
    {
        return cfg.min_idle_timeout_with_attachments;
    }
    idle
}

/// Events that should postpone idle_timeout (assistant progress / turn end).
fn advances_idle_clock(result: &super::classifier::ChatEventResult) -> bool {
    use super::classifier::ChatEventTerminal;
    if !result.content.trim().is_empty() {
        return true;
    }
    if !result.thinking_step.trim().is_empty() {
        return true;
    }
    matches!(
        result.terminal,
        ChatEventTerminal::DeliverableComplete | ChatEventTerminal::FailedComplete
    )
}

/// Drain pooled event channel. When `settle > 0`, keep reading until quiet for `settle`.
async fn drain_event_ch(rx: &mut mpsc::Receiver<Value>, settle: Duration) {
    if settle.is_zero() {
        while rx.try_recv().is_ok() {}
        return;
    }
    let mut deadline = Instant::now() + settle;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(_)) => {
                deadline = Instant::now() + settle;
            }
            Ok(None) | Err(_) => break,
        }
    }
}

fn feed_boundary(boundary: &mut TurnBoundary, msg: &Value) -> Option<&'static str> {
    let frame = if msg.get("type").and_then(|v| v.as_str()) == Some("next") {
        unwrap_next_frame(msg)
    } else {
        msg.clone()
    };
    let event_type = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if event_type == "status" {
        let state = frame.get("state").and_then(|v| v.as_str()).unwrap_or("");
        return boundary.feed_status(state);
    }
    if event_type == "event" {
        let mode = frame.get("mode").and_then(|v| v.as_str()).unwrap_or("");
        let data = frame.get("data").cloned().unwrap_or(Value::Null);
        return boundary.feed_event(mode, &data);
    }
    None
}

fn is_status_running_event(msg: &Value) -> bool {
    let frame = if msg.get("type").and_then(|v| v.as_str()) == Some("next") {
        unwrap_next_frame(msg)
    } else {
        msg.clone()
    };
    frame.get("type").and_then(|v| v.as_str()) == Some("status")
        && frame
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .eq_ignore_ascii_case("running")
}

fn is_stale_turn_end_event(msg: &Value) -> bool {
    let frame = if msg.get("type").and_then(|v| v.as_str()) == Some("next") {
        unwrap_next_frame(msg)
    } else {
        msg.clone()
    };
    let event_type = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if event_type == "status" {
        let state = frame
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        return matches!(state.as_str(), "idle" | "stopped");
    }
    if event_type == "event" {
        let mode = frame.get("mode").and_then(|v| v.as_str()).unwrap_or("");
        let data = frame.get("data").unwrap_or(&Value::Null);
        return mode == "custom" && is_turn_end_custom_data(data);
    }
    false
}
