//! Event → deliverable / streaming / terminal classification.

use std::collections::HashSet;
use std::fmt;

use serde_json::Value;

use crate::events::{
    EVENT_FINAL_REPORT, EVENT_STREAM_TOOL_CALL_UPDATE, EVENT_TOOL_CALL_UPDATES_BATCH,
};
use crate::intent_hints::DEFAULT_DELIVERABLE_PHASES;
use crate::protocol::{unwrap_next, EventMessage};
use crate::stream_terminal::{is_turn_end_custom_data, STREAM_END};

use super::thinking_step;
use super::turn_boundary::TurnLifecycleGate;

/// Daemon's replay completion signal (internal; not exported by the wire catalog).
const EVENT_LOOP_HISTORY_REPLAYED: &str = "soothe.lifecycle.loop.history.replayed";

/// How a processed event should end the query loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChatEventTerminal {
    /// Accumulate content; the query is still running.
    #[default]
    Continue,
    /// User-visible final reply; persist it.
    DeliverableComplete,
    /// The query failed; persist an error.
    FailedComplete,
}

/// Structured outcome of classifying one daemon event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChatEventResult {
    /// Assistant text chunk or final reply content.
    pub content: String,
    /// User-visible progress line (not a final reply).
    pub thinking_step: String,
    /// Terminal classification for this event.
    pub terminal: ChatEventTerminal,
    /// Wire event type when `terminal == DeliverableComplete`.
    pub completion_event: String,
    /// Error message when `terminal == FailedComplete`.
    pub error: Option<String>,
}

/// Product-specific classifier knobs.
#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    /// Loop-tagged message phases that may end a query with user-facing text.
    pub deliverable_phases: HashSet<String>,
    /// Minimum trimmed rune count for a reply to persist as final (default 8).
    pub min_deliverable_runes: usize,
    /// Optional override for thinking-step event allowlist.
    pub thinking_step_events: Option<HashSet<String>>,
    /// Treat `status=idle` plus substantive accumulated text as deliverable.
    pub treat_status_idle_as_complete: bool,
    /// Soft-complete on turn-scoped `soothe.stream.end` in standalone classify paths.
    pub treat_stream_end_as_complete: bool,
    /// Gate idle completion on [`TurnLifecycleGate::allow_idle_complete`].
    pub gate_turn_end_signals: bool,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            deliverable_phases: HashSet::new(),
            min_deliverable_runes: 8,
            thinking_step_events: None,
            treat_status_idle_as_complete: false,
            treat_stream_end_as_complete: false,
            gate_turn_end_signals: false,
        }
    }
}

/// Error constructing an [`EventClassifier`].
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("ClassifierConfig.deliverable_phases must not be empty")]
pub struct ClassifierConfigError;

/// Maps decoded daemon events into deliverable, streaming, or failed outcomes.
#[derive(Debug, Clone)]
pub struct EventClassifier {
    cfg: ClassifierConfig,
}

impl EventClassifier {
    /// Construct a classifier from config.
    ///
    /// Returns an error when `deliverable_phases` is empty (required product decision).
    pub fn new(mut cfg: ClassifierConfig) -> Result<Self, ClassifierConfigError> {
        if cfg.deliverable_phases.is_empty() {
            return Err(ClassifierConfigError);
        }
        if cfg.min_deliverable_runes == 0 {
            cfg.min_deliverable_runes = 8;
        }
        Ok(Self { cfg })
    }

    /// Classifier with default deliverable phases from [`DEFAULT_DELIVERABLE_PHASES`].
    pub fn with_defaults() -> Self {
        Self::new(ClassifierConfig {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            min_deliverable_runes: 8,
            ..ClassifierConfig::default()
        })
        .expect("default deliverable phases are non-empty")
    }

    /// Classify one decoded event. Prefer [`Self::classify_turn`] when stream-end or
    /// gated idle completion needs a per-turn [`TurnLifecycleGate`].
    pub fn classify(&self, msg: &Value, accumulated: &str) -> ChatEventResult {
        self.classify_turn(msg, accumulated, None)
    }

    /// Classify one event and optionally observe a per-turn lifecycle gate first.
    pub fn classify_turn(
        &self,
        msg: &Value,
        accumulated: &str,
        mut gate: Option<&mut TurnLifecycleGate>,
    ) -> ChatEventResult {
        if let Some(g) = gate.as_deref_mut() {
            g.observe(msg);
        }
        let gate_ref = gate.as_deref();
        self.process_chat_event(msg, accumulated, gate_ref)
    }

    /// Report whether a persisted `completion_event` is user-facing.
    pub fn is_deliverable_completion_event(&self, event_type: &str) -> bool {
        let event_type = event_type.trim();
        if event_type.is_empty() {
            return false;
        }
        match event_type {
            "status.idle" | "status.stopped" | STREAM_END | "idle_timeout" | "query_timeout"
            | "stream_closed" => true,
            _ if event_type == EVENT_FINAL_REPORT => true,
            _ => {
                if let Some(phase) = event_type.strip_prefix("soothe.protocol.message.") {
                    self.is_deliverable_loop_phase(phase)
                } else {
                    event_type.contains("soothe.output") && event_type.contains("responded")
                }
            }
        }
    }

    /// Report whether trimmed assistant text is long enough to persist as final.
    pub fn is_substantive_assistant_reply(&self, content: &str) -> bool {
        content.trim().chars().count() >= self.cfg.min_deliverable_runes
    }

    /// Pick the user-visible reply for a completed query.
    ///
    /// Falls back to accumulated streamed text when the deliverable event has empty
    /// content (common after StrangeLoop streamed the answer on non-deliverable phases).
    pub fn resolve_deliverable_final_content(
        &self,
        event_result: &ChatEventResult,
        accumulated: &str,
    ) -> Option<String> {
        if event_result.terminal != ChatEventTerminal::DeliverableComplete {
            return None;
        }
        if !self.is_deliverable_completion_event(&event_result.completion_event) {
            return None;
        }
        let trimmed = event_result.content.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
        let acc = accumulated.trim();
        if !acc.is_empty() && self.is_substantive_assistant_reply(acc) {
            return Some(acc.to_string());
        }
        None
    }

    fn is_deliverable_loop_phase(&self, phase: &str) -> bool {
        self.cfg.deliverable_phases.contains(phase)
    }

    fn deliverable_result(
        &self,
        content: impl Into<String>,
        completion_event: impl Into<String>,
    ) -> ChatEventResult {
        ChatEventResult {
            content: content.into(),
            terminal: ChatEventTerminal::DeliverableComplete,
            completion_event: completion_event.into(),
            ..ChatEventResult::default()
        }
    }

    fn continue_result(&self, content: impl Into<String>) -> ChatEventResult {
        ChatEventResult {
            content: content.into(),
            terminal: ChatEventTerminal::Continue,
            ..ChatEventResult::default()
        }
    }

    fn failed_result(&self, err: impl fmt::Display) -> ChatEventResult {
        ChatEventResult {
            terminal: ChatEventTerminal::FailedComplete,
            error: Some(err.to_string()),
            ..ChatEventResult::default()
        }
    }

    fn process_chat_event(
        &self,
        msg: &Value,
        accumulated: &str,
        gate: Option<&TurnLifecycleGate>,
    ) -> ChatEventResult {
        let msg = &unwrap_next(msg);

        if is_status_message(msg) {
            return self.process_status(msg, accumulated, gate);
        }

        let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match msg_type {
            "error" => self.process_envelope_error(msg),
            "response" | "next" | "complete" | "receipt_response" => ChatEventResult::default(),
            "event" | "" if msg.get("mode").is_some() => {
                self.process_event_message(msg, accumulated, gate)
            }
            _ if msg.get("code").is_some() && msg.get("message").is_some() => {
                let code = msg
                    .get("code")
                    .and_then(|v| {
                        v.as_str()
                            .map(String::from)
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                    })
                    .unwrap_or_else(|| "unknown".to_string());
                let message = msg.get("message").and_then(|v| v.as_str()).unwrap_or("");
                self.failed_result(format!("daemon error [{code}]: {message}"))
            }
            _ => ChatEventResult::default(),
        }
    }

    fn process_status(
        &self,
        msg: &Value,
        accumulated: &str,
        gate: Option<&TurnLifecycleGate>,
    ) -> ChatEventResult {
        let state = msg.get("state").and_then(|v| v.as_str()).unwrap_or("");
        if self.cfg.treat_status_idle_as_complete
            && state.eq_ignore_ascii_case("idle")
            && self.is_substantive_assistant_reply(accumulated)
        {
            if self.cfg.gate_turn_end_signals
                && !gate
                    .map(TurnLifecycleGate::allow_idle_complete)
                    .unwrap_or(false)
            {
                return ChatEventResult::default();
            }
            return self.deliverable_result(accumulated.trim(), "status.idle");
        }
        ChatEventResult::default()
    }

    fn process_envelope_error(&self, msg: &Value) -> ChatEventResult {
        let mut code = -32603_i64;
        let mut message = String::new();
        if let Some(err) = msg.get("error").and_then(|v| v.as_object()) {
            if let Some(c) = err.get("code").and_then(|v| v.as_i64()) {
                code = c;
            }
            if let Some(m) = err.get("message").and_then(|v| v.as_str()) {
                message = m.to_string();
            }
        }
        self.failed_result(format!("daemon error [{code}]: {message}"))
    }

    fn process_event_message(
        &self,
        msg: &Value,
        accumulated: &str,
        gate: Option<&TurnLifecycleGate>,
    ) -> ChatEventResult {
        let mode = msg.get("mode").and_then(|v| v.as_str()).unwrap_or("");
        let data = msg.get("data").cloned().unwrap_or(Value::Null);
        let event_type = event_message_event_type(msg);

        if let Some(data_map) = normalize_event_data(&data) {
            let data_type = data_map
                .get("type")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(&event_type);

            if data_type == EVENT_LOOP_HISTORY_REPLAYED {
                return ChatEventResult::default();
            }
            if let Some(step) = thinking_step::extract_thinking_step(
                self.cfg.thinking_step_events.as_ref(),
                data_type,
                &data_map,
            ) {
                return thinking_step_result(step);
            }
        }

        if mode == "custom" && is_turn_end_custom_data(&data) {
            if self.cfg.treat_stream_end_as_complete
                && self.is_substantive_assistant_reply(accumulated)
                && gate
                    .map(TurnLifecycleGate::allow_stream_end)
                    .unwrap_or(false)
            {
                return self.deliverable_result(accumulated.trim(), STREAM_END);
            }
            return ChatEventResult::default();
        }

        if mode == "messages" {
            let (msg_type, raw_content, phase, has_payload) = first_message_payload(&data);
            if has_payload && !raw_content.is_empty() && is_streaming_message_type(&msg_type) {
                return self.continue_result(raw_content);
            }

            if let Ok(em) = serde_json::from_value::<EventMessage>(msg.clone()) {
                if let Some(loop_msg) = em.loop_ai_message() {
                    let content = loop_msg.loop_ai_text();
                    if !content.is_empty() {
                        let loop_type = loop_msg.r#type.as_deref().unwrap_or("");
                        if is_streaming_message_type(loop_type) {
                            return self.continue_result(content);
                        }
                        if let Some(phase) = loop_msg.phase.as_deref() {
                            if self.is_deliverable_loop_phase(phase)
                                && self.is_substantive_assistant_reply(&content)
                            {
                                return self.deliverable_result(
                                    content,
                                    format!("soothe.protocol.message.{phase}"),
                                );
                            }
                        }
                        return self.continue_result(content);
                    }
                }
            }

            if let Some(content) = messages_mode_assistant_content(msg) {
                return self.continue_result(content);
            }

            if has_payload && !raw_content.is_empty() {
                if (is_terminal_message_type(&msg_type) || msg_type.is_empty())
                    && self.is_deliverable_loop_phase(&phase)
                    && self.is_substantive_assistant_reply(&raw_content)
                {
                    return self.deliverable_result(
                        raw_content,
                        format!("soothe.protocol.message.{phase}"),
                    );
                }
                return self.continue_result(raw_content);
            }
        }

        let Some(data_map) = normalize_event_data(&data) else {
            return ChatEventResult::default();
        };

        let data_type = data_map.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let ns = event_type.as_str();
        let mut completion_event = data_type.to_string();
        if completion_event.is_empty() {
            completion_event.clone_from(&event_type);
        }

        if is_namespace_match(ns, data_type, "soothe.output")
            || is_namespace_match(ns, data_type, "responded")
        {
            if let Some(content) = extract_content_from_data(&data_map) {
                if self.is_final_output_event(data_type, ns) {
                    return self.deliverable_result(content, completion_event);
                }
                return self.continue_result(content);
            }
        }

        if is_namespace_match(ns, data_type, "agent_loop.completed")
            || is_namespace_match(ns, data_type, "agent_loop.reasoned")
            || is_namespace_match(ns, data_type, "loop.completed")
        {
            if let Some(content) = extract_content_from_data(&data_map) {
                return self.continue_result(content);
            }
        }

        if is_namespace_match(ns, data_type, "final_report") {
            if let Some(content) = extract_content_from_data(&data_map) {
                return self.deliverable_result(content, completion_event);
            }
        }

        if data_type.contains("soothe.error.") || ns.contains("soothe.error.") {
            let err_type = if data_type.is_empty() { ns } else { data_type };
            if let Some(message) = data_map.get("message").and_then(|v| v.as_str()) {
                if !message.is_empty() {
                    return self.failed_result(format!("{err_type}: {message}"));
                }
            }
            if let Some(content) = extract_content_from_data(&data_map) {
                return self.failed_result(format!("{err_type}: {content}"));
            }
            return self.failed_result(err_type);
        }

        if is_namespace_match(ns, data_type, "stream")
            || is_namespace_match(ns, data_type, "progress")
            || is_namespace_match(ns, data_type, EVENT_TOOL_CALL_UPDATES_BATCH)
            || is_namespace_match(ns, data_type, EVENT_STREAM_TOOL_CALL_UPDATE)
        {
            if let Some(delta) = data_map.get("delta").and_then(|v| v.as_str()) {
                return self.continue_result(delta);
            }
        }

        if is_namespace_match(ns, data_type, "heartbeat")
            || is_namespace_match(ns, data_type, "system.daemon")
            || is_namespace_match(ns, data_type, "agent_loop.started")
            || is_namespace_match(ns, data_type, "intent.classified")
        {
            return ChatEventResult::default();
        }

        ChatEventResult::default()
    }

    fn is_final_output_event(&self, data_type: &str, ns: &str) -> bool {
        let combined = format!("{data_type} {ns}");
        if combined.contains("final_report") {
            return true;
        }
        self.cfg
            .deliverable_phases
            .iter()
            .any(|phase| combined.contains(phase.as_str()))
    }
}

fn thinking_step_result(step: String) -> ChatEventResult {
    ChatEventResult {
        thinking_step: step.trim().to_string(),
        terminal: ChatEventTerminal::Continue,
        ..ChatEventResult::default()
    }
}

fn is_status_message(msg: &Value) -> bool {
    msg.get("type").and_then(|v| v.as_str()) == Some("status")
        || (msg.get("state").is_some() && msg.get("mode").is_none())
}

fn event_message_event_type(msg: &Value) -> String {
    if let Some(data) = msg.get("data").and_then(|d| d.as_object()) {
        if let Some(t) = data.get("type").and_then(|v| v.as_str()) {
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    match msg.get("namespace") {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => String::new(),
    }
}

fn is_streaming_message_type(msg_type: &str) -> bool {
    matches!(msg_type, "AIMessageChunk" | "ai_chunk" | "message_chunk")
}

fn is_terminal_message_type(msg_type: &str) -> bool {
    matches!(msg_type, "AIMessage" | "ai" | "assistant")
}

fn messages_mode_assistant_content(msg: &Value) -> Option<String> {
    if msg.get("mode").and_then(|v| v.as_str()) != Some("messages") {
        return None;
    }
    let data = msg.get("data")?;
    let items = data.as_array()?;
    let msg_map = items.first()?.as_object()?;
    let phase = msg_map
        .get("phase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if !phase.is_empty() {
        return None;
    }
    let msg_type = msg_map.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if !msg_type.is_empty() && !is_terminal_message_type(msg_type) {
        return None;
    }
    let content = extract_content_from_message(msg_map);
    let content = content.trim();
    if content.is_empty() {
        None
    } else {
        Some(content.to_string())
    }
}

fn first_message_payload(data: &Value) -> (String, String, String, bool) {
    let Some(items) = data.as_array() else {
        return (String::new(), String::new(), String::new(), false);
    };
    let Some(msg_map) = items.first().and_then(|v| v.as_object()) else {
        return (String::new(), String::new(), String::new(), false);
    };
    let msg_type = msg_map
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let phase = msg_map
        .get("phase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let content = extract_content_from_message(msg_map);
    (msg_type, content, phase, true)
}

fn extract_content_from_message(msg_map: &serde_json::Map<String, Value>) -> String {
    if let Some(c) = msg_map.get("content").and_then(|v| v.as_str()) {
        if !c.is_empty() {
            return c.to_string();
        }
    }
    if let Some(arr) = msg_map.get("content").and_then(|v| v.as_array()) {
        let mut buf = String::new();
        for item in arr {
            if let Some(s) = item.as_str() {
                buf.push_str(s);
                continue;
            }
            if let Some(blk) = item.as_object() {
                if let Some(t) = blk.get("text").and_then(|v| v.as_str()) {
                    buf.push_str(t);
                }
            }
        }
        if !buf.is_empty() {
            return buf;
        }
    }
    if let Some(blocks) = msg_map.get("content_blocks").and_then(|v| v.as_array()) {
        let mut buf = String::new();
        for blk in blocks {
            if let Some(m) = blk.as_object() {
                if let Some(t) = m.get("text").and_then(|v| v.as_str()) {
                    buf.push_str(t);
                }
            }
        }
        return buf;
    }
    String::new()
}

fn is_subscription_metadata_map(data: &serde_json::Map<String, Value>) -> bool {
    if !data.contains_key("loop_id") || !data.contains_key("latest_seq") {
        return false;
    }
    for key in [
        "content", "text", "response", "output", "message", "report", "answer",
    ] {
        if let Some(v) = data.get(key).and_then(|v| v.as_str()) {
            if !v.trim().is_empty() {
                return false;
            }
        }
    }
    true
}

fn extract_content_from_data(data: &serde_json::Map<String, Value>) -> Option<String> {
    if is_subscription_metadata_map(data) {
        return None;
    }
    for key in [
        "final_stdout_message",
        "completion_summary",
        "content",
        "text",
        "response",
        "output",
        "message",
        "report",
    ] {
        if let Some(val) = data.get(key).and_then(|v| v.as_str()) {
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    if let Some(nested) = data.get("data").and_then(|v| v.as_object()) {
        if is_subscription_metadata_map(nested) {
            return None;
        }
        for key in [
            "final_stdout_message",
            "completion_summary",
            "content",
            "text",
            "response",
            "output",
            "message",
            "report",
        ] {
            if let Some(val) = nested.get(key).and_then(|v| v.as_str()) {
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

fn is_namespace_match(ns: &str, data_type: &str, pattern: &str) -> bool {
    data_type.contains(pattern) || ns.contains(pattern)
}

fn normalize_event_data(data: &Value) -> Option<serde_json::Map<String, Value>> {
    match data {
        Value::Null => None,
        Value::Object(map) => Some(map.clone()),
        Value::String(s) => serde_json::from_str(s).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn default_classifier() -> EventClassifier {
        EventClassifier::with_defaults()
    }

    #[test]
    fn status_idle_after_content_opt_in() {
        let cl = EventClassifier::new(ClassifierConfig {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            treat_status_idle_as_complete: true,
            ..ClassifierConfig::default()
        })
        .unwrap();
        let msg = json!({"type": "status", "state": "idle", "loop_id": "L1"});
        let r = cl.classify(&msg, "Hello, this is enough text.");
        assert_eq!(r.terminal, ChatEventTerminal::DeliverableComplete);
        assert_eq!(r.completion_event, "status.idle");
    }

    #[test]
    fn status_idle_no_content_ignored() {
        let cl = EventClassifier::new(ClassifierConfig {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            treat_status_idle_as_complete: true,
            ..ClassifierConfig::default()
        })
        .unwrap();
        let msg = json!({"type": "status", "state": "idle"});
        let r = cl.classify(&msg, "");
        assert_eq!(r.terminal, ChatEventTerminal::Continue);
    }

    #[test]
    fn goal_completion_deliverable() {
        let cl = default_classifier();
        let msg = json!({
            "type": "event",
            "mode": "messages",
            "data": [{"type": "ai", "phase": "goal_completion", "content": "The answer is forty-two."}]
        });
        let r = cl.classify(&msg, "");
        assert_eq!(r.terminal, ChatEventTerminal::DeliverableComplete);
    }

    #[test]
    fn plan_direct_not_deliverable_by_default() {
        let cl = default_classifier();
        let msg = json!({
            "type": "event",
            "mode": "messages",
            "data": [{"type": "ai", "phase": "plan_direct", "content": "I will count the files next."}]
        });
        let r = cl.classify(&msg, "");
        assert_ne!(r.terminal, ChatEventTerminal::DeliverableComplete);
    }

    #[test]
    fn stream_end_opt_in_gated() {
        let cl = EventClassifier::new(ClassifierConfig {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            treat_stream_end_as_complete: true,
            ..ClassifierConfig::default()
        })
        .unwrap();
        let mut gate = TurnLifecycleGate::default();
        let accumulated = "Dali weather is sunny, twenty-eight degrees Celsius today.";
        let end = json!({
            "type": "event",
            "mode": "custom",
            "data": {"type": STREAM_END, "scope": "turn"}
        });

        let r = cl.classify_turn(&end, accumulated, Some(&mut gate));
        assert_eq!(r.terminal, ChatEventTerminal::Continue);

        gate.observe(&json!({"type": "status", "state": "running", "loop_id": "L1"}));
        let chunk = json!({
            "type": "event",
            "mode": "messages",
            "data": [{"type": "AIMessageChunk", "content": "Dali weather"}]
        });
        let _ = cl.classify_turn(&chunk, "", Some(&mut gate));
        assert!(gate.saw_turn_progress);

        let r = cl.classify_turn(&end, accumulated, Some(&mut gate));
        assert_eq!(r.terminal, ChatEventTerminal::DeliverableComplete);
        assert_eq!(r.completion_event, STREAM_END);
        assert!(cl.is_deliverable_completion_event(&r.completion_event));
    }

    #[test]
    fn resolve_deliverable_final_content_falls_back_to_accumulated() {
        let cl = EventClassifier::new(ClassifierConfig {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|p| (*p).to_string())
                .collect(),
            min_deliverable_runes: 1,
            ..ClassifierConfig::default()
        })
        .unwrap();
        let empty_deliverable = ChatEventResult {
            terminal: ChatEventTerminal::DeliverableComplete,
            completion_event: "soothe.protocol.message.goal_completion".to_string(),
            ..ChatEventResult::default()
        };
        let final_content = cl
            .resolve_deliverable_final_content(&empty_deliverable, "1, 2, 3, 4, 5")
            .unwrap();
        assert_eq!(final_content, "1, 2, 3, 4, 5");
    }

    #[test]
    fn skips_subscription_metadata_map() {
        let cl = default_classifier();
        let msg = json!({
            "type": "event",
            "namespace": ["soothe", "system"],
            "mode": "custom",
            "data": {"loop_id": "L1", "latest_seq": 3}
        });
        let r = cl.classify(&msg, "");
        assert!(r.content.is_empty());
        assert_eq!(r.terminal, ChatEventTerminal::Continue);
    }
}
