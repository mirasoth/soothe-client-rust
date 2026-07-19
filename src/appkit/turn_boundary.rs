//! DaemonSession turn-end contract for the pool TurnRunner path.

use serde_json::Value;

use crate::stream_terminal::{is_turn_end_custom_data, is_turn_progress_chunk, STREAM_END};

/// Completion event for turn-scoped `soothe.stream.end`.
pub const TURN_END_STREAM_END: &str = STREAM_END;
/// Completion event for gated `status=idle`.
pub const TURN_END_IDLE: &str = "status.idle";
/// Completion event for `status=stopped` after running.
pub const TURN_END_STOPPED: &str = "status.stopped";

/// Per-turn progress flags (DaemonSession parity; not shared across chats).
#[derive(Debug, Default, Clone)]
pub struct TurnLifecycleGate {
    /// Saw `status=running` for this turn.
    pub saw_running: bool,
    /// Saw any event payload after filtering.
    pub saw_stream_payload: bool,
    /// Saw non-intake turn progress (`messages` / step customs).
    pub saw_turn_progress: bool,
}

impl TurnLifecycleGate {
    /// Observe a status frame.
    pub fn observe_status(&mut self, state: &str) {
        if state.eq_ignore_ascii_case("running") {
            self.saw_running = true;
        }
    }

    /// Observe an event frame.
    pub fn observe_event(&mut self, mode: &str, data: &Value) {
        self.saw_stream_payload = true;
        if is_turn_progress_chunk(mode, data) {
            self.saw_turn_progress = true;
        }
    }

    /// Whether turn-scoped `stream.end` may end the turn.
    pub fn allow_stream_end(&self) -> bool {
        self.saw_running && self.saw_turn_progress
    }

    /// Whether `status=idle` may soft-complete the turn.
    pub fn allow_idle_complete(&self) -> bool {
        self.saw_running && self.saw_stream_payload
    }
}

/// Applies DaemonSession end rules to pool frames.
#[derive(Debug, Default)]
pub struct TurnBoundary {
    /// Progress gate for this turn.
    pub gate: TurnLifecycleGate,
    /// True after a terminal boundary was observed.
    pub ended: bool,
    /// Wire completion reason when `ended`.
    pub reason: String,
}

impl TurnBoundary {
    /// Feed a status frame. Returns Some(reason) when the turn ends.
    pub fn feed_status(&mut self, state: &str) -> Option<&'static str> {
        if self.ended {
            return static_reason(&self.reason);
        }
        self.gate.observe_status(state);
        if state.eq_ignore_ascii_case("stopped") && self.gate.saw_running {
            return Some(self.mark(TURN_END_STOPPED));
        }
        if state.eq_ignore_ascii_case("idle") && self.gate.allow_idle_complete() {
            return Some(self.mark(TURN_END_IDLE));
        }
        None
    }

    /// Feed an event frame. Returns Some(reason) when the turn ends.
    pub fn feed_event(&mut self, mode: &str, data: &Value) -> Option<&'static str> {
        if self.ended {
            return static_reason(&self.reason);
        }
        self.gate.observe_event(mode, data);
        if mode == "custom" && is_turn_end_custom_data(data) && self.gate.allow_stream_end() {
            return Some(self.mark(TURN_END_STREAM_END));
        }
        None
    }

    fn mark(&mut self, reason: &'static str) -> &'static str {
        self.ended = true;
        self.reason = reason.to_string();
        reason
    }
}

fn static_reason(reason: &str) -> Option<&'static str> {
    match reason {
        TURN_END_STREAM_END => Some(TURN_END_STREAM_END),
        TURN_END_IDLE => Some(TURN_END_IDLE),
        TURN_END_STOPPED => Some(TURN_END_STOPPED),
        _ => None,
    }
}

/// True for TurnBoundary completion_event values (not phase deliverables).
pub fn is_daemon_turn_end_event(completion_event: &str) -> bool {
    matches!(
        completion_event.trim(),
        TURN_END_STREAM_END | TURN_END_IDLE | TURN_END_STOPPED
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ignores_pre_running_idle() {
        let mut b = TurnBoundary::default();
        assert!(b.feed_status("idle").is_none());
        b.feed_status("running");
        b.feed_event(
            "messages",
            &json!([{"type":"AIMessageChunk","content":"x"}]),
        );
        assert_eq!(b.feed_status("idle"), Some(TURN_END_IDLE));
    }

    #[test]
    fn stream_end_requires_running_and_progress() {
        let mut b = TurnBoundary::default();
        let end = json!({"type": STREAM_END, "scope": "turn"});
        assert!(b.feed_event("custom", &end).is_none());
        b.feed_status("running");
        assert!(b.feed_event("custom", &end).is_none());
        b.feed_event("messages", &json!({}));
        assert_eq!(b.feed_event("custom", &end), Some(TURN_END_STREAM_END));
    }

    #[test]
    fn stopped_after_running() {
        let mut b = TurnBoundary::default();
        assert!(b.feed_status("stopped").is_none());
        b.feed_status("running");
        assert_eq!(b.feed_status("stopped"), Some(TURN_END_STOPPED));
    }

    #[test]
    fn daemon_turn_end_event_helper() {
        assert!(is_daemon_turn_end_event(TURN_END_STREAM_END));
        assert!(!is_daemon_turn_end_event(
            "soothe.protocol.message.goal_completion"
        ));
    }
}
