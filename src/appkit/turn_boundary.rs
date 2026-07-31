//! DaemonSession turn-end contract for the pool TurnRunner path.

use serde_json::Value;

use crate::stream_terminal::{is_turn_end_custom_data, is_turn_progress_chunk, STREAM_END};
use crate::turn_boundary::{
    frame_turn_id, is_idle_terminal_allowed, is_turn_terminal_allowed, parse_turn_generation,
    turn_ids_match,
};

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
    /// Saw non-intake turn progress (`messages` / step customs).
    pub saw_turn_progress: bool,
    /// Bound turn_id from status=running.
    pub expected_turn_id: Option<String>,
    /// Cancellation notice seen.
    pub cancellation_seen: bool,
}

impl TurnLifecycleGate {
    /// Update gate flags from one decoded inbound message (status or event frame).
    pub fn observe(&mut self, msg: &Value) {
        if msg.get("type").and_then(|v| v.as_str()) == Some("status") {
            if let Some(state) = msg.get("state").and_then(|v| v.as_str()) {
                self.observe_status(state, frame_turn_id(Some(msg)));
            }
            return;
        }
        if msg.get("type").and_then(|v| v.as_str()) == Some("event") || msg.get("mode").is_some() {
            let mode = msg.get("mode").and_then(|v| v.as_str()).unwrap_or("");
            let data = msg.get("data").cloned().unwrap_or(Value::Null);
            self.observe_event(mode, &data);
        }
    }

    /// Observe a status frame.
    pub fn observe_status(&mut self, state: &str, turn_id: Option<String>) {
        if state.eq_ignore_ascii_case("running") {
            self.saw_running = true;
            if let Some(status_turn) = turn_id {
                let new_gen = parse_turn_generation(Some(&status_turn));
                let old_gen = parse_turn_generation(self.expected_turn_id.as_deref());
                if self.expected_turn_id.is_none()
                    || (new_gen.is_some()
                        && (old_gen.is_none() || new_gen.unwrap() >= old_gen.unwrap()))
                {
                    if self
                        .expected_turn_id
                        .as_ref()
                        .is_some_and(|e| e != &status_turn)
                    {
                        self.saw_turn_progress = false;
                    }
                    self.expected_turn_id = Some(status_turn);
                }
            }
        }
    }

    /// Observe an event frame.
    pub fn observe_event(&mut self, mode: &str, data: &Value) {
        if is_turn_progress_chunk(mode, data) {
            self.saw_turn_progress = true;
        }
    }

    /// Whether turn-scoped `stream.end` may end the turn.
    pub fn allow_stream_end(&self, frame_turn: Option<&str>) -> bool {
        is_turn_terminal_allowed(
            self.expected_turn_id.as_deref(),
            frame_turn,
            self.saw_running,
            self.saw_turn_progress,
        )
    }

    /// Whether `status=idle` may soft-complete the turn.
    pub fn allow_idle_complete(&self, frame_turn: Option<&str>) -> bool {
        is_idle_terminal_allowed(
            self.expected_turn_id.as_deref(),
            frame_turn,
            self.saw_running,
            self.saw_turn_progress,
            self.cancellation_seen,
        )
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
    /// Feed a status frame (turn_id omitted — prefer [`Self::feed_status_turn`]).
    pub fn feed_status(&mut self, state: &str) -> Option<&'static str> {
        self.feed_status_turn(state, None)
    }

    /// Feed a status frame with wire `turn_id`.
    pub fn feed_status_turn(
        &mut self,
        state: &str,
        turn_id: Option<&str>,
    ) -> Option<&'static str> {
        if self.ended {
            return static_reason(&self.reason);
        }
        self.gate
            .observe_status(state, turn_id.map(|s| s.to_string()));
        if state.eq_ignore_ascii_case("stopped") && self.gate.saw_running {
            if self.gate.expected_turn_id.is_some()
                && !turn_ids_match(self.gate.expected_turn_id.as_deref(), turn_id)
            {
                return None;
            }
            return Some(self.mark(TURN_END_STOPPED));
        }
        if state.eq_ignore_ascii_case("idle") && self.gate.allow_idle_complete(turn_id) {
            return Some(self.mark(TURN_END_IDLE));
        }
        None
    }

    /// Feed an event frame (outer turn_id omitted).
    pub fn feed_event(&mut self, mode: &str, data: &Value) -> Option<&'static str> {
        self.feed_event_turn(mode, data, None)
    }

    /// Feed an event frame with outer-frame `turn_id`.
    pub fn feed_event_turn(
        &mut self,
        mode: &str,
        data: &Value,
        frame_turn: Option<&str>,
    ) -> Option<&'static str> {
        if self.ended {
            return static_reason(&self.reason);
        }
        self.gate.observe_event(mode, data);
        let data_turn = frame_turn_id(Some(data));
        let tid = data_turn.as_deref().or(frame_turn);
        if mode == "custom" && is_turn_end_custom_data(data) && self.gate.allow_stream_end(tid) {
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
