//! Inbound frame drop priorities for pending-event overflow handling.
//!
//! Lower priority values are kept longer under pressure; higher values are drop
//! candidates first. Matches Python and Go client behavior.

use serde_json::{Map, Value};

use crate::stream_terminal::{is_turn_end_custom_data, STREAM_END};

/// Never drop: terminals, status, errors.
pub const DROP_PRIORITY_CRITICAL: i32 = 0;
/// Prefer keep: cognition / tool batches.
pub const DROP_PRIORITY_HIGH: i32 = 1;
/// Default drop candidate: streaming chunks.
pub const DROP_PRIORITY_NORMAL: i32 = 2;

/// Pending-event cap (Python inbound queue parity).
pub const DEFAULT_INBOUND_MAX_SIZE: usize = 20_000;

/// Returns drop priority for an inbound frame.
///
/// `None` maps to [`DROP_PRIORITY_CRITICAL`] (same as a nil map in Go).
pub fn inbound_frame_drop_priority(event: Option<&Map<String, Value>>) -> i32 {
    let Some(event) = event else {
        return DROP_PRIORITY_CRITICAL;
    };

    let mut event_type = value_as_str(event.get("type"));

    if event_type == "event_batch" || event_type == "tool_call_updates_batch" {
        return DROP_PRIORITY_HIGH;
    }

    if event_type == "next" {
        if let Some(payload) = event.get("payload").and_then(|v| v.as_object()) {
            let inner_mode = value_as_str(payload.get("mode"));
            let inner_data = payload.get("data");
            if inner_mode == "messages" {
                if messages_wire_terminal(inner_data) {
                    return DROP_PRIORITY_CRITICAL;
                }
                if let Some(arr) = inner_data.and_then(|v| v.as_array()) {
                    if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                        if value_as_str(first.get("phase")) == "goal_completion" {
                            return DROP_PRIORITY_CRITICAL;
                        }
                    }
                }
            }
            if value_as_str(payload.get("type")) == "complete" {
                return DROP_PRIORITY_CRITICAL;
            }
            if let Some(inner) = inner_data.and_then(|v| v.as_object()) {
                return inbound_frame_drop_priority(Some(inner));
            }
            event_type = value_as_str(payload.get("type"));
        }
    }

    if event_type == "complete" || event_type == "error" || event_type == "connection_ack" {
        return DROP_PRIORITY_CRITICAL;
    }

    if event_type == "status" {
        let state = value_as_str(event.get("state"));
        if matches!(state.as_str(), "idle" | "running" | "stopped" | "detached") {
            return DROP_PRIORITY_CRITICAL;
        }
    }

    if event_type == "event" {
        let mode = value_as_str(event.get("mode"));
        let data = event.get("data");
        if mode == "custom" {
            if let Some(data) = data {
                if is_turn_end_custom_data(data) {
                    return DROP_PRIORITY_CRITICAL;
                }
                if let Some(m) = data.as_object() {
                    let custom_type = value_as_str(m.get("type"));
                    if has_prefix(&custom_type, "soothe.cognition.") {
                        return DROP_PRIORITY_HIGH;
                    }
                    if has_prefix(&custom_type, "soothe.error.") || custom_type == "stream_degraded"
                    {
                        return DROP_PRIORITY_CRITICAL;
                    }
                    if custom_type == "soothe.ux.stream_tool_wire.tool_call_updates_batch" {
                        return DROP_PRIORITY_HIGH;
                    }
                }
            }
        }
        if mode == "messages" {
            if messages_wire_terminal(data) {
                return DROP_PRIORITY_CRITICAL;
            }
            if let Some(arr) = data.and_then(|v| v.as_array()) {
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    if value_as_str(first.get("phase")) == "goal_completion" {
                        return DROP_PRIORITY_CRITICAL;
                    }
                }
            }
        }
    }

    DROP_PRIORITY_NORMAL
}

fn messages_wire_terminal(data: Option<&Value>) -> bool {
    let Some(arr) = data.and_then(|v| v.as_array()) else {
        return false;
    };
    let Some(body) = arr.first().and_then(|v| v.as_object()) else {
        return false;
    };
    let t = value_as_str(body.get("type"));
    t == STREAM_END || contains_str(&t, "stream.end")
}

fn value_as_str(v: Option<&Value>) -> String {
    v.and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn has_prefix(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.starts_with(prefix)
}

fn contains_str(s: &str, sub: &str) -> bool {
    sub.is_empty() || s.contains(sub)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nil_event_is_critical() {
        assert_eq!(inbound_frame_drop_priority(None), DROP_PRIORITY_CRITICAL);
    }

    #[test]
    fn status_idle_is_critical() {
        let event = json!({"type": "status", "state": "idle"})
            .as_object()
            .cloned()
            .unwrap();
        assert_eq!(
            inbound_frame_drop_priority(Some(&event)),
            DROP_PRIORITY_CRITICAL
        );
    }

    #[test]
    fn updates_event_is_normal() {
        let event = json!({"type": "event", "mode": "updates", "data": {}})
            .as_object()
            .cloned()
            .unwrap();
        assert_eq!(
            inbound_frame_drop_priority(Some(&event)),
            DROP_PRIORITY_NORMAL
        );
    }

    #[test]
    fn event_batch_is_high() {
        let event = json!({"type": "event_batch"}).as_object().cloned().unwrap();
        assert_eq!(
            inbound_frame_drop_priority(Some(&event)),
            DROP_PRIORITY_HIGH
        );
    }

    #[test]
    fn cognition_custom_is_high() {
        let event = json!({
            "type": "event",
            "mode": "custom",
            "data": {"type": "soothe.cognition.plan.created"}
        })
        .as_object()
        .cloned()
        .unwrap();
        assert_eq!(
            inbound_frame_drop_priority(Some(&event)),
            DROP_PRIORITY_HIGH
        );
    }
}
