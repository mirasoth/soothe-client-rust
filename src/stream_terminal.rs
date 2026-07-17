//! Stream / turn terminal frame helpers.

use serde_json::Value;

use crate::protocol::as_str;

/// Daemon turn-scoped stream end custom type.
pub const STREAM_END: &str = "soothe.stream.end";

const STRANGE_LOOP_COMPLETED: &str = "soothe.cognition.strange_loop.completed";
const PLAN_CREATED: &str = "soothe.cognition.plan.created";
const STEP_STARTED: &str = "soothe.cognition.strange_loop.step.started";
const STEP_QUEUED: &str = "soothe.cognition.strange_loop.step.queued";
const STEP_COMPLETED: &str = "soothe.cognition.strange_loop.step.completed";
const CARD_REPLAY_BEGIN: &str = "soothe.card.replay.begin";
const CARD_REPLAY_END: &str = "soothe.card.replay.end";
const CARD_CREATED: &str = "soothe.card.created";

fn is_turn_end_type(t: &str) -> bool {
    t == STREAM_END || t == STRANGE_LOOP_COMPLETED
}

fn is_turn_progress_type(t: &str) -> bool {
    matches!(
        t,
        PLAN_CREATED | STEP_STARTED | STEP_QUEUED | STEP_COMPLETED
    ) || t.starts_with("soothe.cognition.strange_loop.step")
}

fn is_stale_pending_type(t: &str) -> bool {
    matches!(
        t,
        "connection_ack" | "complete" | CARD_REPLAY_BEGIN | CARD_REPLAY_END | CARD_CREATED
    )
}

/// True when `data` is a turn-scoped terminal custom payload.
pub fn is_turn_end_custom_data(data: &Value) -> bool {
    let Some(obj) = data.as_object() else {
        return false;
    };
    let custom_type = as_str(obj.get("type").unwrap_or(&Value::Null)).trim();
    if !is_turn_end_type(custom_type) {
        return false;
    }
    if custom_type == STREAM_END {
        let scope = as_str(obj.get("scope").unwrap_or(&Value::Null))
            .trim()
            .to_ascii_lowercase();
        return scope.is_empty() || scope == "turn";
    }
    true
}

/// True when a chunk proves non-intake turn progress.
pub fn is_turn_progress_chunk(mode: &str, data: &Value) -> bool {
    if mode == "messages" || mode == "updates" {
        return true;
    }
    if mode != "custom" {
        return false;
    }
    if is_turn_end_custom_data(data) {
        return false;
    }
    let Some(obj) = data.as_object() else {
        return false;
    };
    let custom_type = as_str(obj.get("type").unwrap_or(&Value::Null)).trim();
    is_turn_progress_type(custom_type)
}

/// Return a peel label when `event` is safe to drop at turn start.
pub fn stale_pending_frame_label(event: &Value) -> Option<String> {
    let obj = event.as_object()?;
    let event_type = as_str(obj.get("type").unwrap_or(&Value::Null));
    if is_stale_pending_type(event_type) {
        return Some(event_type.to_string());
    }
    if event_type == "next" {
        let payload = obj.get("payload")?.as_object()?;
        let stale_mode = as_str(payload.get("mode").unwrap_or(&Value::Null));
        if is_stale_pending_type(stale_mode) {
            return Some(stale_mode.to_string());
        }
        if let Some(inner) = payload.get("data") {
            return stale_pending_frame_label(inner);
        }
        return None;
    }
    if event_type == "event" {
        let mode = as_str(obj.get("mode").unwrap_or(&Value::Null));
        if mode == "custom" {
            if let Some(data) = obj.get("data") {
                if is_turn_end_custom_data(data) {
                    let t = as_str(
                        data.as_object()
                            .and_then(|o| o.get("type"))
                            .unwrap_or(&Value::Null),
                    )
                    .trim();
                    if !t.is_empty() {
                        return Some(t.to_string());
                    }
                }
            }
        }
    }
    None
}

/// True when the client should bump delivery_ack sequence for this frame.
pub fn inbound_needs_delivery_ack(event: &Value) -> bool {
    let Some(obj) = event.as_object() else {
        return false;
    };
    match as_str(obj.get("type").unwrap_or(&Value::Null)) {
        "complete" => true,
        "next" => {
            let Some(payload) = obj.get("payload").and_then(|p| p.as_object()) else {
                return false;
            };
            let Some(inner) = payload.get("data").and_then(|d| d.as_object()) else {
                return false;
            };
            if as_str(payload.get("mode").unwrap_or(&Value::Null)) == "event" {
                return inbound_needs_ack_from_event_shape(&Value::Object(inner.clone()));
            }
            false
        }
        "event" => inbound_needs_ack_from_event_shape(event),
        _ => false,
    }
}

fn inbound_needs_ack_from_event_shape(event: &Value) -> bool {
    let Some(obj) = event.as_object() else {
        return false;
    };
    let mode = as_str(obj.get("mode").unwrap_or(&Value::Null));
    let data = obj.get("data").unwrap_or(&Value::Null);
    if mode == "custom" && is_turn_end_custom_data(data) {
        return true;
    }
    if mode == "messages" {
        if let Some(arr) = data.as_array() {
            if let Some(body) = arr.first().and_then(|v| v.as_object()) {
                let t = as_str(body.get("type").unwrap_or(&Value::Null));
                return t == STREAM_END || t.contains("stream.end");
            }
        }
    }
    false
}

/// Extract `loop_id` from a frame or nested next payload.
pub fn extract_loop_id_from_inbound(event: &Value) -> String {
    let Some(obj) = event.as_object() else {
        return String::new();
    };
    let direct = as_str(obj.get("loop_id").unwrap_or(&Value::Null)).trim();
    if !direct.is_empty() {
        return direct.to_string();
    }
    if as_str(obj.get("type").unwrap_or(&Value::Null)) != "next" {
        return String::new();
    }
    let Some(payload) = obj.get("payload").and_then(|p| p.as_object()) else {
        return String::new();
    };
    let from_payload = as_str(payload.get("loop_id").unwrap_or(&Value::Null)).trim();
    if !from_payload.is_empty() {
        return from_payload.to_string();
    }
    payload
        .get("data")
        .and_then(|d| d.as_object())
        .map(|inner| {
            as_str(inner.get("loop_id").unwrap_or(&Value::Null))
                .trim()
                .to_string()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_end_stream_end_default_scope() {
        assert!(is_turn_end_custom_data(&json!({"type": STREAM_END})));
        assert!(!is_turn_end_custom_data(
            &json!({"type": STREAM_END, "scope": "goal"})
        ));
    }

    #[test]
    fn progress_messages() {
        assert!(is_turn_progress_chunk("messages", &json!({})));
        assert!(!is_turn_progress_chunk(
            "custom",
            &json!({"type": STREAM_END})
        ));
    }
}
