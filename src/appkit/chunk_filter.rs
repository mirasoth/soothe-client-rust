//! Early stream chunk filtering before the turn pipeline.

use serde_json::Value;

use crate::protocol::as_str;

/// Reports whether a chunk can be skipped before the turn pipeline.
pub fn should_drop_stream_chunk_early(namespace: &[Value], mode: &str, data: &Value) -> bool {
    let _ = namespace;
    if mode == "updates" {
        return updates_chunk_is_noop(data);
    }
    if mode == "messages" {
        return message_chunk_is_non_actionable(data);
    }
    false
}

fn updates_chunk_is_noop(data: &Value) -> bool {
    let Some(m) = data.as_object() else {
        return true;
    };
    !m.contains_key("__interrupt__")
}

fn message_chunk_is_non_actionable(data: &Value) -> bool {
    let Some(arr) = data.as_array() else {
        return false;
    };
    if arr.len() != 2 {
        return false;
    }
    if arr[0].is_null() {
        return true;
    }
    let Some(msg) = arr[0].as_object() else {
        return false;
    };
    let body = wire_body(msg);
    let mut raw = as_str(body.get("type").unwrap_or(&Value::Null)).to_string();
    if raw.is_empty() {
        raw = as_str(msg.get("type").unwrap_or(&Value::Null)).to_string();
    }
    if raw == "tool" || raw == "ToolMessage" || has_suffix(&raw, "ToolMessage") {
        return false;
    }
    if dict_has_tool_invocation(msg) {
        return false;
    }
    if body.get("phase").is_some() || msg.get("phase").is_some() {
        return false;
    }
    plain_text(msg).is_empty()
}

fn wire_body(msg: &serde_json::Map<String, Value>) -> &serde_json::Map<String, Value> {
    for key in ["kwargs", "data"] {
        if let Some(nested) = msg.get(key).and_then(|v| v.as_object()) {
            return nested;
        }
    }
    msg
}

fn dict_has_tool_invocation(msg: &serde_json::Map<String, Value>) -> bool {
    let body = wire_body(msg);
    if body.get("tool_calls").is_some() || body.get("tool_call_chunks").is_some() {
        return true;
    }
    for key in ["content", "content_blocks"] {
        let Some(raw) = body.get(key).and_then(|v| v.as_array()) else {
            continue;
        };
        for item in raw {
            let Some(m) = item.as_object() else {
                continue;
            };
            let t = as_str(m.get("type").unwrap_or(&Value::Null));
            if t == "tool_call" || t == "tool_call_chunk" || t == "tool_use" {
                return true;
            }
        }
    }
    false
}

fn plain_text(msg: &serde_json::Map<String, Value>) -> String {
    let body = wire_body(msg);
    let content = body
        .get("content")
        .or_else(|| msg.get("content"))
        .cloned()
        .unwrap_or(Value::Null);

    match content {
        Value::String(s) => trim_space(&s),
        Value::Array(blocks) => {
            let mut parts = String::new();
            for block in blocks {
                if let Some(s) = block.as_str() {
                    parts.push_str(s);
                    continue;
                }
                if let Some(m) = block.as_object() {
                    if let Some(text) = m.get("text").and_then(|v| v.as_str()) {
                        parts.push_str(text);
                    }
                }
            }
            trim_space(&parts)
        }
        _ => String::new(),
    }
}

fn has_suffix(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len() && s.ends_with(suffix)
}

fn trim_space(s: &str) -> String {
    s.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r'))
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_updates_should_drop() {
        assert!(should_drop_stream_chunk_early(&[], "updates", &json!({})));
    }

    #[test]
    fn custom_mode_should_not_drop() {
        assert!(!should_drop_stream_chunk_early(
            &[],
            "custom",
            &json!({"type": "x"})
        ));
    }

    #[test]
    fn updates_with_interrupt_should_not_drop() {
        assert!(!should_drop_stream_chunk_early(
            &[],
            "updates",
            &json!({"__interrupt__": true})
        ));
    }
}
