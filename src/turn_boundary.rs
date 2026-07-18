//! Turn / stream boundary helpers (`turn_id` / `seq`).

use serde_json::Value;

/// Return wire `turn_id` for `loop_id` + admit generation.
pub fn format_turn_id(loop_id: &str, generation: i64) -> String {
    let lid = loop_id.trim();
    if lid.is_empty() || generation <= 0 {
        return String::new();
    }
    format!("{lid}:{generation}")
}

/// Extract generation int from `turn_id`, or `None` if malformed.
pub fn parse_turn_generation(turn_id: Option<&str>) -> Option<i64> {
    let raw = turn_id?.trim();
    if raw.is_empty() || !raw.contains(':') {
        return None;
    }
    let suffix = raw.rsplit(':').next()?;
    let gen: i64 = suffix.parse().ok()?;
    if gen > 0 {
        Some(gen)
    } else {
        None
    }
}

/// Return `turn_id` from a status/event frame or nested custom data.
pub fn frame_turn_id(frame: Option<&Value>) -> Option<String> {
    let frame = frame?;
    if let Some(tid) = frame.get("turn_id").and_then(|v| v.as_str()) {
        let t = tid.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(data) = frame.get("data").and_then(|v| v.as_object()) {
        if let Some(tid) = data.get("turn_id").and_then(|v| v.as_str()) {
            let t = tid.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

/// Return non-negative `seq` from a wire frame, or `None`.
pub fn frame_seq(frame: Option<&Value>) -> Option<u64> {
    let frame = frame?;
    let raw = frame.get("seq")?;
    if raw.is_boolean() {
        return None;
    }
    if let Some(n) = raw.as_u64() {
        return Some(n);
    }
    if let Some(n) = raw.as_i64() {
        if n >= 0 {
            return Some(n as u64);
        }
    }
    if let Some(n) = raw.as_f64() {
        if n >= 0.0 && n.fract() == 0.0 {
            return Some(n as u64);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_id_roundtrip() {
        assert_eq!(format_turn_id("loop-1", 3), "loop-1:3");
        assert_eq!(parse_turn_generation(Some("loop-1:3")), Some(3));
        assert_eq!(parse_turn_generation(Some("bad")), None);
    }

    #[test]
    fn frame_helpers() {
        let f = json!({"turn_id": "L:1", "seq": 4});
        assert_eq!(frame_turn_id(Some(&f)).as_deref(), Some("L:1"));
        assert_eq!(frame_seq(Some(&f)), Some(4));
    }
}
