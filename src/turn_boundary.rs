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

/// True when both ids are non-empty and equal. Absent ids never match.
pub fn turn_ids_match(expected: Option<&str>, candidate: Option<&str>) -> bool {
    let exp = expected.map(str::trim).filter(|s| !s.is_empty());
    let cand = candidate.map(str::trim).filter(|s| !s.is_empty());
    match (exp, cand) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Gate for turn-scoped `stream.end` / `strange_loop.completed`.
pub fn is_turn_terminal_allowed(
    expected_turn_id: Option<&str>,
    frame_turn: Option<&str>,
    query_started: bool,
    turn_progress_seen: bool,
) -> bool {
    if !query_started || !turn_progress_seen {
        return false;
    }
    turn_ids_match(expected_turn_id, frame_turn)
}

/// Gate for `status=idle` soft-complete.
pub fn is_idle_terminal_allowed(
    expected_turn_id: Option<&str>,
    frame_turn: Option<&str>,
    query_started: bool,
    turn_progress_seen: bool,
    cancellation_seen: bool,
) -> bool {
    let exp = expected_turn_id.map(str::trim).filter(|s| !s.is_empty());
    if !query_started || exp.is_none() {
        return false;
    }
    let cand = frame_turn.map(str::trim).filter(|s| !s.is_empty());
    if let Some(c) = cand {
        if !turn_ids_match(exp, Some(c)) {
            return false;
        }
        return turn_progress_seen || cancellation_seen;
    }
    cancellation_seen
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

    #[test]
    fn absent_ids_never_match() {
        assert!(turn_ids_match(Some("L:1"), Some("L:1")));
        assert!(!turn_ids_match(Some("L:1"), None));
        assert!(!turn_ids_match(None, Some("L:1")));
        assert!(!is_turn_terminal_allowed(None, Some("L:1"), true, true));
        assert!(!is_turn_terminal_allowed(Some("L:1"), None, true, true));
        assert!(is_turn_terminal_allowed(Some("L:1"), Some("L:1"), true, true));
    }
}
