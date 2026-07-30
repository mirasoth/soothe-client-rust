//! In-memory apply helpers for daemon `soothe.card.*` frames.

use serde_json::{Map, Value};

use crate::events::{
    EVENT_CARD_CREATED, EVENT_CARD_FINALIZED, EVENT_CARD_REPLAY_BEGIN, EVENT_CARD_REPLAY_END,
    EVENT_CARD_UPDATED,
};
use crate::protocol::as_str;

/// Parsed custom-mode card frame.
#[derive(Debug, Clone)]
pub struct ParsedCardFrame {
    /// Wire type (`soothe.card.*`).
    pub wire_type: String,
    /// Full card on create, otherwise `None`.
    pub card: Option<Map<String, Value>>,
    /// Patch for update/finalize.
    pub patch: Map<String, Value>,
}

fn is_card_mutation_type(t: &str) -> bool {
    matches!(
        t,
        EVENT_CARD_CREATED
            | EVENT_CARD_UPDATED
            | EVENT_CARD_FINALIZED
            | EVENT_CARD_REPLAY_BEGIN
            | EVENT_CARD_REPLAY_END
    )
}

/// Parse a custom-mode card frame, or `None` if not a card frame.
pub fn parse_card_custom_payload(data: &Value) -> Option<ParsedCardFrame> {
    let obj = data.as_object()?;
    let wire_type = as_str(obj.get("type").unwrap_or(&Value::Null))
        .trim()
        .to_string();
    if !is_card_mutation_type(&wire_type) {
        return None;
    }
    if wire_type == EVENT_CARD_REPLAY_BEGIN || wire_type == EVENT_CARD_REPLAY_END {
        return Some(ParsedCardFrame {
            wire_type,
            card: None,
            patch: Map::new(),
        });
    }
    let mut payload = match obj.get("data").and_then(|v| v.as_object()) {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    let mut card_id = as_str(obj.get("card_id").unwrap_or(&Value::Null))
        .trim()
        .to_string();
    if card_id.is_empty() {
        card_id = as_str(payload.get("id").unwrap_or(&Value::Null))
            .trim()
            .to_string();
    }
    if wire_type == EVENT_CARD_CREATED {
        if payload.get("type").is_none() || payload.get("content").is_none() {
            return None;
        }
        if as_str(payload.get("id").unwrap_or(&Value::Null))
            .trim()
            .is_empty()
            && !card_id.is_empty()
        {
            payload.insert("id".into(), Value::String(card_id));
        }
        return Some(ParsedCardFrame {
            wire_type,
            card: Some(payload),
            patch: Map::new(),
        });
    }
    if as_str(payload.get("id").unwrap_or(&Value::Null))
        .trim()
        .is_empty()
        && !card_id.is_empty()
    {
        payload.insert("id".into(), Value::String(card_id));
    }
    Some(ParsedCardFrame {
        wire_type,
        card: None,
        patch: payload,
    })
}

/// In-memory `card_id` → wire dict map driven by `soothe.card.*` frames.
#[derive(Debug, Default)]
pub struct CardProjection {
    cards: Map<String, Value>,
    order: Vec<String>,
    replaying: bool,
}

impl CardProjection {
    /// Construct an empty projection.
    pub fn new() -> Self {
        Self::default()
    }

    /// True while between `replay.begin` and `replay.end`.
    pub fn replaying(&self) -> bool {
        self.replaying
    }

    /// Cards in insertion order.
    pub fn snapshot(&self) -> Vec<Map<String, Value>> {
        self.order
            .iter()
            .filter_map(|id| self.cards.get(id).and_then(|v| v.as_object()).cloned())
            .collect()
    }

    /// One card by id.
    pub fn get(&self, card_id: &str) -> Option<&Map<String, Value>> {
        self.cards.get(card_id).and_then(|v| v.as_object())
    }

    /// Apply one custom-mode card payload. Returns true when handled.
    pub fn apply(&mut self, data: &Value) -> bool {
        let Some(parsed) = parse_card_custom_payload(data) else {
            return false;
        };
        match parsed.wire_type.as_str() {
            EVENT_CARD_REPLAY_BEGIN => {
                self.replaying = true;
                self.cards.clear();
                self.order.clear();
                true
            }
            EVENT_CARD_REPLAY_END => {
                self.replaying = false;
                true
            }
            EVENT_CARD_CREATED => {
                let Some(card) = parsed.card else {
                    return true;
                };
                let id = as_str(card.get("id").unwrap_or(&Value::Null))
                    .trim()
                    .to_string();
                if id.is_empty() {
                    return true;
                }
                if !self.cards.contains_key(&id) {
                    self.order.push(id.clone());
                }
                self.cards.insert(id, Value::Object(card));
                true
            }
            EVENT_CARD_UPDATED | EVENT_CARD_FINALIZED => {
                let id = as_str(parsed.patch.get("id").unwrap_or(&Value::Null))
                    .trim()
                    .to_string();
                if id.is_empty() {
                    return true;
                }
                let Some(existing) = self.cards.get(&id).and_then(|v| v.as_object()).cloned()
                else {
                    return true;
                };
                let mut next = existing;
                for (k, v) in parsed.patch {
                    if k == "id" || k == "type" {
                        continue;
                    }
                    next.insert(k, v);
                }
                self.cards.insert(id, Value::Object(next));
                true
            }
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_legacy_bare_card_types() {
        assert!(parse_card_custom_payload(&json!({
            "type": "card.created",
            "data": {"id": "x", "type": "user", "content": "hi"}
        }))
        .is_none());
    }

    #[test]
    fn applies_soothe_card_frames() {
        let mut proj = CardProjection::new();
        assert!(proj.apply(&json!({
            "type": EVENT_CARD_CREATED,
            "card_id": "a1",
            "data": {"id": "a1", "type": "assistant", "content": "hel"}
        })));
        assert_eq!(
            as_str(proj.get("a1").unwrap().get("content").unwrap()),
            "hel"
        );
        assert!(proj.apply(&json!({
            "type": EVENT_CARD_UPDATED,
            "card_id": "a1",
            "data": {"content": "hello"}
        })));
        assert_eq!(
            as_str(proj.get("a1").unwrap().get("content").unwrap()),
            "hello"
        );
        assert!(proj.apply(&json!({
            "type": EVENT_CARD_FINALIZED,
            "card_id": "a1",
            "data": {}
        })));
    }

    #[test]
    fn peel_labels_match_live_wire_names() {
        use crate::stream_terminal::{is_turn_progress_chunk, stale_pending_frame_label};

        assert!(is_turn_progress_chunk(
            "custom",
            &json!({"type": EVENT_CARD_CREATED})
        ));
        assert!(is_turn_progress_chunk(
            "custom",
            &json!({"type": EVENT_CARD_UPDATED})
        ));
        assert_eq!(
            stale_pending_frame_label(&json!({
                "type": "next",
                "payload": {
                    "mode": EVENT_CARD_REPLAY_BEGIN,
                    "data": {"type": EVENT_CARD_REPLAY_BEGIN}
                }
            })),
            Some(EVENT_CARD_REPLAY_BEGIN.to_string())
        );
    }
}
