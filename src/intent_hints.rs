//! Loop input intent hints.

/// Fast text completion path.
pub const TEXT_COMPLETION: &str = "text_completion";
/// Image understanding → text.
pub const IMAGE_TO_TEXT: &str = "image_to_text";
/// OCR path.
pub const OCR: &str = "ocr";
/// Embedding path.
pub const EMBED: &str = "embed";

/// Default deliverable phases used by EventClassifier.
pub const DEFAULT_DELIVERABLE_PHASES: &[&str] = &[
    "quiz",
    "goal_completion",
    "chitchat",
    "text_completion",
    "image_to_text",
    "ocr",
    "embed",
];

const REMOVED: &[&str] = &["direct_llm", "quiz", "direct_model"];

/// Validate an intent hint; returns an error string when invalid.
pub fn validate_loop_input_intent_hint(hint: &str) -> Option<String> {
    let h = hint.trim();
    if h.is_empty() {
        return None;
    }
    if REMOVED.contains(&h) {
        return Some(format!(
            "intent_hint '{h}' is removed; use text_completion or another supported hint"
        ));
    }
    let ok = matches!(h, TEXT_COMPLETION | IMAGE_TO_TEXT | OCR | EMBED);
    if ok {
        None
    } else {
        Some(format!("unsupported intent_hint '{h}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_text_completion() {
        assert!(validate_loop_input_intent_hint(TEXT_COMPLETION).is_none());
    }

    #[test]
    fn rejects_removed() {
        assert!(validate_loop_input_intent_hint("direct_llm").is_some());
    }
}
