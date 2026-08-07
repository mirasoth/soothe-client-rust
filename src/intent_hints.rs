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
