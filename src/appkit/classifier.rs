//! Event → deliverable / streaming / terminal classification.

use serde_json::Value;

use crate::intent_hints::DEFAULT_DELIVERABLE_PHASES;
use crate::stream_terminal::{is_turn_end_custom_data, STREAM_END};

/// Classifies turn chunks for product backends.
#[derive(Debug, Clone)]
pub struct EventClassifier {
    /// Deliverable phase names.
    pub deliverable_phases: Vec<String>,
    /// Treat status idle as complete.
    pub treat_status_idle_as_complete: bool,
}

impl Default for EventClassifier {
    fn default() -> Self {
        Self {
            deliverable_phases: DEFAULT_DELIVERABLE_PHASES
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            treat_status_idle_as_complete: false,
        }
    }
}

impl EventClassifier {
    /// Create with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// True when custom data is a deliverable phase.
    pub fn is_deliverable(&self, mode: &str, data: &Value) -> bool {
        if mode != "custom" {
            return false;
        }
        let phase = data
            .get("phase")
            .or_else(|| data.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        self.deliverable_phases.iter().any(|p| p == phase)
    }

    /// True when chunk is a terminal custom.
    pub fn is_terminal(&self, mode: &str, data: &Value) -> bool {
        mode == "custom" && is_turn_end_custom_data(data)
    }

    /// Extract text from a messages-mode chunk when possible.
    pub fn extract_text(&self, mode: &str, data: &Value) -> Option<String> {
        if mode == "messages" {
            if let Some(arr) = data.as_array() {
                for item in arr {
                    if let Some(c) = item.get("content").and_then(|v| v.as_str()) {
                        if !c.is_empty() {
                            return Some(c.to_string());
                        }
                    }
                    if let Some(c) = item.get("text").and_then(|v| v.as_str()) {
                        if !c.is_empty() {
                            return Some(c.to_string());
                        }
                    }
                }
            }
            if let Some(c) = data.get("content").and_then(|v| v.as_str()) {
                return Some(c.to_string());
            }
        }
        if mode == "custom" {
            if data.get("type").and_then(|v| v.as_str()) == Some(STREAM_END) {
                return None;
            }
            if let Some(c) = data
                .get("content")
                .or_else(|| data.get("text"))
                .and_then(|v| v.as_str())
            {
                return Some(c.to_string());
            }
        }
        None
    }
}
