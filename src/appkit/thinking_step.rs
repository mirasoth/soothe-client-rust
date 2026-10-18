//! Maps allowlisted progress events to one-line thinking steps.

use std::collections::HashSet;

use serde_json::Value;

use crate::events::{EVENT_GOAL_CREATED, EVENT_PLAN_CREATED, EVENT_TOOL_STARTED};

const MAX_THINKING_STEP_RUNES: usize = 280;

/// Default thinking-step event allowlist (overridable via [`ClassifierConfig`](super::ClassifierConfig)).
pub fn default_thinking_step_events() -> HashSet<String> {
    [
        EVENT_PLAN_CREATED,
        EVENT_GOAL_CREATED,
        EVENT_TOOL_STARTED,
        "soothe.cognition.strange_loop.step.started",
        "soothe.cognition.strange_loop.step.queued",
        "soothe.cognition.strange_loop.step.completed",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// Maps an allowlisted progress event to one structured UI line.
pub fn extract_thinking_step(
    allow: Option<&HashSet<String>>,
    event_type: &str,
    data: &serde_json::Map<String, Value>,
) -> Option<String> {
    let event_type = event_type.trim();
    if event_type.is_empty() {
        return None;
    }

    let default_allow = default_thinking_step_events();
    let allow = allow.unwrap_or(&default_allow);
    if !allow.contains(event_type) {
        return None;
    }

    let line = match event_type {
        EVENT_PLAN_CREATED => {
            let g = str_field(data, &["goal"]);
            if g.is_empty() {
                String::new()
            } else {
                format!("Goal: {g}")
            }
        }
        EVENT_GOAL_CREATED => {
            let g = str_field(data, &["friendly_message", "description"]);
            if g.is_empty() {
                String::new()
            } else {
                format!("Goal: {g}")
            }
        }
        EVENT_TOOL_STARTED => {
            let name = str_field(data, &["tool_name", "name"]);
            if name.is_empty() {
                String::new()
            } else {
                format!("Tool: {name}")
            }
        }
        "soothe.cognition.strange_loop.step.started" => format_step_line(data, ""),
        "soothe.cognition.strange_loop.step.queued" => format_step_line(data, "queued"),
        "soothe.cognition.strange_loop.step.completed" => format_step_line(data, "done"),
        _ => return None,
    };

    let line = line.trim().to_string();
    if line.is_empty() {
        return None;
    }
    truncate_thinking_step(line)
}

fn truncate_thinking_step(line: String) -> Option<String> {
    let runes: Vec<char> = line.chars().collect();
    if runes.len() <= MAX_THINKING_STEP_RUNES {
        Some(line)
    } else {
        let truncated: String = runes[..MAX_THINKING_STEP_RUNES].iter().collect();
        Some(format!("{truncated}…"))
    }
}

fn format_step_line(data: &serde_json::Map<String, Value>, suffix: &str) -> String {
    let step_id = str_field(data, &["step_id"]);
    let desc = str_field(data, &["description", "summary"]);
    let body = match (step_id.as_str(), desc.as_str()) {
        (s, d) if !s.is_empty() && !d.is_empty() => format!("Step {s}: {d}"),
        (s, _) if !s.is_empty() => format!("Step {s}"),
        (_, d) if !d.is_empty() => format!("Step: {d}"),
        _ => String::new(),
    };
    if body.is_empty() || suffix.is_empty() {
        body
    } else {
        format!("{body} ({suffix})")
    }
}

fn str_field(data: &serde_json::Map<String, Value>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(s) = data.get(*key).and_then(|v| v.as_str()) {
            let s = s.trim();
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}
