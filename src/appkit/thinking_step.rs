//! Maps allowlisted progress events to one-line thinking steps.

use std::collections::HashSet;

use serde_json::Value;

use crate::events::{
    EVENT_GOAL_CREATED, EVENT_PLAN_BATCH_STARTED, EVENT_PLAN_CREATED, EVENT_TOOL_STARTED,
};

const MAX_THINKING_STEP_RUNES: usize = 280;

/// Default thinking-step event allowlist (overridable via [`ClassifierConfig`](super::ClassifierConfig)).
pub fn default_thinking_step_events() -> HashSet<String> {
    [
        "soothe.cognition.plan.step.started",
        "soothe.cognition.plan.step.completed",
        "soothe.cognition.plan.step.failed",
        "soothe.lifecycle.iteration.started",
        "soothe.agent.loop.step.started",
        "soothe.agent.loop.started",
        EVENT_PLAN_BATCH_STARTED,
        EVENT_PLAN_CREATED,
        EVENT_GOAL_CREATED,
        EVENT_TOOL_STARTED,
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
        "soothe.cognition.plan.step.started" => format_plan_step_line(data, ""),
        "soothe.cognition.plan.step.completed" => format_plan_step_line(data, "done"),
        "soothe.cognition.plan.step.failed" => {
            let step_id = str_field(data, &["step_id"]);
            let err_msg = str_field(data, &["error"]);
            match (step_id.as_str(), err_msg.as_str()) {
                (s, e) if !s.is_empty() && !e.is_empty() => format!("Step {s} failed: {e}"),
                (s, _) if !s.is_empty() => format!("Step {s} failed"),
                (_, e) if !e.is_empty() => format!("Step failed: {e}"),
                _ => String::new(),
            }
        }
        "soothe.agent.loop.step.started" => format_agent_step_line(data, ""),
        EVENT_PLAN_BATCH_STARTED => data
            .get("parallel_count")
            .and_then(|v| v.as_f64())
            .filter(|n| *n > 0.0)
            .map(|n| format!("Running {} steps in parallel", n as i64))
            .unwrap_or_default(),
        EVENT_PLAN_CREATED | "soothe.agent.loop.started" => {
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
        "soothe.lifecycle.iteration.started" => {
            let g = str_field(data, &["goal_description"]);
            if g.is_empty() {
                String::new()
            } else {
                format!("Iteration: {g}")
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

fn format_plan_step_line(data: &serde_json::Map<String, Value>, suffix: &str) -> String {
    let step_id = str_field(data, &["step_id"]);
    let desc = str_field(data, &["description"]);
    match (step_id.as_str(), desc.as_str(), suffix) {
        (s, _, suf) if !s.is_empty() && !suf.is_empty() => format!("Step {s}: {suf}"),
        (s, d, _) if !s.is_empty() && !d.is_empty() => format!("Step {s}: {d}"),
        (s, _, _) if !s.is_empty() => format!("Step {s}"),
        (_, d, suf) if !d.is_empty() && !suf.is_empty() => format!("Step: {suf}"),
        (_, d, _) if !d.is_empty() => format!("Step: {d}"),
        (_, _, suf) if !suf.is_empty() => format!("Step: {suf}"),
        _ => String::new(),
    }
}

fn format_agent_step_line(data: &serde_json::Map<String, Value>, suffix: &str) -> String {
    let step_id = str_field(data, &["step_id"]);
    let desc = str_field(data, &["description"]);
    match (step_id.as_str(), desc.as_str(), suffix) {
        (s, d, _) if !s.is_empty() && !d.is_empty() => format!("Step {s}: {d}"),
        (_, d, suf) if !d.is_empty() && !suf.is_empty() => format!("Step: {suf}"),
        (_, d, _) if !d.is_empty() => format!("Step: {d}"),
        (s, _, _) if !s.is_empty() => format!("Step {s}"),
        _ => String::new(),
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
