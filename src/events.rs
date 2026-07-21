//! Client-facing event namespace constants for the Soothe daemon wire protocol.
//!
//! Internal catalog types (`soothe.internal.*`) are server-only and are never
//! broadcast to WebSocket clients.

use crate::verbosity::VerbosityTier;

// ---------------------------------------------------------------------------
// Plan / goal
// ---------------------------------------------------------------------------

/// Plan created.
pub const EVENT_PLAN_CREATED: &str = "soothe.cognition.plan.created";
/// Plan batch started.
pub const EVENT_PLAN_BATCH_STARTED: &str = "soothe.cognition.plan.batch.started";
/// Plan reflected.
pub const EVENT_PLAN_REFLECTED: &str = "soothe.cognition.plan.reflected";

/// Goal created.
pub const EVENT_GOAL_CREATED: &str = "soothe.cognition.goal.created";
/// Goal completed.
pub const EVENT_GOAL_COMPLETED: &str = "soothe.cognition.goal.completed";
/// Goal failed.
pub const EVENT_GOAL_FAILED: &str = "soothe.cognition.goal.failed";
/// Goal deferred.
pub const EVENT_GOAL_DEFERRED: &str = "soothe.cognition.goal.deferred";
/// Goal batch started.
pub const EVENT_GOAL_BATCH_STARTED: &str = "soothe.cognition.goal.batch.started";
/// Goal reported.
pub const EVENT_GOAL_REPORTED: &str = "soothe.cognition.goal.reported";
/// Goal directives applied.
pub const EVENT_GOAL_DIRECTIVES_APPLIED: &str = "soothe.cognition.goal.directives.applied";

// ---------------------------------------------------------------------------
// Subagents
// ---------------------------------------------------------------------------

/// Deep research started.
pub const EVENT_DEEP_RESEARCH_STARTED: &str = "soothe.subagent.deep_research.started";
/// Deep research progress.
pub const EVENT_DEEP_RESEARCH_PROGRESS: &str = "soothe.subagent.deep_research.progress";
/// Deep research step completed.
pub const EVENT_DEEP_RESEARCH_STEP_COMPLETED: &str = "soothe.subagent.deep_research.step.completed";
/// Deep research gather summary.
pub const EVENT_DEEP_RESEARCH_GATHER_SUMMARY: &str = "soothe.subagent.deep_research.gather.summary";
/// Deep research crawl summary.
pub const EVENT_DEEP_RESEARCH_CRAWL_SUMMARY: &str = "soothe.subagent.deep_research.crawl.summary";
/// Deep research completed.
pub const EVENT_DEEP_RESEARCH_COMPLETED: &str = "soothe.subagent.deep_research.completed";

// ---------------------------------------------------------------------------
// Control / cards / tools
// ---------------------------------------------------------------------------

/// Replay complete control-plane frame.
pub const EVENT_REPLAY_COMPLETE: &str = "replay_complete";
/// Loop reattached control-plane frame.
pub const EVENT_LOOP_REATTACHED_WIRE: &str = "loop_reattached";

/// Card replay begin.
pub const EVENT_CARD_REPLAY_BEGIN: &str = "card.replay_begin";
/// Card created.
pub const EVENT_CARD_CREATED: &str = "card.created";
/// Card replay end.
pub const EVENT_CARD_REPLAY_END: &str = "card.replay_end";

/// Tool started.
pub const EVENT_TOOL_STARTED: &str = "soothe.tool.execution.started";
/// Tool completed.
pub const EVENT_TOOL_COMPLETED: &str = "soothe.tool.execution.completed";
/// Tool error.
pub const EVENT_TOOL_ERROR: &str = "soothe.tool.execution.error";

/// Stream tool call update.
pub const EVENT_STREAM_TOOL_CALL_UPDATE: &str = "soothe.stream.tool_call.update";
/// Tool call updates batch.
pub const EVENT_TOOL_CALL_UPDATES_BATCH: &str = "tool_call_updates_batch";

// ---------------------------------------------------------------------------
// StrangeLoop / branch / protocol / output / autopilot / error
// ---------------------------------------------------------------------------

/// Strange loop started.
pub const EVENT_STRANGE_LOOP_STARTED: &str = "soothe.cognition.strange_loop.started";
/// Strange loop completed.
pub const EVENT_STRANGE_LOOP_COMPLETED: &str = "soothe.cognition.strange_loop.completed";
/// Strange loop plan decision.
pub const EVENT_STRANGE_LOOP_PLAN_DECISION: &str = "soothe.cognition.strange_loop.plan.decision";
/// Strange loop reasoned.
pub const EVENT_STRANGE_LOOP_REASONED: &str = "soothe.cognition.strange_loop.reasoned";
/// Strange loop step started.
pub const EVENT_STRANGE_LOOP_STEP_STARTED: &str = "soothe.cognition.strange_loop.step.started";
/// Strange loop step queued.
pub const EVENT_STRANGE_LOOP_STEP_QUEUED: &str = "soothe.cognition.strange_loop.step.queued";
/// Strange loop step completed.
pub const EVENT_STRANGE_LOOP_STEP_COMPLETED: &str = "soothe.cognition.strange_loop.step.completed";
/// Strange loop context compacted.
pub const EVENT_STRANGE_LOOP_CONTEXT_COMPACTED: &str =
    "soothe.cognition.strange_loop.context.compacted";

/// Branch created.
pub const EVENT_BRANCH_CREATED: &str = "soothe.cognition.branch.created";
/// Branch retry started.
pub const EVENT_BRANCH_RETRY_STARTED: &str = "soothe.cognition.branch.retry.started";

/// Message received.
pub const EVENT_MESSAGE_RECEIVED: &str = "soothe.protocol.message.received";
/// Message sent.
pub const EVENT_MESSAGE_SENT: &str = "soothe.protocol.message.sent";

/// Final report.
pub const EVENT_FINAL_REPORT: &str = "soothe.output.autonomous.final_report.reported";

/// Autopilot status changed.
pub const EVENT_AUTOPILOT_STATUS_CHANGED: &str = "soothe.system.autopilot.status.changed";
/// Autopilot goal created.
pub const EVENT_AUTOPILOT_GOAL_CREATED: &str = "soothe.system.autopilot.goal.created";
/// Autopilot goal progress.
pub const EVENT_AUTOPILOT_GOAL_PROGRESS: &str = "soothe.system.autopilot.goal.reported";
/// Autopilot goal completed.
pub const EVENT_AUTOPILOT_GOAL_COMPLETED: &str = "soothe.system.autopilot.goal.completed";
/// Autopilot goal suspended.
pub const EVENT_AUTOPILOT_GOAL_SUSPENDED: &str = "soothe.system.autopilot.goal.suspended";
/// Autopilot goal blocked.
pub const EVENT_AUTOPILOT_GOAL_BLOCKED: &str = "soothe.system.autopilot.goal.blocked";
/// Autopilot dreaming entered.
pub const EVENT_AUTOPILOT_DREAMING_ENTERED: &str = "soothe.system.autopilot.dreaming.started";
/// Autopilot dreaming exited.
pub const EVENT_AUTOPILOT_DREAMING_EXITED: &str = "soothe.system.autopilot.dreaming.completed";

/// General failure.
pub const EVENT_GENERAL_FAILED: &str = "soothe.error.general.failed";

/// Parsed 4-segment namespace components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedNamespace {
    /// Domain (e.g. `cognition`).
    pub domain: String,
    /// Component (e.g. `plan`).
    pub component: String,
    /// Action (e.g. `created`).
    pub action: String,
}

/// Split a 4-segment event namespace into domain / component / action.
///
/// Returns `None` for non-`soothe.*` paths, short paths, or `soothe.internal.*`.
pub fn parse_namespace(ns: &str) -> Option<ParsedNamespace> {
    let parts: Vec<&str> = ns.split('.').collect();
    if parts.len() < 4 || parts[0] != "soothe" {
        return None;
    }
    if parts[1] == "internal" {
        return None;
    }
    Some(ParsedNamespace {
        domain: parts[1].to_string(),
        component: parts[2].to_string(),
        action: parts[3].to_string(),
    })
}

/// Classify an event type / namespace into a verbosity tier.
pub fn classify_event_verbosity(event_type_or_namespace: &str) -> VerbosityTier {
    if let Some(parsed) = parse_namespace(event_type_or_namespace) {
        return classify_by_domain(&parsed.domain, event_type_or_namespace);
    }
    classify_by_event_type_string(event_type_or_namespace)
}

fn classify_by_domain(domain: &str, full: &str) -> VerbosityTier {
    match domain {
        "cognition" => VerbosityTier::Normal,
        "protocol" => VerbosityTier::Detailed,
        "tool" => VerbosityTier::Internal,
        "subagent" => classify_subagent_event(full),
        "autopilot" | "system" => VerbosityTier::Normal,
        "output" | "error" => VerbosityTier::Quiet,
        _ => VerbosityTier::Normal,
    }
}

fn classify_subagent_event(full: &str) -> VerbosityTier {
    let Some(parsed) = parse_namespace(full) else {
        return VerbosityTier::Normal;
    };
    match parsed.action.as_str() {
        "started" | "completed" => VerbosityTier::Normal,
        _ => VerbosityTier::Detailed,
    }
}

fn classify_by_event_type_string(event_type: &str) -> VerbosityTier {
    if event_type == EVENT_FINAL_REPORT || event_type == EVENT_GENERAL_FAILED {
        return VerbosityTier::Quiet;
    }
    if event_type == EVENT_TOOL_STARTED {
        return VerbosityTier::Internal;
    }
    VerbosityTier::Normal
}

/// Whether the event type represents a completion milestone.
pub fn is_completion_event(event_type: &str) -> bool {
    event_type.ends_with(".completed")
        || event_type.ends_with(".failed")
        || event_type == EVENT_GENERAL_FAILED
}

/// Lifecycle subagent events (started/completed) for progress UI.
pub fn is_subagent_progress_event(event_type: &str) -> bool {
    let Some(parsed) = parse_namespace(event_type) else {
        return false;
    };
    parsed.domain == "subagent" && matches!(parsed.action.as_str(), "started" | "completed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid() {
        let p = parse_namespace("soothe.cognition.plan.created").unwrap();
        assert_eq!(p.domain, "cognition");
        assert_eq!(p.component, "plan");
        assert_eq!(p.action, "created");
    }

    #[test]
    fn reject_internal() {
        assert!(parse_namespace("soothe.internal.loop.completed").is_none());
    }

    #[test]
    fn classify_quiet() {
        assert_eq!(
            classify_event_verbosity(EVENT_FINAL_REPORT),
            VerbosityTier::Quiet
        );
    }
}
