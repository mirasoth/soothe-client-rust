//! Client-facing event namespace constants for the Soothe daemon wire protocol.
//!
//! Internal catalog types (`soothe.internal.*`) are server-only and are never
//! broadcast to WebSocket clients.

/// Plan created.
pub const EVENT_PLAN_CREATED: &str = "soothe.cognition.plan.created";

/// Explorer subagent started.
pub const EVENT_EXPLORER_STARTED: &str = "soothe.subagent.explorer.started";
/// Explorer milestone.
pub const EVENT_EXPLORER_MILESTONE: &str = "soothe.subagent.explorer.milestone";
/// Explorer step completed.
pub const EVENT_EXPLORER_STEP_COMPLETED: &str = "soothe.subagent.explorer.step.completed";
/// Explorer completed.
pub const EVENT_EXPLORER_COMPLETED: &str = "soothe.subagent.explorer.completed";

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

/// Replay complete control-plane frame.
pub const EVENT_REPLAY_COMPLETE: &str = "replay_complete";
/// Loop reattached control-plane frame.
pub const EVENT_LOOP_REATTACHED_WIRE: &str = "loop_reattached";
