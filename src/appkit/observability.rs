//! Per-turn stream filtering counters for observability.

/// Tracks per-turn stream filtering counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnEventStats {
    /// Total events seen this turn.
    pub total: usize,
    /// `messages` mode chunks.
    pub messages: usize,
    /// `updates` mode chunks.
    pub updates: usize,
    /// `custom` mode chunks.
    pub custom: usize,
    /// Chunks skipped (not yielded) by non-early-drop logic.
    pub skipped: usize,
    /// Chunks dropped by early stream filtering.
    pub filtered_early: usize,
    /// Tool-call events.
    pub tool_calls: usize,
    /// Tool-result events.
    pub tool_results: usize,
    /// Text (assistant) delta chunks.
    pub text_chunks: usize,
    /// Heartbeats dropped during the turn.
    pub heartbeats_dropped: usize,
    /// Events drained after idle status.
    pub post_idle_drained: usize,
    /// Inbound frames dropped under queue pressure.
    pub inbound_dropped: usize,
}

impl TurnEventStats {
    /// Returns an empty stats bag.
    pub fn new() -> Self {
        Self::default()
    }
}
