//! Per-turn stream filtering counters for observability.

/// Tracks per-turn stream filtering counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnEventStats {
    /// Chunks dropped by early stream filtering.
    pub filtered_early: usize,
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
