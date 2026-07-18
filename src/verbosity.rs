//! Verbosity levels and tiers for event filtering.

/// Quiet verbosity.
pub const VERBOSITY_QUIET: &str = "quiet";
/// Normal verbosity.
pub const VERBOSITY_NORMAL: &str = "normal";
/// Debug verbosity.
pub const VERBOSITY_DEBUG: &str = "debug";

/// Minimum verbosity level at which content is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum VerbosityTier {
    /// Always visible (errors, assistant text, final reports).
    Quiet = 0,
    /// Standard progress (plan updates, milestones, agentic loop).
    Normal = 1,
    /// Detailed internals (protocol events, tool calls, subagent activity).
    Detailed = 2,
    /// Everything including internals (thinking, heartbeats).
    Debug = 3,
    /// Never shown at any level (implementation details).
    Internal = 99,
}

fn verbosity_level_value(verbosity: &str) -> i32 {
    match verbosity {
        VERBOSITY_QUIET => 0,
        VERBOSITY_NORMAL => 1,
        VERBOSITY_DEBUG => 3,
        _ => 1,
    }
}

/// Returns true if content at `tier` is visible at `verbosity`.
pub fn should_show(tier: VerbosityTier, verbosity: &str) -> bool {
    if tier == VerbosityTier::Internal {
        return false;
    }
    (tier as i32) <= verbosity_level_value(verbosity)
}

/// Whether `s` is a valid verbosity level string.
pub fn is_valid_verbosity_level(s: &str) -> bool {
    matches!(s, VERBOSITY_QUIET | VERBOSITY_NORMAL | VERBOSITY_DEBUG)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_always_shows_quiet_tier() {
        assert!(should_show(VerbosityTier::Quiet, VERBOSITY_QUIET));
        assert!(!should_show(VerbosityTier::Normal, VERBOSITY_QUIET));
    }

    #[test]
    fn internal_never_shows() {
        assert!(!should_show(VerbosityTier::Internal, VERBOSITY_DEBUG));
    }

    #[test]
    fn valid_levels() {
        assert!(is_valid_verbosity_level("normal"));
        assert!(!is_valid_verbosity_level("loud"));
    }
}
