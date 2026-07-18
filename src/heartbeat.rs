//! Daemon heartbeat tracking for aliveness checks.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

/// Snapshot of daemon health from heartbeat tracking.
#[derive(Debug, Clone)]
pub struct DaemonHealth {
    /// Instant of last received heartbeat (or tracker start if none yet).
    pub last_heartbeat: Instant,
    /// Daemon state: `"running"` or `"idle"` (when known).
    pub state: String,
    /// Loop id the daemon is processing (if running).
    pub loop_id: String,
    /// True if heartbeat received within the alive threshold (or grace period).
    pub is_alive: bool,
}

/// Tracks daemon heartbeat events to monitor aliveness.
#[derive(Debug)]
pub struct HeartbeatTracker {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    last_heartbeat: Option<Instant>,
    daemon_state: String,
    heartbeat_loop_id: String,
    alive_threshold: Duration,
    start_time: Instant,
}

impl HeartbeatTracker {
    /// Create a tracker with the default 15s alive threshold.
    pub fn new() -> Self {
        Self::with_threshold(Duration::from_secs(15))
    }

    /// Create a tracker with a custom alive threshold.
    pub fn with_threshold(alive_threshold: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                last_heartbeat: None,
                daemon_state: String::new(),
                heartbeat_loop_id: String::new(),
                alive_threshold,
                start_time: Instant::now(),
            }),
        }
    }

    /// Process a heartbeat event payload (typically `event["data"]`).
    pub fn update(&self, heartbeat_data: Option<&Value>) {
        let mut g = self.inner.lock().expect("heartbeat lock");
        g.last_heartbeat = Some(Instant::now());
        if let Some(data) = heartbeat_data.and_then(|v| v.as_object()) {
            if let Some(state) = data.get("state").and_then(|v| v.as_str()) {
                g.daemon_state = state.to_string();
            }
            if let Some(loop_id) = data.get("loop_id").and_then(|v| v.as_str()) {
                g.heartbeat_loop_id = loop_id.to_string();
            }
        }
    }

    /// Record that an application-level pong was received.
    pub fn note_pong(&self) {
        let mut g = self.inner.lock().expect("heartbeat lock");
        g.last_heartbeat = Some(Instant::now());
    }

    /// Current daemon health snapshot.
    pub fn get_health(&self) -> DaemonHealth {
        let g = self.inner.lock().expect("heartbeat lock");
        let now = Instant::now();
        let last = g.last_heartbeat.unwrap_or(g.start_time);
        let grace = Duration::from_secs(20);
        let is_alive = now.duration_since(last) < g.alive_threshold
            || now.duration_since(g.start_time) < grace;
        DaemonHealth {
            last_heartbeat: last,
            state: g.daemon_state.clone(),
            loop_id: g.heartbeat_loop_id.clone(),
            is_alive,
        }
    }

    /// True when daemon state is `"running"`.
    pub fn is_processing(&self) -> bool {
        self.inner.lock().expect("heartbeat lock").daemon_state == "running"
    }

    /// True when daemon state is `"idle"`.
    pub fn is_idle(&self) -> bool {
        self.inner.lock().expect("heartbeat lock").daemon_state == "idle"
    }
}

impl Default for HeartbeatTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grace_period_alive() {
        let t = HeartbeatTracker::new();
        assert!(t.get_health().is_alive);
    }
}
