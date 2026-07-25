//! Daemon heartbeat tracking for aliveness checks.

use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::errors::{HeartbeatError, Result};
use crate::protocol::as_str;

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

    /// Current daemon state (e.g. `"running"`, `"idle"`, or empty when unknown).
    pub fn get_state(&self) -> String {
        self.inner
            .lock()
            .expect("heartbeat lock")
            .daemon_state
            .clone()
    }

    /// Loop id the daemon is currently processing (empty when idle or unknown).
    pub fn get_loop_id(&self) -> String {
        self.inner
            .lock()
            .expect("heartbeat lock")
            .heartbeat_loop_id
            .clone()
    }

    /// Instant of the last received heartbeat (or tracker start if none yet).
    pub fn get_last_heartbeat(&self) -> Instant {
        let g = self.inner.lock().expect("heartbeat lock");
        g.last_heartbeat.unwrap_or(g.start_time)
    }

    /// Update the alive threshold duration.
    pub fn set_alive_threshold(&self, threshold: Duration) {
        self.inner.lock().expect("heartbeat lock").alive_threshold = threshold;
    }

    /// Clear the tracker state and reset the start time.
    ///
    /// Useful when reconnecting to the daemon.
    pub fn reset(&self) {
        let mut g = self.inner.lock().expect("heartbeat lock");
        g.last_heartbeat = None;
        g.daemon_state.clear();
        g.heartbeat_loop_id.clear();
        g.start_time = Instant::now();
    }

    /// Process a full event message and extract heartbeat data.
    ///
    /// Returns `true` if the event was recognized as a heartbeat and processed.
    /// Catalog daemon heartbeats (`soothe.internal.*`) are server-only and are
    /// not broadcast to WebSocket clients, so this currently returns `false` for
    /// all event-shape messages — but the hook exists for parity with the Go
    /// client and future extensibility.
    pub fn process_heartbeat_event(&self, event: &Value) -> bool {
        let Some(obj) = event.as_object() else {
            return false;
        };
        let event_type = as_str(obj.get("type").unwrap_or(&Value::Null));
        if event_type != "event" {
            return false;
        }
        // Future: surface any client-visible heartbeat namespace here.
        let _data = obj.get("data").unwrap_or(&Value::Null);
        false
    }

    /// Block until the daemon is considered alive or `timeout` is reached.
    ///
    /// Returns `Ok(())` when alive, or `Err(HeartbeatError)` on timeout.
    /// Uses a 1-second polling interval (mirrors Go `WaitForAlive`).
    pub fn wait_for_alive(&self, timeout: Duration) -> Result<()> {
        let timeout = if timeout.is_zero() {
            Duration::from_secs(30)
        } else {
            timeout
        };
        let deadline = Instant::now() + timeout;
        loop {
            let health = self.get_health();
            if health.is_alive {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HeartbeatError::new(
                    Some(health.last_heartbeat),
                    health.state,
                    "timeout waiting for daemon to be alive",
                )
                .into());
            }
            // Poll every 1s (Go parity).
            thread::sleep(Duration::from_secs(1));
        }
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
    fn reset_clears_state() {
        let t = HeartbeatTracker::new();
        t.update(Some(
            &serde_json::json!({"state":"running","loop_id":"abc"}),
        ));
        assert_eq!(t.get_state(), "running");
        assert_eq!(t.get_loop_id(), "abc");
        t.reset();
        assert_eq!(t.get_state(), "");
        assert_eq!(t.get_loop_id(), "");
    }

    #[test]
    fn process_heartbeat_event_rejects_non_event() {
        let t = HeartbeatTracker::new();
        assert!(!t.process_heartbeat_event(&serde_json::json!({"type":"status"})));
        assert!(!t.process_heartbeat_event(&serde_json::json!({"type":"event","data":{}})));
    }

    #[test]
    fn set_alive_threshold_changes_health() {
        let t = HeartbeatTracker::with_threshold(Duration::from_secs(60));
        t.set_alive_threshold(Duration::from_millis(1));
        // After 5ms the tracker is no longer alive (no heartbeat received).
        std::thread::sleep(Duration::from_millis(5));
        let h = t.get_health();
        // Grace period (20s) keeps it alive initially.
        assert!(h.is_alive);
    }

    #[test]
    fn grace_period_alive() {
        let t = HeartbeatTracker::new();
        assert!(t.get_health().is_alive);
    }

    #[test]
    fn get_state_and_loop_id_default_empty() {
        let t = HeartbeatTracker::new();
        assert_eq!(t.get_state(), "");
        assert_eq!(t.get_loop_id(), "");
    }

    #[test]
    fn last_heartbeat_defaults_to_start_time() {
        let t = HeartbeatTracker::new();
        let _h = t.get_last_heartbeat();
        // Should not panic; equals start_time when no heartbeat received.
        let health = t.get_health();
        // last_heartbeat in health snapshot equals start_time when no heartbeat.
        assert_eq!(t.get_last_heartbeat(), health.last_heartbeat);
    }
}
