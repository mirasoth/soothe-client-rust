//! Single-flight query gate per session.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// Session already has an in-flight query.
#[derive(Debug, Clone, thiserror::Error)]
#[error("query busy for session {0}")]
pub struct ErrQueryBusy(pub String);

/// Single-flight gate keyed by session id.
#[derive(Clone, Default)]
pub struct QueryGate {
    inflight: Arc<Mutex<HashMap<String, bool>>>,
}

impl QueryGate {
    /// Create a gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire exclusive query slot.
    pub async fn acquire(&self, session_id: &str) -> Result<(), ErrQueryBusy> {
        let mut map = self.inflight.lock().await;
        if map.get(session_id).copied().unwrap_or(false) {
            return Err(ErrQueryBusy(session_id.to_string()));
        }
        map.insert(session_id.to_string(), true);
        Ok(())
    }

    /// Mark cancelled (slot still held until release).
    pub async fn cancel(&self, _session_id: &str) {}

    /// Release exclusive slot.
    pub async fn release(&self, session_id: &str) {
        self.inflight.lock().await.remove(session_id);
    }
}
