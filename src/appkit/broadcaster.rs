//! Generic SSE-style pub/sub fan-out for application backends.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

/// One Server-Sent Event payload. The event type vocabulary
/// (`delta` / `complete` / `query_error` / `status_change` / …) is app-defined.
#[derive(Debug, Clone)]
pub struct SseEvent {
    /// Application-defined event type name.
    pub event_type: String,
    /// Structured event payload.
    pub data: Value,
}

/// Generic, string-keyed pub/sub fan-out for SSE-style event delivery.
///
/// Applications map their own session ids at the boundary. Non-blocking sends drop on a
/// full subscriber channel so a slow consumer cannot stall the broadcaster.
pub struct SseBroadcaster {
    subscribers: Mutex<HashMap<String, HashMap<String, mpsc::Sender<SseEvent>>>>,
}

impl Default for SseBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl SseBroadcaster {
    /// Creates an empty broadcaster.
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a new subscriber for `app_key`.
    ///
    /// Returns `(subscriber_id, receiver)`. The channel is buffered (cap 100) so a
    /// transient slow consumer does not drop events immediately.
    pub fn subscribe(&self, app_key: &str) -> (String, mpsc::Receiver<SseEvent>) {
        let (tx, rx) = mpsc::channel(100);
        let subscriber_id = Uuid::new_v4().to_string();

        let mut subs = self.subscribers.lock().expect("broadcaster mutex poisoned");
        subs.entry(app_key.to_string())
            .or_default()
            .insert(subscriber_id.clone(), tx);

        (subscriber_id, rx)
    }

    /// Removes a subscriber by id and closes its channel.
    pub fn unsubscribe(&self, app_key: &str, subscriber_id: &str) {
        let mut subs = self.subscribers.lock().expect("broadcaster mutex poisoned");
        let Some(app_subs) = subs.get_mut(app_key) else {
            return;
        };
        app_subs.remove(subscriber_id);
        if app_subs.is_empty() {
            subs.remove(app_key);
        }
    }

    /// Sends an event to all subscribers for `app_key`.
    ///
    /// Non-blocking: a full subscriber channel is skipped (drop-on-full) so one slow
    /// consumer cannot block the others.
    pub fn broadcast(&self, app_key: &str, event: SseEvent) {
        let subs = self.subscribers.lock().expect("broadcaster mutex poisoned");
        let Some(app_subs) = subs.get(app_key) else {
            return;
        };
        for tx in app_subs.values() {
            let _ = tx.try_send(event.clone());
        }
    }

    /// Closes all subscriber channels for `app_key` and removes the entry.
    pub fn close(&self, app_key: &str) {
        let mut subs = self.subscribers.lock().expect("broadcaster mutex poisoned");
        subs.remove(app_key);
    }

    /// Closes every subscriber channel across all sessions.
    pub fn close_all(&self) {
        let mut subs = self.subscribers.lock().expect("broadcaster mutex poisoned");
        subs.clear();
    }
}
