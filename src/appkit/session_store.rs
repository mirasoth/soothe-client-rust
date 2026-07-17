//! Session ↔ loop persistence.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;

/// Persisted session record.
#[derive(Debug, Clone, Default)]
pub struct SessionRecord {
    /// Application session id.
    pub session_id: String,
    /// Bound loop id.
    pub loop_id: Option<String>,
    /// Workspace id.
    pub workspace_id: String,
    /// User id.
    pub user_id: String,
    /// Reset counter.
    pub reset_count: u32,
    /// Message log.
    pub messages: Vec<Value>,
}

/// Session ↔ loop persistence.
pub trait SessionStore: Send + Sync {
    /// Load session.
    fn get_session(
        &self,
        session_id: &str,
    ) -> impl std::future::Future<Output = Option<SessionRecord>> + Send;

    /// Create session.
    fn create_session(
        &self,
        record: SessionRecord,
    ) -> impl std::future::Future<Output = SessionRecord> + Send;

    /// Touch last-used (no-op ok).
    fn update_last_used(&self, session_id: &str) -> impl std::future::Future<Output = ()> + Send;

    /// Increment reset count.
    fn increment_reset_count(
        &self,
        session_id: &str,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Lookup loop id.
    fn get_loop_id_for_session(
        &self,
        session_id: &str,
    ) -> impl std::future::Future<Output = Option<String>> + Send;

    /// Bind loop id.
    fn set_loop_id(
        &self,
        session_id: &str,
        loop_id: &str,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Append a message.
    fn append_message(
        &self,
        session_id: &str,
        message: Value,
    ) -> impl std::future::Future<Output = ()> + Send;
}

/// Process-local store.
#[derive(Clone, Default)]
pub struct InMemorySessionStore {
    inner: Arc<Mutex<HashMap<String, SessionRecord>>>,
}

impl InMemorySessionStore {
    /// Create empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl SessionStore for InMemorySessionStore {
    async fn get_session(&self, session_id: &str) -> Option<SessionRecord> {
        self.inner.lock().await.get(session_id).cloned()
    }

    async fn create_session(&self, record: SessionRecord) -> SessionRecord {
        let mut map = self.inner.lock().await;
        map.insert(record.session_id.clone(), record.clone());
        record
    }

    async fn update_last_used(&self, _session_id: &str) {}

    async fn increment_reset_count(&self, session_id: &str) {
        if let Some(rec) = self.inner.lock().await.get_mut(session_id) {
            rec.reset_count += 1;
        }
    }

    async fn get_loop_id_for_session(&self, session_id: &str) -> Option<String> {
        self.inner
            .lock()
            .await
            .get(session_id)
            .and_then(|r| r.loop_id.clone())
    }

    async fn set_loop_id(&self, session_id: &str, loop_id: &str) {
        let mut map = self.inner.lock().await;
        if let Some(rec) = map.get_mut(session_id) {
            rec.loop_id = Some(loop_id.to_string());
        } else {
            map.insert(
                session_id.to_string(),
                SessionRecord {
                    session_id: session_id.to_string(),
                    loop_id: Some(loop_id.to_string()),
                    ..Default::default()
                },
            );
        }
    }

    async fn append_message(&self, session_id: &str, message: Value) {
        if let Some(rec) = self.inner.lock().await.get_mut(session_id) {
            rec.messages.push(message);
        }
    }
}
