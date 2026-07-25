//! Single-flight query gate per application key.
//!
//! [`QueryGate`] enforces single-flight query execution per app key and
//! **cancel-before-context ordering**: when a query is cancelled, the daemon is
//! told to stop (`command_request` with `command:"cancel"`) **before** the local
//! cancel callback runs, on a detached 10s timeout so the caller's cancellation
//! cannot block the wire send.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::errors::Result;

/// Local cancel callback for an in-flight query (typically a timeout context cancel).
pub type CancelFn = Arc<dyn Fn() + Send + Sync>;

/// Async daemon-cancel sender (`command_request` cancel for the loop).
pub type SendCancelFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Returned when an app key already has an in-flight query.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("appkit: query already in progress for app key")]
pub struct ErrQueryBusy;

struct GateState {
    cancels: HashMap<String, CancelFn>,
    send_cancels: HashMap<String, SendCancelFn>,
}

/// Enforces single-flight query execution per app key with daemon-first cancel.
pub struct QueryGate {
    inner: Mutex<GateState>,
}

impl QueryGate {
    /// Construct an empty gate.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(GateState {
                cancels: HashMap::new(),
                send_cancels: HashMap::new(),
            }),
        }
    }

    /// Reserve `app_key` for one agent turn.
    ///
    /// Returns [`ErrQueryBusy`] when a query is already in flight. Pass the local
    /// cancel callback for the query's timeout context (the same one the turn loop
    /// derives from). `send_cancel`, when provided, sends the daemon cancel and is
    /// invoked from [`Self::cancel`] on a detached 10s timeout.
    pub fn acquire(
        &self,
        app_key: &str,
        cancel: CancelFn,
        send_cancel: Option<SendCancelFn>,
    ) -> std::result::Result<(), ErrQueryBusy> {
        let mut state = self.inner.lock().expect("query gate mutex poisoned");
        if state.cancels.contains_key(app_key) {
            return Err(ErrQueryBusy);
        }
        state.cancels.insert(app_key.to_string(), cancel);
        if let Some(send) = send_cancel {
            state.send_cancels.insert(app_key.to_string(), send);
        }
        Ok(())
    }

    /// Cooperatively stop a running query for `app_key`.
    ///
    /// Sends the daemon cancel first (10s timeout, detached from the caller's
    /// cancellation), then invokes the local cancel callback. Returns `Ok(())` when
    /// no query is in flight (intent already satisfied).
    pub async fn cancel(&self, app_key: &str) -> Result<()> {
        let (cancel, send_cancel) = {
            let mut state = self.inner.lock().expect("query gate mutex poisoned");
            let cancel = state.cancels.remove(app_key);
            let send_cancel = state.send_cancels.remove(app_key);
            (cancel, send_cancel)
        };

        if cancel.is_none() && send_cancel.is_none() {
            return Ok(());
        }

        if let Some(send_cancel) = send_cancel {
            let fut = send_cancel();
            let _ = tokio::time::timeout(Duration::from_secs(10), fut).await;
        }

        if let Some(cancel) = cancel {
            cancel();
        }
        Ok(())
    }

    /// Clear the gate for `app_key` without sending a daemon cancel.
    ///
    /// Call when a query completes normally (success or local failure) so the next
    /// turn can acquire.
    pub fn release(&self, app_key: &str) {
        let mut state = self.inner.lock().expect("query gate mutex poisoned");
        state.cancels.remove(app_key);
        state.send_cancels.remove(app_key);
    }

    /// Update the daemon-cancel sender for an already-acquired session.
    pub fn set_send_cancel(&self, app_key: &str, send_cancel: SendCancelFn) {
        let mut state = self.inner.lock().expect("query gate mutex poisoned");
        if !state.cancels.contains_key(app_key) {
            return;
        }
        state.send_cancels.insert(app_key.to_string(), send_cancel);
    }

    /// Update the local cancel callback for an already-acquired session.
    pub fn replace_cancel(&self, app_key: &str, cancel: CancelFn) {
        let mut state = self.inner.lock().expect("query gate mutex poisoned");
        if !state.cancels.contains_key(app_key) {
            return;
        }
        state.cancels.insert(app_key.to_string(), cancel);
    }

    /// Report whether a query is in flight for `app_key`.
    pub fn is_active(&self, app_key: &str) -> bool {
        self.inner
            .lock()
            .expect("query gate mutex poisoned")
            .cancels
            .contains_key(app_key)
    }
}

impl Default for QueryGate {
    fn default() -> Self {
        Self::new()
    }
}
