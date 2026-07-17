//! Client-facing error types.

use thiserror::Error;

/// Distinguishes clean vs unclean connection loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectCause {
    /// Abrupt loss: read/write error or missed pong.
    Unclean = 0,
    /// Graceful peer-initiated disconnect notification.
    Clean = 1,
}

/// Human-readable cause name for logging.
pub fn disconnect_cause_name(cause: DisconnectCause) -> &'static str {
    match cause {
        DisconnectCause::Clean => "clean",
        DisconnectCause::Unclean => "unclean",
    }
}

/// WebSocket connection failure.
#[derive(Debug, Error)]
#[error("connection error to {url} (attempt {attempt}): {source}")]
pub struct ConnectionError {
    /// Daemon WebSocket URL.
    pub url: String,
    /// Attempt number (1-based).
    pub attempt: u32,
    /// Underlying cause.
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

impl ConnectionError {
    /// Create a connection error.
    pub fn new(
        url: impl Into<String>,
        attempt: u32,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            url: url.into(),
            attempt,
            source: source.into(),
        }
    }
}

/// Error reported by the daemon (protocol-1 structured error object).
#[derive(Debug, Error, Clone)]
#[error("daemon error [{code}]: {message}")]
pub struct DaemonError {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable message.
    pub message: String,
    /// Optional structured data.
    pub data: Option<serde_json::Value>,
}

impl DaemonError {
    /// Create a daemon error.
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attach optional data.
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// Timeout waiting for a daemon response.
#[derive(Debug, Error)]
#[error("timeout after {duration} waiting for {operation}")]
pub struct TimeoutError {
    /// Operation name.
    pub operation: String,
    /// Duration string.
    pub duration: String,
}

impl TimeoutError {
    /// Create a timeout error.
    pub fn new(operation: impl Into<String>, duration: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            duration: duration.into(),
        }
    }
}

/// Bounded reconnect attempts exhausted.
#[derive(Debug, Error)]
#[error("reconnect to {url} failed after {attempts} attempts")]
pub struct ReconnectError {
    /// Daemon URL.
    pub url: String,
    /// Attempts made.
    pub attempts: u32,
    /// Last cause.
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ReconnectError {
    /// Create a reconnect error.
    pub fn new(
        url: impl Into<String>,
        attempts: u32,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            url: url.into(),
            attempts,
            source,
        }
    }
}

/// Loop accepted reattach but failed the `loop_get` liveness probe.
#[derive(Debug, Error)]
#[error("stale loop {loop_id}: reattach accepted but liveness probe failed")]
pub struct StaleLoopError {
    /// Loop id that failed the probe.
    pub loop_id: String,
    /// Underlying cause.
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl StaleLoopError {
    /// Create a stale-loop error.
    pub fn new(
        loop_id: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            loop_id: loop_id.into(),
            source,
        }
    }
}

/// Unified client error.
#[derive(Debug, Error)]
pub enum Error {
    /// Connection failure.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// Daemon structured error.
    #[error(transparent)]
    Daemon(#[from] DaemonError),
    /// Timeout.
    #[error(transparent)]
    Timeout(#[from] TimeoutError),
    /// Reconnect exhausted.
    #[error(transparent)]
    Reconnect(#[from] ReconnectError),
    /// Stale loop after reattach.
    #[error(transparent)]
    StaleLoop(#[from] StaleLoopError),
    /// Protocol / codec / transport failure.
    #[error("{0}")]
    Protocol(String),
    /// I/O failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// JSON failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Generic message.
    #[error("{0}")]
    Message(String),
}

impl Error {
    /// Protocol error helper.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
    }

    /// Message helper.
    pub fn msg(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;
