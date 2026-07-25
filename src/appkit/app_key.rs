//! Application conversation key type.

/// Application conversation key used by [`super::ConnectionPool`], [`super::QueryGate`],
/// [`super::TurnRunner`], and [`super::SessionStore`] (e.g. Triarch `chat_id`).
///
/// This is **not** a daemon protocol identity. The daemon's first-class ids are
/// `loop_id` (conversation continuity) and `client_id` (WebSocket connection).
/// AppKit maps `AppKey` → `loop_id` via [`super::SessionStore`]; that mapping never
/// leaves the product process as a wire `session_id`.
pub type AppKey = String;
