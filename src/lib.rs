//! Soothe WebSocket client for talking to a running soothe-daemon.
//!
//! Public surface mirrors Python / Go / TypeScript RFC-629 tiers:
//!
//! ```text
//! Need                         → Entry point
//! One conversation, stream     → appkit::DaemonSession
//! Jobs / cron one-shots        → CommandClient
//! Raw protocol / custom        → Client
//! Multi-user HTTP backend      → appkit::ConnectionPool + TurnRunner
//! ```

#![deny(missing_docs)]
#![allow(clippy::result_large_err)]

pub mod appkit;
pub mod client;
pub mod command_client;
pub mod config;
pub mod errors;
pub mod helpers;
pub mod intent_hints;
pub mod protocol;
pub mod session;
pub mod stream_terminal;

pub use client::{Client, ClientConfig, SendInputOptions};
pub use command_client::{AsyncCommandClient, CommandClient};
pub use config::{load_config_from_env, Config};
pub use errors::{
    disconnect_cause_name, ConnectionError, DaemonError, DisconnectCause, ReconnectError,
    StaleLoopError, TimeoutError,
};
pub use helpers::{
    check_daemon_status, fetch_config_section, fetch_loop_cards, fetch_loop_history,
    fetch_skills_catalog, is_daemon_live, protocol1_rpc, request_daemon_config_reload,
    request_daemon_shutdown, websocket_url_from_env,
};
pub use intent_hints::{
    validate_loop_input_intent_hint, DEFAULT_DELIVERABLE_PHASES, EMBED, IMAGE_TO_TEXT, OCR,
    TEXT_COMPLETION,
};
pub use protocol::{
    decode_message, expand_wire_messages, new_connection_init, new_notification, new_ping,
    new_pong, new_request, new_request_id, new_subscribe, new_unsubscribe, Envelope, ErrorObject,
    MessageType, CLIENT_VERSION, DEFAULT_CLIENT_CAPABILITIES, PROTO_VERSION,
};
pub use session::{bootstrap_loop_session, connect_with_retries, BootstrapOptions};
pub use stream_terminal::{
    extract_loop_id_from_inbound, inbound_needs_delivery_ack, is_turn_end_custom_data,
    is_turn_progress_chunk, stale_pending_frame_label, STREAM_END,
};

/// Crate version reported in `connection_init`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
