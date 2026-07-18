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
pub mod events;
pub mod heartbeat;
pub mod helpers;
pub mod intent_hints;
pub mod protocol;
pub mod session;
pub mod stream_terminal;
pub mod turn_boundary;
pub mod verbosity;

pub use client::{Client, ClientConfig, SendInputOptions};
pub use command_client::{AsyncCommandClient, CommandClient};
pub use config::{load_config_from_env, Config};
pub use errors::{
    disconnect_cause_name, ConnectionError, DaemonError, DisconnectCause, ReconnectError,
    StaleLoopError, TimeoutError,
};
pub use events::{
    classify_event_verbosity, is_completion_event, is_subagent_progress_event, parse_namespace,
    ParsedNamespace, EVENT_AUTOPILOT_DREAMING_ENTERED, EVENT_AUTOPILOT_DREAMING_EXITED,
    EVENT_AUTOPILOT_GOAL_BLOCKED, EVENT_AUTOPILOT_GOAL_COMPLETED, EVENT_AUTOPILOT_GOAL_CREATED,
    EVENT_AUTOPILOT_GOAL_PROGRESS, EVENT_AUTOPILOT_GOAL_SUSPENDED, EVENT_AUTOPILOT_STATUS_CHANGED,
    EVENT_BRANCH_CREATED, EVENT_BRANCH_RETRY_STARTED, EVENT_CARD_CREATED, EVENT_CARD_REPLAY_BEGIN,
    EVENT_CARD_REPLAY_END, EVENT_DEEP_RESEARCH_COMPLETED, EVENT_DEEP_RESEARCH_CRAWL_SUMMARY,
    EVENT_DEEP_RESEARCH_GATHER_SUMMARY, EVENT_DEEP_RESEARCH_PROGRESS, EVENT_DEEP_RESEARCH_STARTED,
    EVENT_DEEP_RESEARCH_STEP_COMPLETED, EVENT_EXPLORER_COMPLETED, EVENT_EXPLORER_MILESTONE,
    EVENT_EXPLORER_STARTED, EVENT_EXPLORER_STEP_COMPLETED, EVENT_FINAL_REPORT,
    EVENT_GENERAL_FAILED, EVENT_GOAL_BATCH_STARTED, EVENT_GOAL_COMPLETED, EVENT_GOAL_CREATED,
    EVENT_GOAL_DEFERRED, EVENT_GOAL_DIRECTIVES_APPLIED, EVENT_GOAL_FAILED, EVENT_GOAL_REPORTED,
    EVENT_LOOP_REATTACHED_WIRE, EVENT_MESSAGE_RECEIVED, EVENT_MESSAGE_SENT,
    EVENT_PLAN_BATCH_STARTED, EVENT_PLAN_CREATED, EVENT_PLAN_REFLECTED, EVENT_REPLAY_COMPLETE,
    EVENT_STRANGE_LOOP_COMPLETED, EVENT_STRANGE_LOOP_CONTEXT_COMPACTED,
    EVENT_STRANGE_LOOP_PLAN_DECISION, EVENT_STRANGE_LOOP_REASONED, EVENT_STRANGE_LOOP_STARTED,
    EVENT_STRANGE_LOOP_STEP_COMPLETED, EVENT_STRANGE_LOOP_STEP_QUEUED,
    EVENT_STRANGE_LOOP_STEP_STARTED, EVENT_STREAM_TOOL_CALL_UPDATE, EVENT_TOOL_CALL_UPDATES_BATCH,
    EVENT_TOOL_COMPLETED, EVENT_TOOL_ERROR, EVENT_TOOL_STARTED,
};
pub use heartbeat::{DaemonHealth, HeartbeatTracker};
pub use helpers::{
    check_daemon_status, fetch_config_section, fetch_loop_cards, fetch_loop_history,
    fetch_skills_catalog, is_daemon_live, protocol1_rpc, request_auth, request_auth_refresh,
    request_daemon_config_reload, request_daemon_shutdown, websocket_url_from_env,
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
pub use turn_boundary::{format_turn_id, frame_seq, frame_turn_id, parse_turn_generation};
pub use verbosity::{
    is_valid_verbosity_level, should_show, VerbosityTier, VERBOSITY_DEBUG, VERBOSITY_NORMAL,
    VERBOSITY_QUIET,
};

/// Crate version reported in `connection_init`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
