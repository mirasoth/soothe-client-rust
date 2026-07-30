//! Application kit: DaemonSession, pool, TurnRunner, and helpers.

mod app_key;
mod attachments;
mod broadcaster;
mod chunk_filter;
mod classifier;
mod daemon_session;
mod loop_session_store;
mod observability;
mod pool;
mod query_gate;
mod thinking_step;
mod turn_boundary;
mod turn_runner;

pub use app_key::AppKey;
pub use attachments::{compact_attachments, compact_image_attachment, CompactImageOptions};
pub use broadcaster::{SseBroadcaster, SseEvent};
pub use chunk_filter::should_drop_stream_chunk_early;
pub use classifier::{
    ChatEventResult, ChatEventTerminal, ClassifierConfig, ClassifierConfigError, EventClassifier,
};
pub use daemon_session::{
    DaemonSession, DaemonSessionOptions, EarlyDropFn, SendTurnOptions, TurnChunk,
};
pub use loop_session_store::{InMemoryLoopSessionStore, LoopSessionEntry, LoopSessionStore};
pub use observability::TurnEventStats;
pub use pool::{
    input_message_for_loop, ConnectionPool, ErrPoolExhausted, PoolConfig, PoolStats, PooledConn,
};
pub use query_gate::{CancelFn, ErrQueryBusy, QueryGate, SendCancelFn};
pub use thinking_step::{default_thinking_step_events, extract_thinking_step};
pub use turn_boundary::{
    is_daemon_turn_end_event, TurnBoundary, TurnLifecycleGate, TURN_END_IDLE, TURN_END_STOPPED,
    TURN_END_STREAM_END,
};
pub use turn_runner::{InputOpts, TimeoutPolicy, TurnConfig, TurnRunner};
