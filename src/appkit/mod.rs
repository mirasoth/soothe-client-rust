//! Application kit: DaemonSession, pool, TurnRunner, and helpers.

mod classifier;
mod daemon_session;
mod pool;
mod query_gate;
mod session_store;
mod turn_boundary;
mod turn_runner;

pub use classifier::EventClassifier;
pub use daemon_session::{DaemonSession, DaemonSessionOptions, SendTurnOptions, TurnChunk};
pub use pool::{input_message_for_loop, ConnectionPool, PoolConfig, PoolStats, PooledConn};
pub use query_gate::{ErrQueryBusy, QueryGate};
pub use session_store::{InMemorySessionStore, SessionRecord, SessionStore};
pub use turn_boundary::{
    is_daemon_turn_end_event, TurnBoundary, TurnLifecycleGate, TURN_END_IDLE, TURN_END_STOPPED,
    TURN_END_STREAM_END,
};
pub use turn_runner::{InputOpts, TimeoutPolicy, TurnConfig, TurnRunner};
