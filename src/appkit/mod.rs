//! Application kit: DaemonSession, pool, TurnRunner, and helpers.

mod classifier;
mod daemon_session;
mod pool;
mod query_gate;
mod session_store;
mod turn_runner;

pub use classifier::EventClassifier;
pub use daemon_session::{DaemonSession, DaemonSessionOptions, SendTurnOptions, TurnChunk};
pub use pool::{ConnectionPool, PoolConfig, PoolStats, PooledConn};
pub use query_gate::{ErrQueryBusy, QueryGate};
pub use session_store::{InMemorySessionStore, SessionRecord, SessionStore};
pub use turn_runner::{InputOpts, TimeoutPolicy, TurnConfig, TurnRunner};
