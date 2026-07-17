//! ConnectionPool + TurnRunner for two sessions.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use soothe_client::appkit::{
    ConnectionPool, EventClassifier, InMemorySessionStore, InputOpts, QueryGate, TurnRunner,
};
use soothe_client::TEXT_COMPLETION;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let workspace = dir.path().display().to_string();
    let store = Arc::new(InMemorySessionStore::new());
    let pool = Arc::new(ConnectionPool::new(
        common::daemon_url(),
        store.clone(),
        None,
    ));
    let runner = TurnRunner::new(
        pool.clone(),
        QueryGate::new(),
        EventClassifier::new(),
        store,
        None,
    );

    let opts = Some(InputOpts {
        intent_hint: Some(TEXT_COMPLETION.into()),
        ..Default::default()
    });

    let a = runner
        .execute(
            "session-a",
            "Say hi from session A in three words.",
            "user-a",
            &workspace,
            None,
            opts.clone(),
        )
        .await?;
    println!("A: {a}");

    let b = runner
        .execute(
            "session-b",
            "Say hi from session B in three words.",
            "user-b",
            &workspace,
            None,
            opts,
        )
        .await?;
    println!("B: {b}");

    let stats = pool.stats().await;
    println!("pool active={} idle={}", stats.active, stats.idle);
    pool.stop().await;
    Ok(())
}
