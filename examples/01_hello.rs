//! Minimal: one prompt, wait for the reply, print what it said.

#[path = "common/mod.rs"]
mod common;

use soothe_client::appkit::{DaemonSession, DaemonSessionOptions};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let opts = DaemonSessionOptions {
        workspace: Some(dir.path().display().to_string()),
        ..Default::default()
    };
    let session = DaemonSession::new(common::daemon_url(), Some(opts));
    session.connect(None).await?;
    println!("loop={}", session.loop_id().await);
    let _ = common::send_and_consume(&session, "Say hello in one short sentence.").await?;
    session.close().await?;
    Ok(())
}
