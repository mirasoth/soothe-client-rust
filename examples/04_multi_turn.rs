//! Multi-turn follow-ups on the same loop.

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

    for (i, prompt) in [
        "Remember the codeword: orchid. Reply 'noted'.",
        "What codeword did I give you?",
        "Reply with only that codeword.",
    ]
    .into_iter()
    .enumerate()
    {
        println!("--- turn {} ---", i + 1);
        let _ = common::send_and_consume(&session, prompt).await?;
    }

    session.close().await?;
    Ok(())
}
