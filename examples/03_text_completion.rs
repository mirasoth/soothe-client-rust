//! Force text_completion intent hint.

#[path = "common/mod.rs"]
mod common;

use soothe_client::appkit::{DaemonSession, DaemonSessionOptions, SendTurnOptions};
use soothe_client::TEXT_COMPLETION;
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
    session
        .send_turn(
            "Reply with exactly: ok",
            Some(SendTurnOptions {
                intent_hint: Some(TEXT_COMPLETION.into()),
                ..Default::default()
            }),
        )
        .await?;
    let chunks = session
        .iter_turn_chunks(Some(common::example_timeout()))
        .await?;
    for chunk in &chunks {
        common::print_chunk(chunk);
    }
    println!();
    session.close().await?;
    Ok(())
}
