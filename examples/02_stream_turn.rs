//! Stream turn chunks as they arrive.

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
    session
        .send_turn(
            "Count from 1 to 3, one number per line.",
            common::turn_opts(),
        )
        .await?;
    let chunks = session
        .iter_turn_chunks(Some(common::example_timeout()))
        .await?;
    for chunk in &chunks {
        print!("[{}] ", chunk.mode);
        common::print_chunk(chunk);
        println!();
    }
    session.close().await?;
    Ok(())
}
