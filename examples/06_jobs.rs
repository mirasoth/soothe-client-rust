//! CommandClient job create / status / cancel.

#[path = "common/mod.rs"]
mod common;

use soothe_client::AsyncCommandClient;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let client = AsyncCommandClient::new(common::daemon_url());

    match client.autopilot_status().await {
        Ok(status) => println!("autopilot_status: {status:?}"),
        Err(e) => println!("autopilot_status skipped: {e}"),
    }

    let created = client
        .job_create(
            "Echo: rust client smoke job — reply done",
            Some(&dir.path().display().to_string()),
        )
        .await?;
    println!("created: {created:?}");
    let job_id = created
        .get("job_id")
        .or_else(|| created.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if job_id.is_empty() {
        eprintln!("no job_id in create response");
        return Ok(());
    }
    let status = client.job_status(&job_id).await?;
    println!("status: {status:?}");
    let cancelled = client.job_cancel(&job_id).await?;
    println!("cancel: {cancelled:?}");
    Ok(())
}
