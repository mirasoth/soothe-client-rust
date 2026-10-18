//! AsyncCommandClient job create, then loop-native status via loop_get.

#[path = "common/mod.rs"]
mod common;

use soothe_client::AsyncCommandClient;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let client = AsyncCommandClient::new(common::daemon_url());

    let created = client
        .job_create(
            "Echo: rust client smoke job — reply done",
            "auto",
            Some(&dir.path().display().to_string()),
        )
        .await?;
    println!("created: {created:?}");
    // Loop-native submission: response carries loop_id + status=submitted.
    let Some(loop_id) = created.get("loop_id").and_then(|v| v.as_str()) else {
        eprintln!("no loop_id in create response");
        return Ok(());
    };
    let mut params = serde_json::Map::new();
    params.insert("loop_id".into(), serde_json::json!(loop_id));
    let status = client.request("loop_get", params).await?;
    println!("loop_get: {status:?}");
    Ok(())
}
