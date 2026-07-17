//! Shared helpers for examples.

#![allow(dead_code)]

use std::env;
use std::time::Duration;

use soothe_client::appkit::{DaemonSession, SendTurnOptions, TurnChunk};
use soothe_client::TEXT_COMPLETION;

pub fn daemon_url() -> String {
    env::var("SOOTHE_WS_URL")
        .or_else(|_| env::var("SOOTHE_DAEMON_URL"))
        .unwrap_or_else(|_| "ws://127.0.0.1:8765".into())
}

pub fn example_timeout() -> Duration {
    let secs: u64 = env::var("SOOTHE_EXAMPLE_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(90);
    Duration::from_secs(secs)
}

pub fn use_full_agent() -> bool {
    matches!(
        env::var("SOOTHE_EXAMPLE_AGENT").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

pub fn turn_opts() -> Option<SendTurnOptions> {
    if use_full_agent() {
        None
    } else {
        Some(SendTurnOptions {
            intent_hint: Some(TEXT_COMPLETION.into()),
            ..Default::default()
        })
    }
}

pub fn print_chunk(chunk: &TurnChunk) {
    if chunk.mode == "messages" || chunk.mode == "custom" {
        if let Some(text) = chunk
            .data
            .get("content")
            .or_else(|| chunk.data.get("text"))
            .and_then(|v| v.as_str())
        {
            print!("{text}");
            return;
        }
        if let Some(arr) = chunk.data.as_array() {
            for item in arr {
                if let Some(text) = item
                    .get("content")
                    .or_else(|| item.get("text"))
                    .and_then(|v| v.as_str())
                {
                    print!("{text}");
                }
            }
        }
    }
}

pub async fn send_and_consume(
    session: &DaemonSession,
    prompt: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    session.send_turn(prompt, turn_opts()).await?;
    let chunks = session.iter_turn_chunks(Some(example_timeout())).await?;
    let mut n = 0;
    for chunk in &chunks {
        print_chunk(chunk);
        n += 1;
    }
    println!();
    Ok(n)
}
