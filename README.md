# soothe-client (Rust)

Talk to a running **soothe-daemon** over WebSocket — send prompts, stream agent
turns, run jobs.

```bash
cargo add soothe-client
```

Requires a local daemon (default `ws://127.0.0.1:8765`).

## Quick start

```rust
use soothe_client::appkit::{DaemonSession, SendTurnOptions};
use soothe_client::TEXT_COMPLETION;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = DaemonSession::new("ws://127.0.0.1:8765", None);
    session.connect(None).await?;
    session
        .send_turn(
            "Summarize this in one sentence: agents need tools.",
            Some(SendTurnOptions {
                intent_hint: Some(TEXT_COMPLETION.into()),
                ..Default::default()
            }),
        )
        .await?;
    for chunk in session.iter_turn_chunks(None).await? {
        println!("{} {:?}", chunk.mode, chunk.data);
    }
    session.close().await?;
    Ok(())
}
```

More patterns: [`examples/`](examples/) (hello → streaming → multi-turn → pool → jobs).

Route to a specialist with `preferred_subagent` (canonical ids: `explorer`,
`deep_research`, `academic_research`, `browser_use`, `planner`):

```rust
use soothe_client::{Client, SendInputOptions};

# async fn demo() -> Result<(), soothe_client::errors::Error> {
let client = Client::new("ws://127.0.0.1:8765");
client.connect().await?;
client
    .send_input(
        "Find the auth middleware",
        SendInputOptions {
            preferred_subagent: Some("explorer".into()),
            ..Default::default()
        },
    )
    .await?;
# Ok(())
# }
```

## What you get

| Need | Use |
|------|-----|
| One conversation, stream replies | `appkit::DaemonSession` |
| Jobs / cron (async) | `AsyncCommandClient` |
| Jobs / cron (scripts / sync) | `CommandClient` |
| Raw WebSocket / custom RPCs | `Client` |
| Many users / HTTP backend | `ConnectionPool` + `TurnRunner` |

## Develop

```bash
make check                 # fmt + clippy + unit tests
make test-integration      # live suite (needs soothed)
make test-examples         # live 01–06 (needs soothed)
```

## Compatibility

Same protocol-1 WebSocket contract as `soothe-client-python`,
`soothe-client-go`, and `@mirasoth/soothe-client`.

## License

MIT
