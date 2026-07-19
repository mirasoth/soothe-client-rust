# Examples

Mirror the Python `examples/01`–`06` ladder. Require a live daemon
(`SOOTHE_WS_URL`, default `ws://127.0.0.1:8765`).

| Script | What it shows |
|--------|----------------|
| `01_hello` | Connect + one prompt |
| `02_stream_turn` | Stream chunks as they arrive |
| `03_text_completion` | `intent_hint=text_completion` |
| `04_multi_turn` | Follow-ups on the same loop |
| `05_pool_service` | `ConnectionPool` + `TurnRunner` (`TurnBoundary` = DaemonSession turn end) |
| `06_jobs` | `AsyncCommandClient` job create/status/cancel |

```bash
cargo run --example 01_hello
# or
make test-examples
```

Env:

- `SOOTHE_WS_URL` — daemon URL
- `SOOTHE_EXAMPLE_AGENT=1` — full agent path (default: text_completion)
- `SOOTHE_EXAMPLE_TIMEOUT` — seconds (default 90)
