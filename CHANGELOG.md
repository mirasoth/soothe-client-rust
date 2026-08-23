# Changelog

## Unreleased

## 0.3.8 — 2026-08-23

### Added
- `loop_execution_state_fetch` request-response method in `client.rs` (previously only `send_loop_execution_state_fetch` fire-and-forget existed)
- `fetch_execution_state` helper in `helpers.rs`, exported from `lib.rs`
- `loop_execution_state_fetch` assertion in the `integration_loop_tree_cards_history_messages_state` integration test

## 0.3.7 — 2026-08-07

### Changed
- Fix version bump for dependency sync

## 0.3.6 — 2026-07-31

### Changed
- Display card wire types renamed to `soothe.card.*` (`created` / `updated` / `finalized` / `replay.begin` / `replay.end`)

### Added
- `card_projection::{CardProjection, parse_card_custom_payload}`
- `EVENT_CARD_UPDATED` / `EVENT_CARD_FINALIZED`; card frames count as turn progress

## 0.3.2 — 2026-07-26

### Fixed
- **`TurnRunner` idle clock:** arm only after the turn is accepted (or first non-stale
  event), so pre-accept LLM latency is not counted as silence
- **Idle postponement:** heartbeats / empty catalog events no longer reset the idle
  clock (StrangeLoop planner churn could hold `QueryGate` indefinitely)
- **`SoftComplete`:** always completes (including empty content) so idle/query timeouts
  release the gate; callers map `completion_event` → chat.done codes

## 0.3.1 — 2026-07-26

### Changed
- **`TurnRunner`:** takes `Arc<QueryGate>` so the gate can be shared with the caller for
  pre-acquire + [`execute_reserved`] (Go `soothe-client-go` / `ExecuteReserved` parity)
- **`execute_reserved`:** always `release`s the gate on exit (including `validate_opts` failure)

### Added
- `TurnRunner::gate()` accessor for the shared `Arc<QueryGate>`

## 0.3.0 — 2026-07-25

### Added
- **Appkit parity with Go 0.4.x:** `SseBroadcaster`, image `attachments` (`image` feature),
  `chunk_filter` / `EarlyDropFn`, deep `EventClassifier` (`Classify` / `ClassifyTurn`),
  thinking-step extraction, `TurnEventStats`, `AppKey`, full `QueryGate` cancel-before-context
- **TurnRunner:** pre-send settle-drain, arm-before-end (Go 0.4.6–0.4.8), SSE hooks,
  `ExecuteReserved`, attachment compaction knobs, idle floor with attachments
- **ConnectionPool:** dedicated `receive_messages` reader, `reader_live`, rebuild on dead stream,
  optional client factory / bootstrap hooks
- **DaemonSession:** `rpc_client()`, early-drop filtering, turn event stats, cancel-seen tracking
- **Transport:** graded `inbound_frame_drop_priority`, `is_handshake_complete` / `readiness_state`,
  fire-and-forget `send_job_*` methods

### Changed
- Crate version `0.3.0`; `EventClassifier::with_defaults()` is the preferred constructor
- `input_message_for_loop` returns a protocol-1 notification envelope (params nested)

## 0.2.4 — 2026-07-19

### Fixed
- `cargo fmt` on TurnBoundary unit tests (release CI)

## 0.2.3 — 2026-07-19

### Added
- Unit tests for `TurnBoundary` (gates, stopped, public surface); example/docs note on pool turn end

## 0.2.2 — 2026-07-19

### Changed
- `TurnRunner` ends turns via `TurnBoundary` (DaemonSession gated `stream.end` / idle / stopped; Go v0.4.4 parity)

### Added
- `appkit::TurnBoundary`, `TurnLifecycleGate`, `is_daemon_turn_end_event`

## 0.2.1 — 2026-07-19

### Removed
- Legacy `intent_hint` values `direct_llm`, `quiz`, and `direct_model` (rejected by validation)
- Legacy loop phase `direct_model` from `DEFAULT_DELIVERABLE_PHASES`

## 0.2.0 — 2026-07-18

### Added
- `Client::reconnect`, auth (`authenticate` / `refresh_auth_token`), stream-degraded callback
- Heartbeat tracking (`HeartbeatTracker` / `DaemonHealth`)
- Loop admin RPCs: `loop_tree`, `loop_prune`, `loop_delete`, `loop_detach`, `loop_state_update`
- Long-lived `Client` job / autopilot / cron / `memory_stats` helpers + `autopilot_subscribe`
- Full event catalog parity with Go/TS + `parse_namespace` / `classify_event_verbosity`
- Verbosity helpers (`VerbosityTier`, `should_show`)
- Turn-boundary helpers (`format_turn_id`, `frame_turn_id`, `frame_seq`)
- Sync `CommandClient` mirrors the full `AsyncCommandClient` surface

## 0.1.1 — 2026-07-18

- Add client-facing subagent event constants (`EVENT_EXPLORER_*`, `EVENT_DEEP_RESEARCH_*`)
- Document and unit-test `preferred_subagent` on `SendInputOptions` / `InputOpts` / `SendTurnOptions`
  (field was already wired; examples/README previously omitted it)

## 0.1.0 — 2026-07-17

- Initial release: protocol-1 `Client`, `CommandClient` / `AsyncCommandClient`,
  `appkit::DaemonSession` (dual-socket), `ConnectionPool` + `TurnRunner`,
  helpers, examples 01–06, unit + live integration tests.
