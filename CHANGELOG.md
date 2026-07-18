# Changelog

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
