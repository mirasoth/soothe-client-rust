# Changelog

## 0.1.1 — 2026-07-18

- Add client-facing subagent event constants (`EVENT_EXPLORER_*`, `EVENT_DEEP_RESEARCH_*`)
- Document and unit-test `preferred_subagent` on `SendInputOptions` / `InputOpts` / `SendTurnOptions`
  (field was already wired; examples/README previously omitted it)

## 0.1.0 — 2026-07-17

- Initial release: protocol-1 `Client`, `CommandClient` / `AsyncCommandClient`,
  `appkit::DaemonSession` (dual-socket), `ConnectionPool` + `TurnRunner`,
  helpers, examples 01–06, unit + live integration tests.
