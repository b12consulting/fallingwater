# RAID log

Track open risks, assumptions, issues, and dependencies here. Close or revise an item when a milestone resolves it; keep the reason in the entry.

## Risks

- **R1 — Duplicate turn execution (open).** A worker can finish work and fail before acknowledging its queue entry. Use stable run IDs and idempotent completion writes in milestone 2.
- **R2 — Conversation changes across upgrades (open).** An agent spec may change while a conversation is idle. Record the spec identity and version used for each run; decide when an existing conversation adopts a newer version.
- **R3 — Event volume (open).** Persisting every output delta may create more Redis writes than useful history. Measure this in milestone 2 and batch display deltas if needed.

## Assumptions

- **A1 — One active turn per conversation (to validate).** Serialize turns for a conversation even when multiple workers are available.
- **A2 — Redis is the first transport (to validate).** Keep application interfaces independent of Redis commands where practical, without making another backend part of the first release.

## Issues

- **I1 — Conversation ownership (open).** Choose how workers prevent two turns for one conversation from running at once; resolve in milestone 2.
- **I2 — Agent catalog (open).** Decide how file-based specs and Python-registered tools or capabilities are named, listed, and combined; resolve in milestone 1.
- **I3 — Event contract (open).** Define durable conversation events, display events, and cursor behavior before the SSE endpoint in milestone 3.

## Dependencies

- **D1 — Pydantic AI (available).** Agent specs, capabilities, message history, and event streaming provide the agent primitives.
- **D2 — Redis (available via Compose).** `compose.yaml` supplies a development instance; integration tests will require it when Redis behavior is implemented.
- **D3 — Model provider (later).** Real chats need provider credentials. Tests should use Pydantic AI test models wherever possible.
