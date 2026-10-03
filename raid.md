# RAID log

Track technical risks, assumptions, issues, and decisions that depend on how
Fallingwater is implemented. Record the status and reason for each choice,
including when to revisit it. Add a decision when a technical choice is
discussed or an implementation must be revisited. Close or revise an item when
the code resolves it.

## Risks

- **R1 — Duplicate turn execution (open).** A worker can publish a result and fail before acknowledging its work entry. Recovery could run the same turn again. Turn IDs already remain stable across both streams, but completion writes and agent side effects are not idempotent yet.
- **R2 — Conversation changes across upgrades (open).** An agent definition may change while a conversation is idle. The conversation stores its import path but no version, so later turns may run different behavior.
- **R3 — Event volume (open).** Persisting every output delta may create more Redis writes than useful history. Measure this in milestone 2 and batch display deltas if needed.
- **R4 — Claimed work can stall (open).** A worker crash leaves its entry pending in the `workers` group. The current proxy reads only new entries and never reclaims pending work.

## Assumptions

- **A1 — Serialized prompts when required (accepted).** A sender that needs sequential conversation history waits for one turn to finish before submitting the next. Independent senders may submit overlapping turns, which can read the same prior history.
- **A2 — Redis is the first transport (to validate).** Other backends are outside the first release; assess whether application interfaces can stay independent of Redis commands when they are expanded.

## Issues

- **I1 — Conversation ownership (closed).** Workers do not reserve a conversation; senders coordinate prompt submission when sequential history is required.
- **I2 — Agent catalog (open).** Import-path loading works, including `AgentSpec` objects, but file-based specs, capability composition, and `fw ls` do not. Decide how these are named, listed, and combined in milestone 1.
- **I3 — Event contract and retention (open).** Decide which progress events are durable, how long conversation history remains available, and how SSE readers resume from a cursor. The current feed replays from the beginning on every connection. Completion deltas must remain available for history replay; creation and queued events must remain while workers may need to load them from a poke or while a CLI reader group still has them pending.
- **I4 — Conversation ending (open).** Decide what ends a conversation, how its status is recorded, and what happens to its stream and future submissions.
- **I5 — Dispatch stream retention (open).** `XACK` clears pending status but leaves the entry in `fw:dispatch`; decide when acknowledged entries are deleted without losing recoverable work.
- **I6 — Redis integration coverage (open).** Current tests do not exercise restart, pending-work recovery, or concurrent workers against Redis. Add integration coverage as those behaviors are implemented.

## Decisions

- **D1 — Two stream roles (decided).** Use one shared `fw:dispatch` stream for worker dispatch and one event stream per conversation for history and observers. The dispatch name leaves room for other message types later; workers can discover work through one consumer group while readers follow a conversation independently.
- **D2 — Turn boundary (decided).** A work entry runs one complete Pydantic AI turn. Workers finish claimed turns before shutdown; a later worker reconstructs history for a later turn. Mid-turn execution state is not restored.
- **D3 — Completion history as deltas (decided).** Write `new_messages_json()` in `turn.completed` and replay completed turns in stream order. Treat older entries without `history_format` as full-history snapshots.
- **D4 — Conversation IDs (decided).** The CLI generates readable `qualifier_scientist` IDs for resumption; library-created conversations use UUIDs. Both map to the same per-conversation stream naming scheme.
- **D5 — Fixed agent and model (decided).** The creation event identifies the agent import path and model for a conversation. `Chat` uses those saved values on resume, ignoring supplied agent and model options so a conversation cannot change them.
- **D6 — Admission of overlapping turns (decided).** Fallingwater does not enforce one active turn per conversation. Senders serialize prompts when they need each turn to include the preceding result. The `workers` consumer group assigns dispatch entries individually, so overlapping turns can run on different workers.
- **D7 — Pending work and duplicate effects (open).** Choose how to reclaim abandoned group entries and detect a result already written for the same turn ID. Retrying an agent can repeat tool side effects, so completion deduplication alone is insufficient.
- **D8 — Dispatch stream cleanup (open).** Choose when to delete acknowledged dispatch entries without removing pending entries that still need recovery. `XACK` alone leaves the entry in `fw:dispatch`.
- **D9 — In-process conversation cache (open).** Keep reconstructed conversation history in Python process memory, together with the last conversation stream entry ID read. On the next access, read only entries after that ID and apply new completed-turn deltas. Rebuild from the stream after a cache miss or worker restart; decide how long cached conversations stay in memory.
- **D10 — Conversation ending (open).** Choose how an ended state is represented in the event stream, how submissions are rejected afterward, and when its data can be removed.
- **D11 — Agent definition versioning (open).** Decide what identity or version to store with a turn and how a resumed conversation selects an agent definition after an upgrade.
- **D12 — Progress event contract (open).** Choose which output, tool, and delegate events to persist, how readers use stream cursors, and how to limit write volume.
- **D13 — Transport boundary (open).** Decide which library interfaces should hide Redis commands while preserving stream semantics for a possible later backend.
- **D14 — Catalog composition (open).** Decide how file-based specs and application-provided tools and capabilities are registered, named, and combined before adding catalog listing commands.
- **D15 — Dispatch payload (decided).** A `conversation.poke` instruction carries only a conversation ID and turn ID. Workers load the matching `turn.queued` prompt, fixed agent and model, and prior history from the conversation stream. Keeping the turn ID identifies the queued event even when senders allow overlapping turns; the dispatch stream can carry other instruction types later.
- **D16 — Chat validation (decided).** `Chat` validates the selected agent and available model before creating or resuming a conversation. The CLI passes its agent and model arguments directly to `Chat`, which chooses saved settings for an existing conversation. CLI-generated names are checked for existing streams before use.
- **D17 — Web event feed (decided).** Export a module-level FastAPI router and mount it under `/fw-api` in the small `fw web` app. The hosting app supplies the Redis client and namespace through app state. Use asynchronous `XREAD` on each conversation stream for non-consuming SSE readers; publish each stream entry ID as the SSE ID and its stored type as the SSE event name. Connections currently start at the beginning; cursor-based reconnection remains open in I3.
- **D18 — CLI reader recovery (decided).** Use a consumer group on each conversation stream for one logical terminal reader, defaulting to `user` and configurable with `--group`. Reuse the stable `terminal` consumer name to read pending entries after restart. Keep a queued entry pending until its result is printed, then acknowledge the queued and terminal entries in one `XACK`; other event types are acknowledged as read. Independent readers use separate groups, while the web feed remains on `XREAD`.
