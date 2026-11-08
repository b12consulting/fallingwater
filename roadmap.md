# Roadmap

This roadmap tracks high-level features and decisions visible to people using
Fallingwater, including developers integrating the library. Each milestone
states what is available, what remains, and what completion means. Detailed
technical implementation choices and risks belong in `raid.md`.

## Development principle

The library architecture is still evolving. Prioritize concrete workflows and
keep public behavior minimal. Defer advanced behavioral guarantees, such as
submission deduplication, retry semantics, strict ordering, and conversation
lifecycle rules, until a demonstrated use case requires them. Record deferred
choices and their reasons in `raid.md` without implementing them speculatively.
The observed worker races in milestone 5 are concrete correctness issues,
not speculative advanced guarantees.

## 0. Repository setup — complete

### Available

The library installs locally, `fw --help` works, Redis can be started with
Docker Compose, and the test suite runs. The README and project planning
documents guide local development.


## 1. Agent configuration and discovery — partial

### Available

An application can supply an `Agent`, an `AgentSpec`, or a zero-argument factory
returning either by Python import path. Imported agents can bring attached
tools and capabilities. `Chat` validates the selected agent and available
model; the CLI offers Pydantic AI's plain `Agent` as a default and accepts
dependency values through `--param`.

### Remaining

Fallingwater has no file-based agent catalog or discovery commands for agents,
tools, and capabilities. Define application-level composition and discovery
beyond importing an already configured Pydantic AI agent. Commands such as
`fw ls agent` and `fw ls capability` remain proposals; current `fw ls` lists
conversation IDs.

### Decision

Define whether an existing conversation keeps its original agent definition or
adopts an updated one after an upgrade. Resumption currently uses the saved
import path and model, without versioning the agent definition. Decide how to
continue a conversation when its saved model becomes unavailable.

### Done when

An application can configure and compose an agent, inspect what is available,
and predict which definition a resumed conversation will use.


## 2. Durable CLI conversations — partial

### Available

The CLI can start and resume a chat by ID, including answers or failures that
arrived while it was closed. `fw chat --msg` submits one prompt, prints its
result, and exits. Its conversation reader group defaults to `user` and can be
selected with `--group`. Prompts and completed answers pass through Redis, and a
worker can finish its active action before shutdown, leaving subsequent actions
for another worker. A later turn uses completed history captured when that turn
was queued.

`fw ls` lists conversations, `fw tail` shows recent conversation events, and
`fw dispatch` shows recent worker actions. `fw archive` exports a conversation's
events to a compressed local file; `fw restore` restores those events to an
absent conversation stream. Archives do not restore dispatch work.

Attached synchronous and asynchronous function tools execute through independent
per-call actions. A batch waits for all results before the agent resumes. Tools
returning `SubAgentRequest` queue child conversations and receive child answers
as tool results without occupying a worker while the child runs. Ordinary tool
exceptions and child failures fail the parent turn.

Workers reclaim eligible pending actions and skip work with saved results or
terminal outcomes. This is not complete crash recovery or exactly-once execution.

### Remaining

Add incremental answer streaming and live tool or delegate progress to the
terminal, beyond final answers and event inspection. Decide how long
conversations and dispatch entries remain available.

Repair missing dispatch actions and partial child setup after failures between
separate writes. `recover()` is currently a placeholder; pending-action
reclamation alone does not repair every saved event missing its next action.
Extend real-Redis coverage to worker restart and recovery; current isolated
Redis tests cover concurrent result saves alongside in-memory and mocked tests.

### Decision

Senders that need sequential conversation history wait for a turn to finish
before submitting the next prompt. Decide what ends a conversation, whether
ending can happen automatically, and what later submissions do.

### Done when

A client can disconnect and return to the same conversation, worker crashes
and restarts do not lose accepted turns, and serialized prompts produce a
coherent sequence of answers. Automated tests demonstrate these behaviors.


## 3. Web integration and monitoring — partial

### Available

`fw web` serves an optional FastAPI router with read-only SSE feeds for one
conversation or the ten most recently active conversations. The
per-conversation feed replays its full retained stream; the recent feed replays
up to ten events per selected conversation. Recent selection follows dispatched
conversation pokes, not all conversation activity. Both feeds follow new events.
A simple monitoring page displays the recent feed, including tool and subagent
events, with optional completed history and usage.

The read-only `/fw-api/dashboard/` presents recently poked conversations as cards
with green non-failed and red failed event slots. Selecting a slot reveals its full
details. htmx 4 refreshes the latest ten events of the latest ten conversations
every two seconds; Bootstrap 6 alpha provides the layout, event dialog, and
button-triggered calendar.
The existing raw monitor and SSE feeds remain available.
The overview supports an exclusive Redis-ID cutoff via shareable
`?before=<redis-id>` URLs. Date selection starts before midnight UTC; **< Prev**
continues before the oldest selected dispatch poke without skipping entries
that share a millisecond. Polling retains the cutoff and **Live** clears it.
Older views may repeat conversations with earlier pokes; event timelines use
the cutoff's millisecond because Redis sequences are stream-local.

Conversation names link to a full-history dashboard with rows of ten event
slots and inline event details beside the card. It follows the retained stream
without restricting selection to the ten recent conversations.

### Remaining

Add health and statistics endpoints, and a documented way for clients to
reconnect from their last event instead of replaying the full stream.

### Done when

A browser can observe an active conversation and reconnect without starting or
consuming its work, resuming from a supported cursor instead of replaying events
already received.


## 4. Internals — partial

### Available

Synchronous `DispatchStream` and `ConversationStream` classes provide shared
stream operations, including consumer-group reads and acknowledgements.
`Chat` and `Worker` use these classes. Related conversation and dispatch
writes are separate Redis commands.

Pure `Conversation` logic coordinates turn-state decisions and reconstructs
all turns from persisted events. Turn state machines validate transitions and
track pending tool calls and delegations. `Chat` and `Worker` coordinate I/O;
stream classes do not construct workflow payloads or call domain machines.
Replay keeps the first result per call and first terminal outcome; queue-time
history snapshots share immutable message prefixes.

### Remaining

Move the web SSE routes and CLI event inspection onto the stream classes. They
still call `XREAD` or `XREVRANGE` directly; the recent-conversations feed needs
stream methods that preserve its reverse-tail replay and multi-stream follow
behavior.

Add consumer-group lifecycle management, including deleting worker
consumers with no pending entries when the `WorkerPool` starts.

### Done when

Chat, worker, web, and CLI inspection code use the stream classes for Redis
stream operations. Worker pool startup removes consumers with no pending entries
and preserves consumers that still have pending work.


## 5. Worker concurrency correctness — partial

### Available

Concurrent tool-result saves publish one resume for a completed request batch.
A cursor-checked append rejects stale writes; workers replay newer events and
revalidate before retrying. Resumption is decided through the successfully
appended result's event ID, without reading later writes. Duplicate saves for
the same call are rejected after replaying the winning result. This shared
result-saving path also handles successful child-agent outcomes.

Isolated real-Redis tests reproduce simultaneous completion of distinct calls
and duplicate saves, verifying one resume publication. Result persistence and
dispatch publication remain separate writes; this does not close the crash
recovery gap or provide exactly-once external effects.

### Issues

- **Premature reclamation of active dispatch actions.** `XAUTOCLAIM` can reassign
  an action while its original worker is still processing it. The idle threshold
  defaults to five minutes and is configurable with `--claim-idle-ms`; workers
  do not renew ownership or prevent concurrent execution after reclamation.
  Both workers can execute the same action and publish downstream work.

### Remaining

Implement automatic ownership renewal for active dispatch actions, preserving
recovery of genuinely abandoned work. Renewal must continue independently of
blocking model/tool calls, cover processing through acknowledgement, and never
reclaim an entry whose ownership has moved to another worker. Define handling
of renewal failures and ownership loss, including protection against stale
writes and acknowledgements.

Ownership renewal does not stop a stuck tool. Tool/network timeouts and bounded
retries remain a separate concern; a healthy renewal loop could otherwise keep
a hung call owned indefinitely.

Add real-Redis tests for long-running actions, renewal stopping, ownership loss,
and active-action reclamation. RAID R1 and R5 remain open; D88 records the
implemented result fan-in fix and D89 records the renewal decision.

### Done when

Concurrent completion of distinct tool and subagent calls publishes one resume
per batch. A long-running active action is not executed concurrently by another
worker, while an abandoned action remains recoverable. Tests reproduce the
fan-in race and active-action reclamation, including real-Redis multi-worker
coverage. This does not promise exactly-once external effects across crashes.
