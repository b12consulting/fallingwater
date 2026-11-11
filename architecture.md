# Architecture

## AD1 — Redis key namespace

Every Redis key owned by Fallingwater, including stream keys, uses a configurable
namespace prefix followed by `:`. The default namespace is `fw`; Redis-backed
tests use `fw-test`. For example, a conversation stream might be named
`fw:conversation:abc:events` in normal use and
`fw-test:conversation:abc:events` in a test.

Namespaces contain lowercase letters, digits, and hyphens. The
`fmt_dispatch_stream` and `fmt_conversation_stream` helpers validate and format
these names from the configured namespace and conversation ID, so an application
and its tests can share a Redis instance without mixing their data.

## AD2 — Library first

Fallingwater's core is an importable Python library. Applications can create a
`Chat` with a Redis client, submit turns, and run `WorkerPool` in their own process.
The bundled `fw` CLI calls these same library classes; its chat input and
responses pass through Redis.

## AD3 — Pydantic AI is the agent layer

`reify_agent` loads a Python import path resolving to a Pydantic AI `Agent`, an
`AgentSpec`, or a zero-argument callable returning either. It converts specs
with `Agent.from_spec`; import-path resolution is cached, while callable results
are created for each turn. Agents can bring their own tools and capabilities.

`agent_proxy.run` invokes `Agent.run_sync` once and returns either final output or
deferred tool requests. `Worker.run` loads each dispatch action and handles
checkpoint failures; `Worker.process` handles the loaded action: an agent
run, one tool call, or a child result. The full request set and per-call jobs
are written as separate stream commands; each result is saved by call ID, and
the last unique result queues one agent resume. A restarted worker reconstructs
the turn from the conversation stream and recovers pending actions through
`XAUTOCLAIM`.
`agent_proxy.execute_tool` resolves attached functions and reconstructs a
basic `RunContext` for each deferred call. The next turn rebuilds its history
from completed turns.
Fallingwater coordinates the turns; the imported agent defines their behavior.
Pydantic AI generates its own run ID for each invocation. Fallingwater uses the
stream's `turn_id` for application-level correlation and does not reuse it as
Pydantic AI's run ID.

## AD4 — Redis-backed conversation turns

Redis Streams connect message submitters, workers, and readers. The shared
`fw:dispatch` stream distributes turns through the `workers` consumer group. A
per-conversation stream records the creation event, queued prompts, outcomes,
and history needed for the next turn. `Chat.receive` reads that stream through
`ConversationStream` with `XREAD`; the CLI uses a separate consumer group on
the same stream.

`DispatchStream` owns the shared dispatch key, consumer-group reads, claims,
and acknowledgements. It decodes each dispatch entry into a typed action or an
invalid-entry value that the worker acknowledges. It creates a
`ConversationStream` bound to an ID; that object owns typed event reads and
writes and its reader groups. `Chat` keeps one bound
conversation stream. `Worker` keeps one dispatch stream and creates a bound
conversation stream for each action, including child conversations.
`models.py` defines the event and action schemas; `stream_utils.py`
handles naming validation and wire codecs. `Chat` and `Worker` decide when to
acknowledge entries; `Conversation` and its turn machines decide which work
is enabled.

`Chat.send` prepares a queued event through `Conversation`, then writes the
event and dispatch poke through the two stream objects with successive `XADD`
commands. Group and consumer names remain explicit on stream operations.

| Stream                                      | `type`              | Other fields                 |
|---------------------------------------------|---------------------|------------------------------|
| Conversation, `fw:conversation:<id>:events` | conversation event  | `payload`                    |
| Dispatch, `fw:dispatch`                     | action type         | conversation, turn, step, call, or child IDs |

Each conversation event payload has a Pydantic model selected by its `type`.
The stream entry stores only the text event type and a Zstandard-compressed
JSON payload. `encode_conversation_event` serializes and compresses payloads on
writes; `decode_conversation_event` decompresses and validates them on
reads.
The dispatch stream stays text based because its action entries only carry
routing identifiers. Stream reads attach Redis entry IDs to decoded dispatch
actions and conversation events as metadata; those IDs are excluded from the
Pydantic payload serialization. `Chat` uses event IDs for cursors and
acknowledgements, while `Worker` uses action IDs for acknowledgements.
All conversation reads, CLI inspection, web feeds, and archives use one
`ConversationEvent` model with a required `entry_id`. The decoder attaches the
Redis ID to that model, rather than returning a separate tuple. Event envelope
serialization includes the ID; Redis payload serialization does not.

Submitting a prompt writes `turn.queued` to the conversation stream and a
`conversation.poke` instruction to dispatch in a following write. The
entries have different Redis stream IDs but share a turn ID. A worker uses the
conversation ID and turn ID to load the queued prompt, fixed agent and model,
and prior history from the conversation stream. It writes `turn.tool.requested`
after a run returns deferred calls and `turn.tool.result` after each tool call
completes. It writes `turn.completed` or `turn.failed` when the turn ends. Each
dispatch action is acknowledged after its own event checkpoint; a logical turn
can span several independently recovered actions.

Each tool-request event stores the message delta and `RunUsage` from that
`Agent.run_sync` call, along with JSON-compatible deferred requests. The same
sequence of writes adds one dispatch entry per tool call. An empty deferred
request set is rejected before the event is written. Tool-result events are
associated with that request and store one JSON-compatible result per call.
The result handler compares unique call IDs and queues one resume when the
whole request set has results. Completed events store the accumulated new
messages, display response, and `RunUsage`; their `turn_messages` field remains
the canonical delta for later turns. Deferred request payloads use
`step_messages` for the new messages from one model step. Neither field contains
the full conversation history.

`ConversationStream.iter_events()` pages through the event stream and supplies
decoded events to `conversation.reconstruct_conversation`. That pure function
returns a `Conversation` containing the configuration, conversation ID, replay
cursor, and `turn_state: dict[str, TurnStateMachine]`. Each machine owns its
prompt, prior-history snapshot, accumulated turn messages, usage, active tool
request, per-call results, and child delegations. All conversations capture
history and usage when the queued event is replayed, including persistent
agent sessions. Later outcomes do not change an already queued turn's snapshot.
Snapshots share immutable `MessageHistory` prefixes. Replay appends one completed
message chunk at a time rather than retaining a full history copy for every
turn; the worker materializes only its selected turn's history for execution.

Senders wait for a turn to finish before submitting the next prompt when they
need sequential history. Workers can process overlapping turns from the same
conversation, and each may load the same prior history. Persistent agent
conversations follow the same policy.

## AD5 — Conversation identity and configuration

`Chat` generates a UUID when called without a conversation ID. The CLI instead
generates a readable `qualifier_scientist` ID for each new chat and accepts that
ID as the positional argument to resume it. The first conversation event stores
the agent import path and model. `Chat` validates the effective agent and model
before creating a conversation or resuming one. On resume it uses the saved
values, ignoring supplied agent and model options. The CLI passes its arguments
to `Chat` in both cases and defaults to `pydantic_ai:Agent` when `FW_AGENT` is
unset; a new chat needs a model on its agent, in `FW_MODEL`, or through `-m`.

`Chat.send(prompt)` generates a fresh UUID turn ID on every call and returns
that ID with the queued event cursor. `Chat.ask(prompt)` submits through `send`
and waits for the result through `receive`. Repeated prompts create separate
turns; client submission deduplication is deferred until its behavior is needed.

## AD6 — Worker pool lifecycle

`WorkerPool` starts one or more workers and handles process stop signals. Each
worker creates the `workers` consumer group with `MKSTREAM`, then uses
`XAUTOCLAIM` to recover entries idle for the configured period before reading
new entries with `XREADGROUP`, through `DispatchStream`. The default idle
period is five minutes and `fw worker --claim-idle-ms` changes it. Dispatch
entries identify one initial/resume run, one tool call, or one child result.
Each action checkpoints its event before acknowledgement, so independent calls
can run on different workers and out of order. On SIGINT
or SIGTERM, an active model or tool call finishes and is checkpointed; the
worker then leaves unfinished work pending for another worker. A restarted
worker has a new consumer ID and recovers eligible dispatch entries with
`XAUTOCLAIM`. Acknowledged entries remain in the dispatch stream. Malformed or
stale actions are logged and acknowledged. `Worker.run` records non-Redis
processing errors as `turn.failed`; Redis failures leave actions pending.

## AD7 — Web conversation feed

The module-level `router` exposes a read-only SSE route at
`/conversations/{conversation_id}/events`. The small `create_app` wrapper mounts
it under `/fw-api`, and `fw web` serves it with Uvicorn. The route gets its
synchronous Redis client and namespace from the hosting app's state. The
required Redis client is passed to `create_app`; the app does not create or
close it. `fw web` owns the client and keeps it open until Uvicorn returns. The
SSE routes return synchronous iterators; FastAPI's `EventSourceResponse`
advances them in a worker thread so blocking Redis reads do not block the event
loop.

The route returns 404 for an unknown conversation. For an existing one, it reads
the conversation stream from the beginning with `XREAD`, then follows new
entries. Each SSE message carries the Redis stream entry ID, the stored event
type, and its fields. The route currently replays from the beginning on every
connection; it does not use `Last-Event-ID` to resume from a cursor.

The `/conversations/recent/events` route scans dispatch entries backward for
the ten most recently poked distinct conversations. It fetches up to ten
retained events from each selected stream with `XREVRANGE`, sends them in stream
order, then follows the streams alongside `fw:dispatch` in one `XREAD` call.
A new poke moves its conversation to the recent set and evicts the least
recently poked one when the set is full; a newly selected stream gets the same
ten-event replay. Each SSE event includes its conversation ID and uses
`conversation_id:stream_entry_id` as its SSE ID. The feed is a monitoring
heuristic: a conversation appears after its first prompt, and events from
different streams have no strict global order.

The optional web package keeps the module-level router in `web/apirouter.py`
and its packaged Jinja2 template in `web/templates`. The `/monitor` route
renders a small read-only page whose `EventSource` subscribes to the recent
feed. The monitor uses payload `fmt()` output, omitting completed-turn history
and usage by default; `/monitor?full=1` includes them. It listens for each
current conversation event type and keeps at most 200 rendered entries in the
browser. The per-conversation SSE route always sends complete payloads.
Idle feed polls emit an SSE comment every second so the synchronous reader
returns control regularly. `fw web` gives active connections three seconds to
close on shutdown, then cancels remaining streaming tasks. Its Redis client is
closed after Uvicorn returns, so cancelled stream reads cannot race client
cleanup.

The `/dashboard/` router route presents the same ten most recently poked
conversations as responsive cards, each with its latest ten events ordered
oldest to newest. The standalone app exposes it only at `/fw-api/dashboard/`,
without root-level dashboard routes or redirects. htmx 4 polls
`/fw-api/dashboard-cards` every two seconds
and uses built-in `innerMorph` swaps to retain focus and stable event controls.
Each slot is green unless its event is `turn.failed`, in which case it is red.
User and assistant message slots show Bootstrap Icons `person` and `robot`,
respectively, without changing their accessible labels or event actions.
Selecting a slot fetches `/fw-api/dashboard-event` by conversation and exact
event ID into a Bootstrap 6 dialog outside the refreshed card region, preserving open
details during updates. Detail payloads are complete; text and JSON are escaped.
ISO 8601 UTC datetimes come from Redis entry IDs. The page is server-rendered initially and
reports refresh, detail, or asset-loading failures explicitly. Pinned,
integrity-checked CDN assets avoid a frontend build; network access is required
for styling and interactions. Existing monitor and SSE behavior is unchanged.

`web/dashboard.py` owns I/O-free card and event presentation, with typed
template dictionaries for slots, cards, overview, history, and details.
The router validates requests and coordinates reads; its immutable
`DashboardCutoff` parses the ID once and supplies separate dispatch and event
bounds. Conversation range, reverse-tail, and single-conversation SSE reads
use `ConversationStream`, which owns decoding. Existing forward-range defaults
remain unchanged; explicit bounds and `count=None` support exact events,
historical settings, and unlimited retained history. Dispatch discovery and
the recent feed's multiplexed `XREAD` remain web coordination.

The overview and card fragment accept an optional `before=<redis-id>` cutoff.
Dispatch discovery uses `XREVRANGE` with an exclusive upper bound, scanning
batches of 100 until ten distinct conversations are selected or history ends.
Each selected conversation retains its latest eligible poke ID; **< Prev**
uses the oldest of those IDs as the next exclusive cutoff, avoiding skipped
pokes within a millisecond. Conversations can recur in older views through
earlier pokes. Redis sequences are stream-local: event tails and initial
settings use the cutoff's millisecond, including all event sequences when the
dispatch sequence is nonzero and excluding the whole boundary millisecond for
`-0`. This is retained-history navigation, not a cross-stream atomic snapshot.

A real button-triggered Bootstrap 6 alpha datepicker, using its bundled
Vanilla Calendar Pro dependency, displays dates as `yyyy/mm/dd`. Selecting a
date supplies `<UTC-midnight-milliseconds>-0`. Date selection, Prev, and Live
replace the shareable URL; reloading restores the filter. Polling includes
the unchanged hidden cutoff and updates the next Prev cursor from the returned
fragment. Clearing it restores unbounded live reads. Bootstrap 6's prerelease
stability is a tradeoff of using its datepicker without another UI library.
Monitor/SSE feeds and full-history detail pages remain unfiltered. Historical
results depend on retained streams and dispatch pokes, not on an archive.

Conversation names link to `/fw-api/dashboard/{conversation_id}`. This page
reads the entire retained stream, including conversations outside the recent set, and
reuses the card template with rows of at most ten slots. The card keeps the
overview's responsive column width and grows vertically; event details occupy
the adjacent column, stacking below the card on small screens. htmx polls
`/fw-api/dashboard-history` by conversation ID, morphing only the history card so
selected details stay visible. Full-history refresh cost grows with the retained
stream length, unlike the bounded overview. Fragment routes live outside
`/dashboard/` so conversation IDs such as `cards` and `event` are not reserved.

## AD8 — CLI reader groups

`Chat` creates or reuses a consumer group on the conversation stream during
initialization, starting at ID `0`. The default group is `user`;
`fw chat --group` selects another. Terminal reads use the stable consumer name
`terminal` so a later CLI process can read entries left pending by an
interrupted session. Each `Chat` starts with a reader cursor at `0`, advances
through pending entries, and switches to `>` when the pending read is empty.
Later reads on that instance stay on `>`.

The CLI acknowledges `conversation.created`, `turn.queued`, and unrecognized
events as it reads them. It prints each `turn.completed` or `turn.failed` event
before acknowledging it, so a terminal exit between printing and acknowledgement
can cause the result to be printed again. A single `Chat.read` method reads
pending entries for the stable consumer, then new entries. With `ffwd=True`, it
handles all available events without blocking; normal reads wait up to one
second for new events and return after a batch. On startup it handles
events that are available and returns without waiting for queued turns whose
results have not arrived; a new prompt can therefore overlap such a turn. Each
group represents one logical terminal reader. In one-shot mode, `--msg` prints
earlier results read during startup catch-up and the new turn's result to stdout.
Results that arrive after the new prompt is queued use the same output.
The library's `Chat.receive` and the web feed still use independent `XREAD` calls.

## AD9 — Compressed conversation payloads

Conversation stream entries contain exactly `type` and `payload`. The payload
is JSON serialized from a Pydantic event payload model and compressed with
Zstandard through the shared `compress` and `decompress` helpers in
`utils.py`. Redis clients use binary responses so compressed bytes remain
intact; `ConversationStream` uses the event codec functions in `stream_utils.py` for
compression, decompression, and model validation. Dispatch entries remain text
fields because they carry only a conversation ID and turn ID. `PAYLOAD_MODELS`
is the public mapping from event types to their Pydantic payload models.

The web event feeds decode conversation payloads before sending SSE data. A
completed payload carries the turn's new message history and `RunUsage`; the
worker restores the latest usage and passes it to `Agent.run_sync` on the next
turn. This preserves usage across sequential turns. Deferred steps restore their
latest checkpointed usage when resuming. Existing stream data is reset when
adopting the format; legacy event decoding is not supported.

## AD10 — Event payload schemas

Conversation stream entries contain a `type` and a compressed Pydantic JSON
`payload`. `PAYLOAD_MODELS` maps each supported conversation event type to its
payload model:

- `conversation.created`
  - Fields: `agent: str`, `model: str`, `deps: dict[str, Any]`,
    `parent: ParentToolReference | null`
  - Agent import path, selected model, initial dependencies, and optional
    parent tool correlation for child conversations. `model` defaults to an
    empty string; `deps` defaults to an empty object.
- `turn.queued`
  - Fields: `turn_id: str`, `message: str`,
    `parent: ParentToolReference | null`
  - Turn ID, user prompt, and optional per-turn delegation routing. Missing
    parent references fall back to the creation-level reference for old events.
- `turn.tool.requested`
  - Fields: `turn_id: str`, `step_id: str`, `requests: dict[str, Any]`,
    `step_messages: list[dict[str, Any]]`, `usage: RunUsage`
  - Deferred requests from one model call, its message delta, and usage.
- `turn.tool.result`
  - Fields: `turn_id: str`, `step_id: str`, `tool_call_id: str`, `result: Any`
  - One JSON-compatible tool result associated with a deferred request.
- `turn.agent.requested`
  - Fields: `turn_id`, `step_id`, `tool_call_id`, `child_conversation_id`,
    `child_turn_id`, `agent_path`, `model`, `prompt`, `deps`, `name`,
    `persistent`
  - Correlation and complete resolved specification for an agent turn.
    `name` defaults to null and `persistent` to false; persistent delegations
    require a name and establish or reuse a parent-scoped registration.
- `turn.completed`
  - Fields: `turn_id: str`, `message: str`, `turn_messages: list[dict[str, Any]]`,
    `usage: RunUsage`
  - Turn ID, displayed response, structured JSON data for the new Pydantic AI
    messages, and accumulated usage.
- `turn.failed`
  - Fields: `turn_id: str`, `error: str`, `traceback: str | null`
  - Turn ID, failure description, and formatted Python traceback (including
    chained exceptions). Workers persist the traceback as well as logging it;
    child failures preserve it when propagated to the parent. The field
    defaults to null for older events or failures without a captured exception.

The dispatch stream is text based and does not use the conversation payload
envelope. `Event` contains only conversation event types; `Action`
contains dispatch action types, including `conversation.poke`.
`DispatchStream` writes an action type and a typed dispatch payload through
`encode_dispatch_event`, retaining the existing flat text fields on Redis.
`decode_dispatch_event` rebuilds and validates
`DispatchEvent(entry_id, type, payload)`. Its `payload` is one of
`DispatchPayloadType`: `ConversationPokePayload`, `AgentResumePayload`,
`ToolCallPayload`, or `SubAgentResultPayload`. Payloads carry routing identifiers
but no action type or Redis ID, so callers can create them before publication.
The envelope validates that its type matches its payload. Workers route the
payload and acknowledge the envelope's ID. Invalid entries return
`InvalidDispatchEvent`, retaining their Redis IDs and raw fields so the worker
can log and acknowledge them.
Both `DispatchEvent` and `ConversationEvent` validate their type/payload pairing
on construction and deserialization, including archive reads. Codecs do not
repeat that check: encoders serialize the supplied type and payload, while
decoders delegate envelope validation to the models. Wire-envelope and
compression/JSON errors remain codec responsibilities.
Every dispatch entry must include an explicit action type; a missing type is
invalid and follows the worker's malformed-action acknowledgement policy.
`conversation.poke` starts a turn; `turn.tool.call` identifies one
deferred call; `turn.agent.resume` resumes a request step after fan-in; and
`turn.subagent.result` routes a child terminal result to its parent. Entries
carry the conversation and turn IDs plus the relevant step, call, or child IDs.

## AD11 — CLI stream inspection

`fw ls` scans the selected namespace for conversation event streams and prints
their IDs as Redis returns them. `fw tail <conversation_id>` decodes the latest
ten conversation events by default; `-n` changes the count. `fw dispatch` decodes
the latest ten dispatch entries and accepts the same count option. Both event
commands print each Redis entry ID, event type, and indented JSON payload in
stream order. The tail output omits completed-turn history and usage by default;
`--full` includes both. Compressed conversation payloads are decoded through
the shared conversation event decoder.

## AD12 — Deferred attached-tool execution

`agent_proxy.run` calls `Agent.run_sync` once. A Pydantic AI hook defers attached
function tools and returns their requests to `Worker`. The worker stores each
request and dispatches each call independently. Results are stored by call ID;
the last unique result schedules the next agent run with `DeferredToolResults`.
The reconstructed conversation records its last replayed event ID. Result
fan-in replays newer events and uses a cursor-checked append, retrying after
conflicts. Resumption is decided through the successfully appended result's
event ID, without incorporating later writes.
Synchronous and asynchronous function tools are supported; deferred requests
and results must be JSON serializable. `agent_proxy.execute_tool` resolves the
attached callable, decodes arguments, and rebuilds basic `RunContext` fields
from the saved conversation and request. Approval requests and toolsets without a directly
attached function callable are not supported.

## AD13 — Checkpointed agent steps

Each dispatch entry represents one action and is acknowledged after its
checkpoint. A model request is checkpointed before its per-call tool jobs are
enqueued; each tool result is checkpointed before the resume job is scheduled.
On restart, the worker reconstructs the turn from these events and skips saved
tool results. A crash after a tool side effect but before its result event can
still repeat that tool call. A stop request takes effect after the current
model or tool call is saved, leaving unfinished actions for `XAUTOCLAIM`
recovery. Related event and dispatch writes are separate Redis commands. A
failure between them can leave saved state without its next dispatch action;
replaying the original action does not repair every such partial write.

Nested agent work uses its own conversation stream. A tool may return the
public `AgentRequest` model with a prompt, an agent import path for creation,
optional model/JSON dependencies/name, and explicit persistence. A name-only
persistent request continues a registered agent. The worker first persists
parent `turn.agent.requested` with the complete resolved specification,
then creates settings if absent and queues the agent turn. It publishes
`conversation.poke` when that turn is ready. A recorded request can finish
interrupted initialization when its tool action is replayed. The worker does
not wait for the agent. Child completion
or failure queues a parent-result action carrying the child's terminal event
ID, which lets the parent read that event directly. Child IDs are deterministic for the
parent tool call; persistent names reuse conversation IDs but receive distinct
turn IDs. A child failure fails its
parent, and late sibling results are ignored after a terminal event. No worker
capacity is reserved while child work runs.

## AD14 — Stream access by role

`fallingwater.stream.DispatchStream` handles dispatch group creation, typed
action decoding, reads, claims, acknowledgements, and action writes through
`append_event`. Its
`conversation_stream(id)` factory creates a `ConversationStream` bound to one
conversation. That object handles typed event encoding and decoding,
conversation reads and reader groups, and paginated event iteration from an
optional exclusive Redis ID. Related writes to conversation and
dispatch streams are issued separately, without a Redis transaction or an
agent-run lock. Conversation events are written through the bound stream's
`append_event` method.
Both stream classes treat Redis `BUSYGROUP` during group creation as an
already-created group and propagate other Redis errors.
`models.py` contains the event and action schemas and the event payload
registry. `stream_utils.py` contains stream key formatting and codecs; callers
import schemas directly from `models.py`.

Streams accept supplied payloads and provide only typed Redis access.
`append_payload` selects the registered wire type without constructing a new
workflow payload; `append_event` retains explicit type/payload writes.
`stream.py` has no dependency on `Conversation`, turn machines, or notification
intents.

`conversation.py` defines `Conversation`, `PreparedEvent`, and
`reconstruct_conversation`, depending only on payload models and the pure
turn-state module. `Conversation` exposes pure preparation, dispatch eligibility, result
eligibility, child-outcome normalization, and post-replay resumption decisions.
`prepare` returns a `PreparedEvent` containing the validated payload and
emissions without advancing state. Its `dispatch(entry_id)` binds terminal
notification intents to the actual persisted event ID. New-turn preparation
uses an unregistered machine, allowing `Chat` to submit turns without replaying
history or optimistically adding them to canonical state.

`Chat` and `Worker` coordinate reads, writes, and acknowledgement policy.
Worker checkpointing refreshes state, prepares a fact, persists it with a
cursor check, applies the saved event, then publishes its actions.
Only applying persisted events advances replayed conversation state, so an
optimistic write cannot displace an earlier concurrent outcome. Worker result
saving refreshes before checking eligibility and validating, then uses
`ConversationStream.append_if_current` to atomically check for unseen events
and append through a single-key Lua script. A stale cursor returns up to 100
unseen events without writing; Worker replays them, revalidates, and retries
while the result remains eligible. A successful append returns only its new
event. Worker replays through that event ID and asks `Conversation` for resume
actions without reading later writes, so only the result completing the tool
set emits a resume. Lua handles stream operations and opaque encoded payloads,
not domain decisions. Dispatch publication remains a separate write.
Delegation registration and agent initialization also reuse the cursor-checked
append mechanism. The complete parent request precedes agent creation/queueing.
Queue and terminal checkpoints are cursor-checked so a queued turn's snapshot
reflects the persisted order of concurrent enqueue and completion.
`Worker` routes child-result actions before
loading an agent because those actions use only saved stream state. The CLI
inspection commands and web SSE feeds keep their read-only query patterns,
including scans and multi-stream reads.

`recovery.py` currently contains only a no-op `recover()` and a checklist for
future reconciliation of partial writes. It does not repair streams yet.

## AD15 — Conversation archive files

`archive.py` writes conversation events as newline-delimited Pydantic JSON in a
Zstandard-compressed file. Zstandard's stream writer and reader keep file I/O
incremental. Restore validates each event and appends it to an empty conversation
stream. Each archive record includes the required source `entry_id`; restore
validates it but lets Redis generate new stream IDs when appending events.
Archives without IDs are not supported. Archive files use only `.jsonl.zst`;
Redis conversation event payloads
also use Zstandard, with a separate frame for each event.

## AD16 — Payload transitions and per-turn machines

`turn_state.py` defines Pydantic transition models with actual payloads as their
`start` and `stop` endpoints. A `TypeAdapter` selects the edge; endpoint validators
check payload correlation and schemas, and `validate_context(machine)` checks
active steps, pending calls, delegations, and agent readiness before `emit(machine)`
constructs emissions. Both checks precede any machine mutation. Child-outcome
normalization validates context without emitting actions. Machine context holds
information needed across edges, so a last payload does not imply sequential execution. Request
transitions emit a tuple of tool actions for parallel fan-out; unique result
transitions emit an agent resume only when all calls have results. Delegations
and ordinary results can interleave in any order.

Child-outcome transitions normalize a saved child completion into a parent tool
result, or a child failure into a parent failure. Terminal transitions emit a
`ParentNotification` intent rather than inventing a Redis ID. The persistence
boundary resolves it to `SubAgentResultPayload` after appending the terminal
event, preserving direct reads of that exact child event using the existing
dispatch routing format.

`preview()` validates and emits without mutation; strict `transition()` also
advances the machine and rejects invalid new facts. Replay uses the same transitions
but discards emissions, retaining the first result for each call and the first
terminal outcome. Identical current request checkpoints do not duplicate
messages or reset results; obsolete results and late work after termination are
ignored. Queue-time history includes canonical completed turns in
terminal-event order. Overlapping turns never gain answers that arrived
after submission, including turns in persistent agent conversations.

Machine status is derived from the current payload: `completed` identifies
successful completion, `failed` identifies terminal failure, and `running` means
neither. Running includes a machine before its queued payload and turns waiting
for tools or children; it does not mean a worker is actively executing.
Conversation history advances only when replay changes a running turn into a
completed one, never on failure or repeated terminal events.

Workers ask `Conversation` whether an action is ready, waiting, or stale;
the conversation delegates turn-specific rules to its machines. They execute
ready work, leave premature resumes pending, and
acknowledge stale actions. Dependency/schema validation and Redis/error boundaries
remain outside the machine. These machines do not provide locks, atomic writes,
exactly-once execution, or partial-write reconciliation; `recovery.py` remains
unimplemented.

## AD17 — Persistent named agent conversations

`AgentRequest` defaults to ephemeral execution, creating a fresh
conversation even if named. Explicit `persistent=True` resolves a request name
or the referenced agent definition's name, then creates or reuses its
parent-scoped session. Name-only follow-ups require an existing registration;
explicitly conflicting agent/model/dependency settings are rejected. Parent
delegation events hold the complete resolved specification and rebuild
`Conversation.agent_by_name` plus its original specifications across parent
steps and turns. Deps and answer-only results are unchanged. Persistence means
reusable identity, not different retention.

Persistent agent conversations allow overlapping turns, just like ordinary
conversations. Every queued turn emits its own poke and captures history/usage
from turns completed before its queued event. Callers order dependent follow-ups
by waiting for previous results; persistence does not impose FIFO execution.
Each turn stores its own parent reference, preventing follow-up answers from
resolving the original creation call. Registration and setup are cursor-checked
and replayable, but dispatch publication remains separate and ownership renewal,
exactly-once external effects, and full cross-stream recovery are not provided.
