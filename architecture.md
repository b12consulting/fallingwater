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
`Chat` with a Redis client, submit turns, and run `Worker` in their own process.
The bundled `fw` CLI calls these same library classes; its chat input and
responses pass through Redis.

## AD3 — Pydantic AI is the agent layer

`reify_agent` loads a Python import path resolving to a Pydantic AI `Agent`, an
`AgentSpec`, or a zero-argument callable returning either. It converts specs
with `Agent.from_spec`; import-path resolution is cached, while callable results
are created for each turn. Agents can bring their own tools and capabilities.

`runner.run` invokes `Agent.run_sync` once and returns either final output or
deferred tool requests. `Proxy.process` handles one dispatch action: an agent
run, one tool call, or a child result. The full request set and per-call jobs
are written atomically; each result is saved by call ID, and the last unique
result queues one agent resume. A restarted proxy reconstructs the turn from
the conversation stream and recovers pending actions through `XAUTOCLAIM`.
Context-taking tools receive a reconstructed basic `RunContext` for each
deferred call. The next turn rebuilds its history from completed turns.
Fallingwater coordinates the turns; the imported agent defines their behavior.
Pydantic AI generates its own run ID for each invocation. Fallingwater uses the
stream's `turn_id` for application-level correlation and does not reuse it as
Pydantic AI's run ID.

## AD4 — Redis-backed conversation turns

Redis Streams connect message submitters, workers, and readers. The shared
`fw:dispatch` stream distributes turns through the `workers` consumer group. A
per-conversation stream records the creation event, queued prompts, outcomes,
and history needed for the next turn. `Chat.receive` reads that stream through
`ConversationClient` with `XREAD`; the CLI uses a separate consumer group on
the same stream.

`ConversationClient` is the shared Redis boundary for `Chat` and `Proxy`. It
formats stream names, encodes and decodes conversation events, normalizes
dispatch fields, and performs stream reads, writes, acknowledgements, group
creation, and dispatch claims. `streams.py` retains the event models, naming
validation, and wire codecs. `Chat` and `Proxy` keep their workflow decisions:
the CLI reader decides which conversation entries to acknowledge, and the
worker decides when dispatch work is checkpointed and acknowledged.

`Chat` binds its `ConversationClient` to one conversation ID, so conversation
operations use that ID by default. `Proxy` keeps an unbound client because it
routes actions for many conversations and supplies the routed ID explicitly.

`Chat.send` asks the client to coordinate the queued event and dispatch poke
with two `XADD` commands on one Redis transaction. Consumer-group and consumer
names are explicit on client operations because terminal readers consume
conversation streams while workers consume the dispatch stream.

| Stream                                      | `type`              | Other fields                 |
|---------------------------------------------|---------------------|------------------------------|
| Conversation, `fw:conversation:<id>:events` | conversation event  | `payload`                    |
| Dispatch, `fw:dispatch`                     | action type         | conversation, turn, step, call, or child IDs |

Each conversation event payload has a Pydantic model selected by its `type`.
The stream entry stores only the text event type and a Brotli level 5 compressed
JSON payload. `encode_conversation_event` compresses and validates payloads on
writes; `decode_conversation_event` decompresses and validates them on reads.
The dispatch stream stays text based because its action entries only carry
routing identifiers.

Submitting a prompt writes `turn.queued` to the conversation stream and a
`conversation.poke` instruction to dispatch in one Redis transaction. The
entries have different Redis stream IDs but share a turn ID. A worker uses the
conversation ID and turn ID to load the queued prompt, fixed agent and model,
and prior history from the conversation stream. It writes `turn.tool.requested`
after a run returns deferred calls and `turn.tool.result` after each tool call
completes. It writes `turn.completed` or `turn.failed` when the turn ends. Each
dispatch action is acknowledged after its own event checkpoint; a logical turn
can span several independently recovered actions.

Each tool-request event stores the message delta and `RunUsage` from that
`Agent.run_sync` call, along with JSON-compatible deferred requests. The same
Redis transaction writes one dispatch entry per tool call. Tool-result events
are associated with that request and store one JSON-compatible result per call.
The result transaction compares unique call IDs and queues one resume when the
whole request set has results. Completed events store the accumulated new
messages, display response, and `RunUsage`; their `history` field remains the
canonical delta for later turns.

`Proxy.load_conversation` returns the queued `next_prompt`, fixed agent path and
model, prior history, dependencies, current turn ID, and latest usage together
in a frozen Pydantic `Conversation` model.

Senders wait for a turn to finish before submitting the next prompt when they
need sequential history. Workers can process overlapping turns from the same
conversation, and each may load the same prior history.

## AD5 — Conversation identity and configuration

`Chat` generates a UUID when called without a conversation ID. The CLI instead
generates a readable `qualifier_scientist` ID for each new chat and accepts that
ID as the positional argument to resume it. The first conversation event stores
the agent import path and model. `Chat` validates the effective agent and model
before creating a conversation or resuming one. On resume it uses the saved
values, ignoring supplied agent and model options. The CLI passes its arguments
to `Chat` in both cases and defaults to `pydantic_ai:Agent` when `FW_AGENT` is
unset; a new chat needs a model on its agent, in `FW_MODEL`, or through `-m`.

## AD6 — Worker lifecycle

Each proxy creates the `workers` consumer group with `MKSTREAM`, then uses
`XAUTOCLAIM` to recover entries idle for the configured period before reading
new entries with `XREADGROUP`, through `ConversationClient`. The default idle
period is five minutes and `fw worker --claim-idle-ms` changes it. Dispatch
entries identify one initial/resume run, one tool call, or one child result.
Each action checkpoints its event before acknowledgement, so independent calls
can run on different proxies and out of order. Agent runs use a renewable
per-step Redis lease to avoid concurrent duplicate `run_sync` calls. On SIGINT
or SIGTERM, an active model or tool call finishes and is checkpointed; the
proxy then leaves unfinished work pending for another worker. A restarted
proxy has a new consumer ID and recovers eligible dispatch entries with
`XAUTOCLAIM`. Acknowledged entries remain in the dispatch stream. Malformed or
stale actions are logged and acknowledged; Redis failures leave actions pending.

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

## AD8 — CLI reader groups

`Chat.run` and `Chat.run_once` use a consumer group on the conversation stream,
starting at ID `0`. The default group is `user`; `fw chat --group` selects
another. They use the stable consumer name `terminal` so a later CLI process
can read entries left pending by an interrupted session. On startup it reads
pending entries with `XREADGROUP` at `0`, then reads new entries with `>`.

The CLI acknowledges `conversation.created`, `turn.queued`, and unrecognized
events as it reads them. It prints each `turn.completed` or `turn.failed` event
before acknowledging it, so a terminal exit between printing and acknowledgement
can cause the result to be printed again. A single `Chat.read` method reads
pending entries for the stable consumer, then new entries. On startup it handles
events that are available and returns without waiting for queued turns whose
results have not arrived; a new prompt can therefore overlap such a turn. Each
group represents one logical terminal reader. In one-shot mode, `--msg` prints
the new turn's result to stdout and earlier results read during startup catch-up
to stderr. Results that arrive after the new prompt is queued use normal output.
The library's `Chat.receive` and the web feed still use independent `XREAD` calls.

## AD9 — Compressed conversation payloads

Conversation stream entries contain exactly `type` and `payload`. The payload
is JSON serialized from a Pydantic event payload model and compressed with
Brotli quality 5 through the shared `compress` and `decompress` helpers in
`utils.py`. Redis clients use binary responses so compressed bytes remain
intact; `ConversationClient` uses the event codec functions in `streams.py` for
compression, decompression, and model validation. Dispatch entries remain text
fields because they carry only a conversation ID and turn ID. `PAYLOAD_MODELS`
is the public mapping from event types to their Pydantic payload models.

The web event feeds decode conversation payloads before sending SSE data. A
completed payload carries the turn's new message history and `RunUsage`; the
proxy restores the latest usage and passes it to `Agent.run_sync` on the next
turn. This preserves usage across sequential turns. Existing stream data is
reset when adopting the format; legacy event decoding is not supported.

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
  - Fields: `turn_id: str`, `message: str`
  - Turn ID and user prompt.
- `turn.tool.requested`
  - Fields: `turn_id: str`, `step_id: str`, `requests: dict[str, Any]`,
    `history: list[dict[str, Any]]`, `usage: RunUsage`
  - Deferred requests from one model call, its message delta, and usage.
- `turn.tool.result`
  - Fields: `turn_id: str`, `step_id: str`, `tool_call_id: str`, `result: Any`
  - One JSON-compatible tool result associated with a deferred request.
- `turn.subagent.requested`
  - Fields: `turn_id`, `step_id`, `tool_call_id`, `conversation_id`,
    `child_turn_id`
  - Correlation between a parent tool call and its separately queued child
    conversation.
- `turn.completed`
  - Fields: `turn_id: str`, `message: str`, `history: list[dict[str, Any]]`,
    `usage: RunUsage`
  - Turn ID, displayed response, structured JSON data for the new Pydantic AI
    messages, and accumulated usage.
- `turn.failed`
  - Fields: `turn_id: str`, `error: str`
  - Turn ID and failure description.

The dispatch stream is text based and does not use the conversation payload
envelope. `conversation.poke` starts a turn; `turn.tool.call` identifies one
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

`runner.run` calls `Agent.run_sync` once. A Pydantic AI hook defers attached
function tools and returns their requests to `Proxy`. The proxy stores each
request and dispatches each call independently. Results are stored by call ID;
the last unique result schedules the next agent run with `DeferredToolResults`.
Synchronous and asynchronous function tools are supported; deferred requests
and results must be JSON serializable. The originating `RunContext` is used in
the live process; after recovery the proxy rebuilds its basic fields from the
conversation and request. Approval requests and toolsets without a directly
attached function callable are not supported.

## AD13 — Checkpointed agent steps

Each dispatch entry represents one action and is acknowledged after its
checkpoint. A model request is checkpointed before its per-call tool jobs are
enqueued; each tool result is checkpointed before the resume job is scheduled.
On restart, the proxy reconstructs the turn from these events and skips saved
tool results. A crash after a tool side effect but before its result event can
still repeat that tool call. A stop request takes effect after the current
model or tool call is saved, leaving unfinished actions for `XAUTOCLAIM`
recovery.

Nested agent work uses its own conversation stream. A tool may return the
public `SubAgentRequest` model with an agent import path, prompt, optional
model, and JSON dependencies. The proxy persists parent correlation and queues
the child in one transaction; it does not wait for the child. Child completion
or failure queues a parent-result action. Child IDs are deterministic for the
parent tool call, so retries reuse the same child. A child failure fails its
parent, and late sibling results are ignored after a terminal event. No proxy
capacity is reserved while child work runs.

## AD14 — Shared conversation client

`fallingwater.conversation.ConversationClient` provides the shared Redis stream
operations used by `Chat` and `Proxy`. It centralizes stream naming, typed
conversation event serialization, dispatch text decoding, range and blocking
reads, writes, consumer-group reads, acknowledgements, and dispatch
`XAUTOCLAIM` cursor handling. Group and consumer names remain parameters so the
client can read both terminal conversation groups and worker dispatch groups.

The client does not decide when an event is complete, when a dispatch entry is
safe to acknowledge, or how an agent turn is resumed. `Chat` and `Proxy` retain
those policies. The CLI inspection commands and web SSE feeds keep their
read-only query patterns, which include scans and multi-stream reads.
