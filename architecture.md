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

Workers run agents through `runner.run_until_complete`, which may call
`Agent.run_sync` repeatedly while tools are handled. They store all Pydantic
AI messages produced during that logical turn in the conversation stream. The
next turn rebuilds the full history from completed turns and passes it back to
Pydantic AI.
Fallingwater coordinates the turns; the imported agent defines their behavior.
Pydantic AI generates its own run ID for each invocation. Fallingwater uses the
stream's `turn_id` for application-level correlation and does not reuse it as
Pydantic AI's run ID.

## AD4 — Redis-backed conversation turns

Redis Streams connect message submitters, workers, and readers. The shared
`fw:dispatch` stream distributes turns through the `workers` consumer group. A
per-conversation stream records the creation event, queued prompts, outcomes,
and history needed for the next turn. `Chat.receive` reads that stream with
`XREAD`; the CLI uses a separate consumer group on the same stream.

`Chat`, `Proxy`, and the web routes issue Redis stream commands directly at the
workflow that needs them. `streams.py` holds key formatters, event models, and
the compressed conversation event codec. `Chat.send` coordinates the
transaction across the conversation and dispatch streams with two `XADD`
commands on one pipeline.

| Stream                                      | `type`              | Other fields                 |
|---------------------------------------------|---------------------|------------------------------|
| Conversation, `fw:conversation:<id>:events` | conversation event  | `payload`                    |
| Dispatch, `fw:dispatch`                     | `conversation.poke` | `conversation_id`, `turn_id` |

Each conversation event payload has a Pydantic model selected by its `type`.
The stream entry stores only the text event type and a Brotli level 5 compressed
JSON payload. `encode_conversation_event` compresses and validates payloads on
writes; `decode_conversation_event` decompresses and validates them on reads.
The dispatch stream stays text based because its poke entries only carry
routing identifiers.

Submitting a prompt writes `turn.queued` to the conversation stream and a
`conversation.poke` instruction to dispatch in one Redis transaction. The
entries have different Redis stream IDs but share a turn ID. A worker uses the
conversation ID and turn ID to load the queued prompt, fixed agent and model,
and prior history from the conversation stream. Workers write completion or
failure events to that stream. A claimed dispatch entry stays pending in the
`workers` group until the worker acknowledges it after processing; pending
entries can also remain after a worker crash.

Each completed event stores only the new Pydantic AI messages for that turn,
their display response, and the Pydantic AI `RunUsage` value. The `history`
payload field contains the new messages as structured JSON objects; the proxy
appends these deltas in stream order and passes the latest usage value into the
next agent run.

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
new entries with `XREADGROUP`. The default idle period is five minutes and
`fw worker --claim-idle-ms` changes it. `Proxy` validates and processes each poke,
reconstructs the agent, latest completed message history, and usage, and writes a
result event. On SIGINT or SIGTERM, `Worker` signals its proxies to stop; a
proxy finishes any turn it has taken before exiting. A later worker can
reconstruct the agent and handle the next turn from saved history. An
in-progress Pydantic AI run is not resumed midway through a turn. Acknowledged
entries remain in the dispatch stream. If a poke is malformed or has no matching queued event,
the proxy logs and acknowledges it without writing a result. If stored turn data
is malformed while loading a matching turn, the proxy writes `turn.failed`;
Redis read errors propagate and leave the dispatch entry pending.

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

The CLI acknowledges `conversation.created` and unrecognized events as it reads
them. It keeps a `turn.queued` entry pending until it prints that turn's
`turn.completed` or `turn.failed` event, then acknowledges both entries
together. This lets a resumed CLI wait for a turn that was still running when
it closed. A terminal exit between printing and acknowledgement can cause the
result to be printed again. Each group represents one logical terminal reader.
In one-shot mode, `--msg` prints the new turn's result to stdout and any
earlier unread results to stderr. The library's `Chat.receive` and the web
feed still use independent `XREAD` calls.

## AD9 — Compressed conversation payloads

Conversation stream entries contain exactly `type` and `payload`. The payload
is JSON serialized from a Pydantic event payload model and compressed with
Brotli quality 5 through the shared `compress` and `decompress` helpers in
`utils.py`. Redis clients use binary responses so compressed bytes remain
intact; the event codec functions in `streams.py` perform compression,
decompression, and model validation. Dispatch entries remain text fields
because they carry only a conversation ID and turn ID. `PAYLOAD_MODELS` is the public
mapping from event types to their Pydantic payload models.

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
  - Fields: `agent: str`, `model: str`, `deps: dict[str, Any]`
  - Agent import path, selected model, and initial dependencies. `model`
    defaults to an empty string; `deps` defaults to an empty object.
- `turn.queued`
  - Fields: `turn_id: str`, `message: str`
  - Turn ID and user prompt.
- `turn.completed`
  - Fields: `turn_id: str`, `message: str`, `history: list[dict[str, Any]]`,
    `usage: RunUsage`
  - Turn ID, displayed response, structured JSON data for the new Pydantic AI
    messages, and accumulated usage.
- `turn.failed`
  - Fields: `turn_id: str`, `error: str`
  - Turn ID and failure description.

The dispatch stream is text based and does not use the conversation payload
envelope. A `conversation.poke` entry contains `type`, `conversation_id: str`,
and `turn_id: str`; it tells a worker which queued turn to load.

## AD11 — CLI stream inspection

`fw ls` scans the selected namespace for conversation event streams and prints
their IDs as Redis returns them. `fw tail <conversation_id>` decodes the latest
ten conversation events by default; `-n` changes the count. `fw dispatch` decodes
the latest ten dispatch entries and accepts the same count option. Both event
commands print each Redis entry ID, event type, and indented JSON payload in
stream order. The tail output omits completed-turn history and usage by default;
`--full` includes both. Compressed conversation payloads are decoded through
the shared conversation event decoder.

## AD12 — Deferred caller-side tool execution

`Proxy` runs agents through `runner.run_until_complete`. A Pydantic AI hook
defers each tool call; the runner serializes and restores deferred request and
result objects as JSON, then dispatches handlers from the agent module's
optional `tool_executors_by_agent` mapping. The mapping is keyed by `Agent.name`
and tool name. A regular executor can delegate to another named agent by
calling the runner recursively; depth and tool-round limits prevent runaway
delegation. Executor handlers are context-free and their return values must be
JSON serializable. Tool approval requests and context-dependent executor
handlers are not supported yet.
