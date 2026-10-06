# Architecture

## AD1 — Redis key namespace

Every Redis key owned by Fallingwater, including stream keys, uses a configurable
namespace prefix followed by `:`. The default namespace is `fw`; Redis-backed
tests use `fw-test`. For example, a conversation stream might be named
`fw:conversation:abc:events` in normal use and
`fw-test:conversation:abc:events` in a test.

`StreamNames` builds these names in one place. Redis-facing components receive
the configured namespace, so an application and its tests can share a Redis
instance without mixing their data.

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

Workers invoke `Agent.run_sync` for one complete turn and store Pydantic AI's
new messages for that turn in the conversation stream. The next turn rebuilds
the full history from completed turns and passes it back to Pydantic AI.
Fallingwater coordinates the turns; the imported agent defines their behavior.

## AD4 — Redis-backed conversation turns

Redis Streams connect message submitters, workers, and readers. The shared
`fw:dispatch` stream distributes turns through the `workers` consumer group. A
per-conversation stream records the creation event, queued prompts, outcomes,
and history needed for the next turn. `Chat.receive` reads that stream with
`XREAD`; the CLI uses a separate consumer group on the same stream.

| Stream                                      | `type`                 | Other fields                                      |
|---------------------------------------------|------------------------|---------------------------------------------------|
| Conversation, `fw:conversation:<id>:events` | `conversation.created` | `agent`, `model`                                  |
| Conversation                                | `turn.queued`          | `turn_id`, `message` (user prompt)                |
| Conversation                                | `turn.completed`       | `turn_id`, `message` (response), `history`        |
| Conversation                                | `turn.failed`          | `turn_id`, `error`                                |
| Dispatch, `fw:dispatch`                     | `conversation.poke`    | `conversation_id`, `turn_id`                      |

Submitting a prompt writes `turn.queued` to the conversation stream and a
`conversation.poke` instruction to dispatch in one Redis transaction. The
entries have different Redis stream IDs but share a turn ID. A worker uses the
conversation ID and turn ID to load the queued prompt, fixed agent and model,
and prior history from the conversation stream. Workers write completion or
failure events to that stream. A claimed dispatch entry stays pending in the
`workers` group until the worker acknowledges it after processing; pending
entries can also remain after a worker crash.

Each completed event stores only the new Pydantic AI messages for that turn.
The `history` field contains their JSON representation; the proxy appends
these deltas in stream order when reconstructing history.

`Proxy.load_turn` returns the queued prompt, fixed agent path and model, and
prior history together in a frozen `Turn` dataclass. This gives turn processing
one named value instead of a positional tuple.

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

Each proxy reads one dispatch entry at a time through a `ReadGroup`. A
`DispatchStream` creates the consumer group and returns the `ReadGroup`, which
handles `XREADGROUP` and `XACK`. `Proxy` validates and processes each poke,
reconstructs the agent and latest completed message history, and writes a
result event. On SIGINT or SIGTERM, `Worker` signals its proxies to stop; a
proxy finishes any turn it has taken before exiting. A later worker can
reconstruct the agent and handle the next turn from saved history. An
in-progress Pydantic AI run is not resumed midway through a turn. Acknowledged
entries remain in the dispatch stream, and pending entries are not reclaimed
after a worker crash. If a poke is malformed or has no matching queued event,
the proxy logs and acknowledges it without writing a result. If stored turn data
is malformed while loading a matching turn, the proxy writes `turn.failed`;
Redis read errors propagate and leave the dispatch entry pending.

## AD7 — Web conversation feed

The module-level `router` exposes a read-only SSE route at
`/conversations/{conversation_id}/events`. The small `create_app` wrapper mounts
it under `/fw-api`, and `fw web` serves it with Uvicorn. The route gets its
synchronous Redis client and namespace from the hosting app's state. The SSE
routes return synchronous iterators; FastAPI's `EventSourceResponse` advances
them in a worker thread so blocking Redis reads do not block the event loop. The
app closes the Redis client it creates when it shuts down.

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
feed. It listens for each current conversation event type and keeps at most
200 rendered entries in the browser.

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
