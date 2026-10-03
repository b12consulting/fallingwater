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
per-conversation stream records the creation event, turn status, response, and
history needed for the next turn. `Chat` reads that stream with `XREAD` without
joining the worker group.

| Stream                                      | Event type             | Fields                                                      |
|---------------------------------------------|------------------------|-------------------------------------------------------------|
| Conversation, `fw:conversation:<id>:events` | `conversation.created` | Agent import path, model                                    |
| Conversation                                | `turn.queued`          | Turn ID, user prompt                                        |
| Conversation                                | `turn.started`         | Turn ID                                                     |
| Conversation                                | `turn.completed`       | Turn ID, response text, new Pydantic AI messages, format    |
| Conversation                                | `turn.failed`          | Turn ID, error text                                         |
| Dispatch, `fw:dispatch`                     | `conversation.poke`    | Conversation ID, turn ID                                    |

Submitting a prompt writes `turn.queued` to the conversation stream and a
`conversation.poke` instruction to dispatch in one Redis transaction. The
entries have different Redis stream IDs but share a turn ID. A worker uses the
conversation ID and turn ID to load the queued prompt, fixed agent and model,
and prior history from the conversation stream. Workers write later turn status
events only to that stream.
Existing completion events without a format field contain full-history
snapshots; the proxy can read those alongside newer delta events.
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

Each proxy reads one dispatch entry at a time with `XREADGROUP`, reconstructs the
agent and the latest completed message history, writes a result event, and
acknowledges the dispatch entry. On SIGINT or SIGTERM, `Worker` signals its proxies
to stop; a proxy finishes any turn it has taken before exiting. A later worker
can reconstruct the agent and handle the next turn from saved history. An
in-progress Pydantic AI run is not resumed midway through a turn. Acknowledged
entries remain in the dispatch stream, and pending entries are not reclaimed
after a worker crash.
