# Architecture

## AD1 — Redis key namespace

Every Redis key owned by Fallingwater, including stream keys, uses a configurable
namespace prefix followed by `:`. The default namespace is `fw`; Redis-backed
tests use `fw-test`. For example, a conversation stream might be named
`fw:conversation:abc:events` in normal use and
`fw-test:conversation:abc:events` in a test.

The library should build these names in one place and pass the configured
namespace to every Redis-facing component. This lets the application and tests
share a Redis instance without mixing their data.

## AD2 — Library first

Fallingwater's core is an importable Python library for running durable AI
conversations. Applications can use its agent, conversation, worker, and event
APIs within their own processes. The bundled CLI, optional FastAPI router, and
monitoring interface use the same library so they follow the same conversation
path as an integrating application.

## AD3 — Pydantic AI is the agent layer

Pydantic AI provides agent execution, specs, capabilities, tools, message
history, and streaming. Fallingwater uses these primitives to construct agents
from configuration or from application code. Fallingwater coordinates turns and
stores their history and events; the agent's behavior remains defined through
Pydantic AI.

## AD4 — Redis-backed conversation turns

Redis Streams connect message submitters, workers, and observers. A submitted
user message becomes work for an available worker. The worker reconstructs the
chosen agent, loads prior conversation history, and invokes it for one complete
user turn. A shared work stream distributes turns; a per-conversation stream
records selected progress events, the response, and the history needed for the
next turn. Readers can follow that stream to show responses or monitor progress
without taking work from the workers.

Workers may start when demand rises, process turns for a while, and then shut
down. During graceful shutdown, a worker stops taking new turns, lets its active
turn finish, records the result, acknowledges the work, and exits. A later worker
can reconstruct the agent and resume the conversation on its next turn. Runs
are not resumed midway through a turn.
