# Roadmap

This roadmap tracks high-level features and decisions visible to people using
Fallingwater, including developers integrating the library. Each milestone
states what is available, what remains, and what completion means. Technical
implementation choices and concerns belong in `raid.md`.

## 0. Repository setup — complete

### Available

The library installs locally, `fw --help` works, Redis can be started with
Docker Compose, and the test suite runs. The README and project planning
documents guide local development.


## 1. Agent configuration and discovery — partial

### Available

An application can supply an agent by Python import path, including an
`AgentSpec`. `Chat` validates the selected agent and available model; the CLI
offers Pydantic AI's plain `Agent` as a default.

### Remaining

Catalog support for file-based specs and composable tools and capabilities is
still lacking. Developers should be able to discover available agents and
capabilities with commands such as `fw ls agent` and `fw ls capability`.

### Decision

Define whether an existing conversation keeps its original agent definition or
adopts an updated one after an upgrade.

### Done when

An application can configure and compose an agent, inspect what is available,
and predict which definition a resumed conversation will use.


## 2. Durable CLI conversations — partial

### Available

The CLI can start and resume a chat by ID, including answers or failures that
arrived while it was closed. `fw chat --msg` submits one prompt, prints its
result, and exits. Its conversation reader group defaults to `user` and can be
selected with `--group`. Prompts and completed answers pass through Redis, and a
worker can finish its current turn before shutdown. A later turn uses the saved
conversation history.

### Remaining

List conversations, show responses as they arrive, and show tool or delegate
progress. Decide how long conversations remain available.

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
up to ten events per selected conversation. Both follow new events. A simple
monitoring page displays the recent feed.

### Remaining

Add health and statistics endpoints, and a documented way for clients to
reconnect from their last event instead of replaying the full stream.

### Done when

A browser can observe an active conversation and reconnect without starting or
consuming its work.


## 4. Internals — partial

### Available

Synchronous `RedisStream`, `DispatchStream`, and `ConversationStream` classes
provide shared stream operations and `ReadGroup` handles consumer-group reads
and acknowledgements. `Chat` and `Proxy` use these classes, and `Chat.send`
keeps the cross-stream transaction atomic.

### Remaining

Move the web SSE routes onto the stream classes. They still call `XREAD` and
`XREVRANGE` directly; the recent-conversations feed needs stream methods that
preserve its reverse-tail replay and follow behavior.

Add consumer-group lifecycle management, including deleting worker
consumers with no pending entries when the `Worker` starts.

### Done when

Chat, worker, and web code use the stream classes for Redis stream operations,
with existing event and transaction behavior preserved. Worker startup removes
consumers with no pending entries and preserves consumers that still have
pending work.
