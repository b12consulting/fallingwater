# Roadmap

This roadmap tracks high-level features and decisions visible to people using
Fallingwater, including developers integrating the library. Each milestone
states what is available, what remains, and what completion means. Technical
implementation choices and concerns belong in `raid.md`.

## 0. Repository setup — complete

- The library installs locally, `fw --help` works, Redis can be started with
  Docker Compose, and the test suite runs.
- The README and project planning documents guide local development.

## 1. Agent configuration and discovery — partial

- **Available:** An application can supply an agent by Python import path, including an `AgentSpec`. The CLI validates the selected agent and offers Pydantic AI's plain `Agent` as a default.
- **Remaining:** Catalog support for file-based specs and composable tools and capabilities is still lacking. Developers should be able to discover available agents and capabilities with commands such as `fw ls agent` and `fw ls capability`.
- **Decision:** Define whether an existing conversation keeps its original agent definition or adopts an updated one after an upgrade.
- **Done when:** An application can configure and compose an agent, inspect what is available, and predict which definition a resumed conversation will use.

## 2. Durable CLI conversations — partial

- **Available:** The CLI can start and resume a chat by ID. Prompts and completed answers pass through Redis, and a worker can finish its current turn before shutdown. A later turn uses the saved conversation history.
- **Remaining:** List conversations, show responses as they arrive, and show tool or delegate progress. Decide how long conversations remain available.
- **Decision:** Senders that need sequential conversation history wait for a turn to finish before submitting the next prompt. Decide what ends a conversation, whether ending can happen automatically, and what later submissions do.
- **Done when:** A client can disconnect and return to the same conversation, worker crashes and restarts do not lose accepted turns, and serialized prompts produce a coherent sequence of answers. Automated tests demonstrate these behaviors.

## 3. Web integration and monitoring

- Provide an optional FastAPI `APIRouter` with an SSE conversation feed, health endpoint, and statistics endpoint. Developer clients need a documented way to reconnect to the feed.
- Add a simple read-only monitoring UI showing ongoing conversations and their recent events.
- Done when a browser can observe an active conversation and reconnect without starting or consuming its work.
