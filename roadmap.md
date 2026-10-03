# Roadmap

Fallingwater is a light library for running Pydantic AI conversations through Redis. The `fw` CLI exercises the same path as other clients. Workers finish each turn and can then stop; a later turn resumes the conversation from saved history. The library should be easy to embed in another application.

## 0. Repository setup — complete

- Add an installable Python package, `fw` entry point, Redis-only Docker Compose service, pytest and Typeguard setup.
- Start this roadmap and `raid.md`; add `concept/architecture.md` with `TODO`; update `AGENTS.md`.
- Done when the package installs, `fw --help` works, pytest runs, and Compose configuration validates.

## 1. Agent reification

- Instantiate agents from Pydantic AI specs, with a path for Python-defined tools and capabilities that specs cannot express alone.
- Expose the available agent definitions and capabilities through commands such as `fw ls agent` and `fw ls capability`.
- Done when a configured agent can be constructed and the CLI lists its available building blocks, with tests of spec loading and composition.

## 2. Redis-backed CLI chat

- Send all chat input and output through Redis, using a shared work queue and per-conversation event history.
- Support starting, stopping after a turn, resuming by conversation ID, and listing ongoing conversations.
- Stream responses and tool or delegate progress to the terminal. Drain workers after their active turns during shutdown.
- Done when a CLI can disconnect, workers can restart, and another CLI can resume the same conversation without losing completed turns.

## 3. Web integration and monitoring

- Provide an optional FastAPI `APIRouter` with an SSE conversation feed, health endpoint, and statistics endpoint.
- Add a simple read-only monitoring UI showing ongoing conversations and their recent events.
- Done when a browser can observe an active conversation and reconnect without starting or consuming its work.
