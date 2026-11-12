# AGENTS

## Purpose

This is the quick-start guide for agents working in this repository. See
`README.md` for local development and project conventions.

## File tree

```text
.
|-- AGENTS.md                 Agent-facing repo guide
|-- README.md                 Project overview and local development
|-- Makefile                  Compile D2 diagrams to SVGs beside their sources
|-- architecture.md           Architecture decisions and conventions
|-- backlog.md                Open ideas and todos, plus completed/rejected items
|-- pyproject.toml            Package, dependencies, CLI entry point, pytest setup
|-- compose.yaml              Local Redis service
|-- raid.md                   Technical risks, issues, assumptions, decisions
|-- concept/
|   |-- pydantic-ai-agent.md   Agent specs and conversation turns
|   `-- redis-streams.md      Redis Streams concepts and examples
|-- docs/
|   |-- basic-conversation-flow.d2 Basic conversation sequence diagrams
|   |-- basic-conversation-flow.md Conversation submission, history, and results
|   |-- basic-conversation-flow.svg Rendered basic conversation diagrams
|   |-- subagent-flow.d2      Subagent sequence diagrams
|   |-- subagent-flow.md      Subagent stream events and dispatch flow
|   |-- subagent-flow.svg     Rendered subagent diagrams
|   |-- tool-call-flow.d2     Tool call sequence diagrams
|   |-- tool-call-flow.md     Tool call events, dispatch, and recovery flow
|   `-- tool-call-flow.svg    Rendered tool call diagrams
|-- contrib/                  Small component experiments
|   |-- echo.py               Redis Streams echo worker, chat, and status CLI
|   |-- countdown_stream.py   Redis Streams countdown worker experiment
|   |-- turn-state-machine.py Payload-instance transition model example
|   |-- turn-state-machine2.py Tool and child payload-transition model example
|   |-- test-compression.py   Compression benchmark for a serialized conversation turn
|   |-- test-conversation.py  Standalone Pydantic AI conversation history example
|   |-- test-recursive-runner.py Generic deferred tool and subagent runner example
|   |-- test-subagent.py      Deferred Pydantic AI Harness subagent example
|   |-- test-subagent-json.py  Deferred Pydantic AI subagent JSON round-trip example
|   `-- test-tool-call.py     Automatic and deferred Pydantic AI tool call example
|-- fallingwater/             Importable library and fw CLI
|   |-- __init__.py           Package initialization
|   |-- __main__.py           python -m fallingwater entry point
|   |-- agent_proxy.py        Single-call agent run and attached-tool helpers
|   |-- archive.py            Conversation archive export and restore
|   |-- catalog.py            Agent import-path loading
|   |-- chat.py               Terminal input, event rendering, and reader thread
|   |-- cli.py                argparse CLI for workers, chat, archive, web, and reset
|   |-- conversation.py       Iterable replay, repeatable settings, and pure decisions
|   |-- conversation_client.py Replay snapshots, submission, and reader groups
|   |-- models.py             Conversation and dispatch event/payload schemas
|   |-- names.py              Words and generator for CLI conversation names
|   |-- recovery.py           Recovery work checklist and placeholder
|   |-- stream.py             Low-level typed Redis stream access
|   |-- stream_utils.py       Stream names, validation, and wire codecs
|   |-- turn_state.py         Pure payload transitions and per-turn machines
|   |-- utils.py              Shared logger and CLI logging setup
|   |-- web/                  Optional FastAPI integration, monitor, and dashboard
|   |   |-- __init__.py       Web exports
|   |   |-- apirouter.py      Routes, SSE feeds, and standalone app
|   |   |-- dashboard.py      I/O-free presentation and typed dashboard data
|   |   `-- templates/
|   |       |-- dashboard.html       Recent conversation dashboard page
|   |       |-- dashboard_card.html  Shared card and ten-slot timeline rows
|   |       |-- dashboard_cards.html Polled conversation cards and event slots
|   |       |-- dashboard_conversation.html Full-history page with inline details
|   |       |-- dashboard_event.html Selected event details
|   |       `-- monitor.html  Read-only recent conversation monitor
|   |-- worker.py             Action execution and persistence/replay coordination
|   |-- worker_pool.py        Worker threads and graceful shutdown
|   `-- demo/                 Example client-defined agents
|       |-- __init__.py       Demo agent exports and small examples
|       |-- editor_writers.py Editor delegating to fresh or persistent writers
|       |-- jeopardy.py       Jeopardy agents and an event-printing client example
|       `-- time.py           Basic assistant with a current-time tool
`-- tests/                    Pytest tests; conftest provides shared fixtures
    |-- conftest.py           Typeguard import hook and isolated Redis fixture
    |-- test_agent_proxy.py   Single-call deferred agent checks
    |-- test_archive.py       Zstandard archive export and restore checks
    |-- test_catalog.py       Agent import-path checks
    |-- test_chat.py          Terminal rendering, input, and reader lifecycle checks
    |-- test_cli.py           CLI smoke checks
    |-- test_conversation.py  Dispatch and conversation stream checks
    |-- test_conversation_client.py Client snapshots, submission, and reader-group checks
    |-- test_demo.py          Jeopardy and editor/writer request checks
    |-- test_streams.py       Conversation event codec and naming checks
    |-- test_turn_state.py    Transition validation and all-turn reconstruction checks
    |-- test_web.py           Web routes, page, and SSE feed checks
    `-- test_worker.py        Dispatch and isolated Redis concurrency checks
```

`backlog.md` tracks future ideas and todos under Open, and completed or rejected
items under Closed. `raid.md` tracks technical risks,
assumptions, issues, and implementation choices, including their outcomes and
reasons. Add a RAID decision when a technical choice is discussed or an
implementation must be revisited. `architecture.md` records the implemented
design as short, numbered sections (`AD1`, `AD2`, ...). Give each section one
or a few paragraphs explaining the choice and its reason; revise it when the
implementation changes. Update the ASCII tree above when a file is added or
removed, or when its role changes significantly.
