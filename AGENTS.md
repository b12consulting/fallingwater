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
|-- pyproject.toml            Package, dependencies, CLI entry point, pytest setup
|-- compose.yaml              Local Redis service
|-- raid.md                   Technical risks, issues, assumptions, decisions
|-- roadmap.md                High-level features and milestone outcomes
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
|   |-- catalog.py            Agent import-path loading
|   |-- chat.py               Terminal and library conversation workflow
|   |-- cli.py                argparse CLI for workers, chat, web, and reset
|   |-- names.py              Words and generator for CLI conversation names
|   |-- stream.py             Dispatch and conversation streams and run leases
|   |-- stream_utils.py       Stream names, typed actions, events, IDs, and codecs
|   |-- turn_state.py         Pure reconstruction of one turn from events
|   |-- utils.py              Shared logger and CLI logging setup
|   |-- web/                  Optional FastAPI integration and monitoring page
|   |   |-- __init__.py       Web exports
|   |   |-- apirouter.py      Routes, SSE feeds, and standalone app
|   |   `-- templates/
|   |       `-- monitor.html  Read-only recent conversation monitor
|   |-- worker.py             Dispatch action processing
|   |-- worker_pool.py        Worker threads and graceful shutdown
|   `-- demo/                 Example client-defined agents
|       |-- __init__.py       Demo agent exports and small examples
|       `-- jeopardy.py       Redis-backed Jeopardy host and contestant demos
`-- tests/                    Pytest tests; conftest enables Typeguard
    |-- conftest.py           Typeguard import hook
    |-- test_agent_proxy.py   Single-call deferred agent checks
    |-- test_catalog.py       Agent import-path checks
    |-- test_chat.py          Chat submission checks
    |-- test_cli.py           CLI smoke checks
    |-- test_conversation.py  Dispatch and conversation stream checks
    |-- test_jeopardy.py      Jeopardy child-agent request checks
    |-- test_streams.py       Conversation event codec and naming checks
    |-- test_web.py           Web routes, page, and SSE feed checks
    `-- test_worker.py        Parallel tool and child-turn dispatch checks
```

`roadmap.md` tracks high-level features and decisions visible to users or
developers integrating the library. `raid.md` tracks technical risks,
assumptions, issues, and implementation choices, including their outcomes and
reasons. Add a RAID decision when a technical choice is discussed or an
implementation must be revisited. `architecture.md` records the implemented
design as short, numbered sections (`AD1`, `AD2`, ...). Give each section one
or a few paragraphs explaining the choice and its reason; revise it when the
implementation changes. Update the ASCII tree above when a file is added or
removed, or when its role changes significantly.
