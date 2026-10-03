# AGENTS

## Purpose

This is the quick-start guide for agents working in this repository. See
`README.md` for local development and project conventions.

## File tree

```text
.
|-- AGENTS.md                 Agent-facing repo guide
|-- README.md                 Project overview and local development
|-- architecture.md           Architecture decisions and conventions
|-- pyproject.toml            Package, dependencies, CLI entry point, pytest setup
|-- compose.yaml              Local Redis service
|-- raid.md                   Technical risks, issues, assumptions, decisions
|-- roadmap.md                High-level features and milestone outcomes
|-- concept/
|   |-- pydantic-ai-agent.md   Agent specs and conversation turns
|   `-- redis-streams.md      Redis Streams concepts and examples
|-- contrib/                  Small component experiments
|   `-- echo.py               Redis Streams echo worker, chat, and status CLI
|-- fallingwater/             Importable library and fw CLI
|   |-- __init__.py           Package initialization
|   |-- __main__.py           python -m fallingwater entry point
|   |-- catalog.py            Agent import-path loading
|   |-- chat.py               Redis-backed conversation client
|   |-- cli.py                argparse CLI for workers, chat, web, and reset
|   |-- names.py              Words and generator for CLI conversation names
|   |-- proxy.py              Pydantic AI turn processing
|   |-- streams.py            Redis key and event names
|   |-- utils.py              Shared logger and CLI logging setup
|   |-- web.py                FastAPI router, SSE feed, and standalone app
|   |-- worker.py             Proxy pool and graceful shutdown
|   `-- demo/                 Example client-defined agents
|       `-- __init__.py       haiku_master and flaky_agent demos
`-- tests/                    Pytest tests; conftest enables Typeguard
    |-- conftest.py           Typeguard import hook
    |-- test_catalog.py       Agent import-path checks
    |-- test_chat.py          Chat submission checks
    |-- test_cli.py           CLI smoke checks
    |-- test_proxy.py         Conversation loading and turn processing checks
    `-- test_web.py           Web route and SSE feed checks
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
