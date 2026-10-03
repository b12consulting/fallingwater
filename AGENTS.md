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
|-- raid.md                   Risks, assumptions, issues, dependencies
|-- roadmap.md                Milestones and completion criteria
|-- temp-update.md            Proposed planning document revisions
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
|   |-- cli.py                argparse CLI for workers, chat, and reset
|   |-- names.py              Words and generator for CLI conversation names
|   |-- proxy.py              Pydantic AI turn processing
|   |-- streams.py            Redis key and event names
|   |-- utils.py              Shared logger and CLI logging setup
|   |-- worker.py             Proxy pool and graceful shutdown
|   `-- demo/                 Example client-defined agents
|       `-- __init__.py       haiku_master agent for pai
`-- tests/                    Pytest tests; conftest enables Typeguard
    |-- conftest.py           Typeguard import hook
    |-- test_catalog.py       Agent import-path checks
    `-- test_cli.py           CLI smoke checks
```

`raid.md` tracks the project's risks, assumptions, issues, and dependencies,
including their resolution. `roadmap.md` sets out the milestones and their
completion criteria. `architecture.md` records design decisions as short,
numbered sections (`AD1`, `AD2`, ...). Give each decision one or a few paragraphs
explaining the choice and its reason; revise its section when the decision
changes. Update the ASCII tree above when a file is added or removed, or when
its role changes significantly.
