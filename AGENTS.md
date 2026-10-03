# AGENTS

## Purpose

This is the quick-start guide for agents working in this repository. See
`README.md` for local development and project conventions.

## File tree

```text
.
|-- AGENTS.md                 Agent-facing repo guide
|-- README.md                 Project overview and local development
|-- pyproject.toml            Package, dependencies, CLI entry point, pytest setup
|-- compose.yaml              Local Redis service
|-- raid.md                   Risks, assumptions, issues, dependencies
|-- roadmap.md                Milestones and completion criteria
|-- concept/
|   |-- architecture.md       Architecture placeholder
|   |-- pydantic-ai-agent.md   Agent specs and conversation turns
|   `-- redis-streams.md      Redis Streams concepts and examples
|-- fallingwater/             Importable library and fw CLI
|   |-- __init__.py           Package initialization
|   |-- __main__.py           python -m fallingwater entry point
|   |-- cli.py                argparse CLI
|   `-- demo/                 Example client-defined agents
|       `-- __init__.py       haiku_master agent for pai
`-- tests/                    Pytest tests; conftest enables Typeguard
```

`raid.md` tracks the project's risks, assumptions, issues, and dependencies,
including their resolution. `roadmap.md` sets out the milestones and their
completion criteria. Update the ASCII tree above when a file is added or
removed, or when its role changes significantly.
