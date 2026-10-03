# Fallingwater

Fallingwater is a Python library for durable AI conversations backed by Redis
Streams, with a CLI and web interface in development.

## Local development

Use a virtual environment, then run `python -m pip install -e '.[test]'`. Start
local Redis with `docker compose up -d redis`. Run tests with `pytest`, and
inspect the CLI with `fw --help`.

## Project principles

- Prefer Pydantic AI agent specs, capabilities, tools, message history, and event
  streaming before building equivalents.
- A worker finishes an agent turn and may then stop. A later turn resumes from
  saved conversation history; exact mid-turn continuation is outside the current
  scope.
- `fw chat` sends and receives messages through Redis so it exercises the full
  worker path.
- Keep FastAPI support optional. The package should remain easy to import into
  an existing application.

## Development conventions

- Build `fw` commands with the standard library's `argparse` subparsers.
- Install the Typeguard import hook for `fallingwater` in `tests/conftest.py`
  before importing package code in tests.
