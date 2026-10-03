# Fallingwater

Fallingwater is a Python library for durable AI conversations backed by Redis
Streams, with a CLI and web interface in development.

## Local development

Use a virtual environment, then run `python -m pip install -e '.[test]'`. Start
local Redis with `docker compose up -d redis`. Run tests with `pytest`, and
inspect the CLI with `fw --help`.


## Cli test drive

Start `fw worker` in one terminal and run:

``` shell
fw chat --agent fallingwater.demo:haiku_master --model openai:gpt-6-luna
```

in another. The chat prints its conversation ID; later, use `fw chat
<id>` to continue it.

The model needs its provider's credentials and the corresponding
Pydantic AI extra.

If the selected agent has a default model,  `--model`
is not required.

New CLI conversations receive a readable ID such as
`hopeful_morse`. Use that name as the positional argument to
resume.

Library-created conversations will use UUID IDs.  For repeated new
chats, set `FW_AGENT` and `FW_MODEL` instead and run `fw chat`;
explicit CLI options take precedence.

Resumed chats always use their saved agent and model, `--agent` and
`--model` apply only when starting a new conversation.  Without
`FW_AGENT`, new chats use Pydantic AI's plain `Agent`. It has no
default model, so set `FW_MODEL` or pass `-m`.

## Testing

Test can be run with `pytest tests`

Tests use the `fw-test` key namespace (the default namespace is `fw`).

For a fresh development instance, stop the workers and run `fw
reset`. Use `fw reset --namespace fw-test` to clear the testing
namespace. Beware, the command deletes all Redis keys with the selected
namespace prefix.


## Tooling

- iredis: https://github.com/laixintao/iredis
