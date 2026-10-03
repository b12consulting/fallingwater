# FallingWater

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

To see a `turn.failed` event, run:

```shell
fw chat -a fallingwater.demo:flaky_agent -m openai:gpt-6-luna
```

This plain Pydantic AI agent fails before the model call on about half of its
turns. The CLI prints the failure and keeps the chat open for another prompt.

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

`fw chat` uses the `user` reader group on each conversation stream. Reopening a
chat shows answers or failures that arrived while the terminal was closed. Use
`--group NAME` to keep an independent read position, and reuse that name when
resuming. A new group replays stored answers from the beginning; run one terminal
session per group at a time.


## Web

Install the web dependencies with `python -m pip install -e '.[web]'`, then
start the API with `fw web`. It listens on `http://localhost:8000` by default.

To follow an existing conversation, replace `boring_wozniak` with its ID:

```shell
curl -N -H 'Accept: text/event-stream' \
  http://localhost:8000/fw-api/conversations/boring_wozniak/events
```

The feed replays existing events and stays open for new ones.


## Testing

Test can be run with `pytest tests`

Tests use the `fw-test` key namespace (the default namespace is `fw`).

For a fresh development instance, stop the workers and run `fw
reset`. Use `fw reset --namespace fw-test` to clear the testing
namespace. Beware, the command deletes all Redis keys with the selected
namespace prefix.


## Tooling

- iredis: https://github.com/laixintao/iredis
