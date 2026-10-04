# Fallingwater

Fallingwater is a Python library for durable AI conversations backed by Redis
Streams, with a CLI and web interface in development.


## Local development

Use a virtual environment, then run `python -m pip install -e '.[test]'`. Start
local Redis with `docker compose up -d redis`. Run tests with `pytest`, and
inspect the CLI with `fw --help`.


## CLI test drive

Start `fw worker` in one terminal, then run `fw chat` in another.

The chat prints its conversation ID; later, use `fw chat <id>` to
continue it. Example sessions:

```shell
$ fw chat
Conversation: relaxed_euler
Model: openai:gpt-6-luna
Agent: pydantic_ai:Agent
> what is the capital of Belgium
Brussels.
> ^D
$ fw chat --agent fallingwater.demo:haiku_master
Conversation: youthful_spence
Model: openai:gpt-6-luna
Agent: fallingwater.demo:haiku_master
> what is the capital of France
Paris is France’s heart
Its bright lights shine through the night
By the Seine it rests
> ^D
$

$ fw chat relaxed_euler # here we pass the id of a previous conversation
Conversation: relaxed_euler
Model: openai:gpt-6-luna
Agent: pydantic_ai:Agent
> tell me more about that city
Brussels is Belgium’s capital and its largest urban center. It’s known for its historic Grand-Place,
Art Nouveau architecture, comic-strip murals, and foods such as waffles, chocolate, and fries.

...
```

Resumed chats always use their saved agent and model; `--agent` and
`--model` apply only when starting a new conversation. If set, the
`FW_AGENT` and `FW_MODEL` environment variables provide defaults for
those options.

When you resume a chat, it prints any answers or failures that arrived
while the terminal was closed. This includes a response to a prompt sent
just before exit.

For a single non-interactive turn, run `fw chat <id> --msg "Hello"`. The
command waits for that turn's result, prints it to stdout, and exits. If
you omit the ID, it starts a conversation and prints its new ID to stderr.
Any earlier unread results are also printed to stderr before the new turn.


## Web

Install the web dependencies with `python -m pip install -e '.[web]'`, then
start the API with `fw web`. It listens on `http://localhost:8000` by default.
Open `http://localhost:8000/fw-api/monitor` for a read-only page that follows
the recent conversation feed.

To follow an existing conversation, replace `boring_wozniak` with its ID:

```shell
curl -N -H 'Accept: text/event-stream' http://localhost:8000/fw-api/conversations/boring_wozniak/events
```

The feed replays existing events and stays open for new ones.

To follow the ten most recently active conversations in one feed:

```shell
curl -N -H 'Accept: text/event-stream' http://localhost:8000/fw-api/conversations/recent/events
```

This feed discovers conversations from dispatched prompts. It adds newly active
conversations and drops the least recently active one when the limit is reached.
For each selected conversation, it first sends up to ten retained events, then
follows new ones. Each event includes a `conversation_id`; ordering across
conversations is approximate.


## Testing

Run the tests with `pytest tests`.

Tests use the `fw-test` key namespace (the default namespace is `fw`).

For a fresh development instance, stop the workers and run `fw reset`.
Use `fw reset --namespace fw-test` to clear the testing namespace. The
command deletes all Redis keys with the selected namespace prefix.


## Tooling

- iredis: https://github.com/laixintao/iredis
