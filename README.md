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
                                                  [11:27:38]
You: what is the capital of Belgium
                                         [11:27:38]
Agent: Brussels.
> ^D
$ fw chat --agent fallingwater.demo:haiku_master
Conversation: youthful_spence
Model: openai:gpt-6-luna
Agent: fallingwater.demo:haiku_master
> what is the capital of France
                                                  [11:28:02]
You: what is the capital of France
                                         [11:28:02]
Agent: Paris is France’s heart
Its bright lights shine through the night
By the Seine it rests
> ^D
$

$ fw chat relaxed_euler # here we pass the id of a previous conversation
Conversation: relaxed_euler
Model: openai:gpt-6-luna
Agent: pydantic_ai:Agent
> tell me more about that city
                                                  [11:29:10]
You: tell me more about that city
                                         [11:29:10]
Agent: Brussels is Belgium’s capital and its largest urban center. It’s known for its historic Grand-Place,
Art Nouveau architecture, comic-strip murals, and foods such as waffles, chocolate, and fries.

...
```

Resumed chats always use their latest saved agent and model; `--agent` and
`--model` apply only when starting a new conversation. If set, the
`FW_AGENT` and `FW_MODEL` environment variables provide defaults for
those options.

Interactive chat accepts new prompts immediately, without waiting for earlier
answers. `prompt_toolkit` keeps your current input intact while answers arrive.
Prompts show their local submission time, and answers and failures reference
that same timestamp, even when they arrive out of order. Timestamp headers sit
on their own lines, right-aligned to the current terminal width. Times come
from the queued Redis event and remain consistent when resuming in the same time zone;
turn IDs remain internal. Wait for an answer before sending a follow-up that
needs it in history: overlapping turns retain their queue-time snapshots.
Queued text and its timestamp have a light-grey background; replies and failures
keep the terminal's default background. Messages use natural terminal wrapping,
without inserting line breaks or padding to match the current screen width.

When you resume a chat, it prints any answers or failures that arrived
while the terminal was closed. This includes a response to a prompt sent
just before exit. Ctrl-C or Ctrl-D leaves submitted work running.

Use `fw chat <id> --rewind` to replay the retained conversation from the
beginning, then continue chatting. This resets only the selected reader group's
position (`user` by default, or `--group <name>`); it does not rerun agents or
tools. The reset affects other readers using that same group, so choose a
separate group for an independent replay. With `--msg`, earlier answers are
reprinted before the new message is submitted.

Use `fw ls` to list conversation IDs in the selected namespace. To inspect
recent events, use `fw tail <id>`; it prints the latest ten events in stream
order by default, without completed message history or usage. Set `-n` to
change the number of events, or `--full` to include both. `fw dispatch` shows
the latest ten dispatch entries and accepts the same `-n` option.

```shell
fw ls
fw tail relaxed_euler -n 20
fw tail relaxed_euler --full
fw dispatch
```

For a single non-interactive turn, run `fw chat <id> --msg "Hello"`. The
command waits for that turn's result, prints it to stdout, and exits. If
you omit the ID, it starts a conversation and prints its new ID to stderr.
Any earlier unread results are printed to stdout before the new turn's result.
This uses the same timestamp headers, message labels, and queued-message styling
as interactive chat.


## Named agent sessions

Tools returning `AgentRequest` create fresh agent conversations by default.
Set `persistent=True` to register a session for reuse within the parent
conversation. An explicit `name` overrides the agent definition's name:

```python
from fallingwater.agent_proxy import AgentRequest

first = AgentRequest(
    agent_path="fallingwater.demo.editor_writers:writer",
    name="alice",
    prompt="Draft an invitation for Friday's team lunch.",
    persistent=True,
)
follow_up = AgentRequest(
    name="alice", prompt="Make your draft warmer.", persistent=True
)
```

Each tool receives only the agent's answer, not a conversation ID. Named
follow-ups use the saved agent, model, dependencies, and completed history.
An unknown name or explicitly conflicting settings fail the requesting turn.
Requests to the same persistent agent may run concurrently, each using history
and usage captured when its turn was queued. Wait for the previous answer
before submitting a follow-up that depends on it. No worker is reserved while
waiting for an agent, and a failure still fails its requesting parent turn.

Names without persistence do not register or reuse sessions. Persistence means
reuse, not retention: one-off conversations are also stored in Redis. Deps
remain reconstructed from the conversation's original settings.

Try the editor, which delegates to one or more writers:

```shell
fw worker -w 2
fw chat --agent fallingwater.demo:editor --model <model>
```

Worker ownership renewal and complete cross-stream crash recovery remain
unimplemented; this is not exactly-once model or tool execution.

## Web

Install the web dependencies with `python -m pip install -e '.[web]'`, then
start the API with `fw web`. It listens on `http://localhost:8000` by default.
Open `http://localhost:8000/fw-api/monitor` for a read-only page that follows
the recent conversation feed. Add `?full=1` to include completed-turn history
and usage in the page.

Open `http://localhost:8000/fw-api/dashboard/` for a
card-based view of the ten recently poked conversations and their latest ten
events. Green slots indicate non-failed events; red slots indicate failures.
Bootstrap Icons mark user messages with a person and assistant messages with a
robot.
Subagent requests use `arrow-bar-down`; tool requests use `nut`, and tool
results use `nut-fill`.
Click a slot to read its message and complete payload. The dashboard refreshes
every two seconds using htmx 4 and Bootstrap 6 alpha, including its bundled
Vanilla Calendar Pro datepicker. Its pinned CDN assets require network access;
Bootstrap 6 is prerelease software.

Use the overview's **Before (UTC)** calendar button to select a date. It shows
the last ten distinct conversations poked before that date's midnight UTC.
**< Prev** moves the exclusive cutoff to the oldest selected dispatch-poke ID,
preserving ordering even when several pokes share a millisecond. Older views
may show a conversation again if it has earlier pokes.
Polling and manual refresh retain the cutoff; **Live** clears it.
The shareable URL uses `?before=<redis-id>`, for example
`/fw-api/dashboard/?before=1700006400000-0`; selection and navigation update it.
The button displays dates as `yyyy/mm/dd`. Event tails and initial settings
are bounded by the cutoff's millisecond, not its stream-local sequence number.
For a `-0` cutoff, events in that boundary millisecond are excluded.

Click a conversation name to open `/fw-api/dashboard/<conversation-id>`.
This page shows its full retained history in rows of ten slots,
with the selected event's details alongside the card. History refreshes without
closing or replacing the selected details.

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


## Diagrams

Install [D2](https://d2lang.com/tour/install/), then run `make docs` (or `make`)
to compile all `docs/*.d2` diagrams to SVGs beside their sources in `docs/`.
Only changed sources are rebuilt. Use `make -B docs` to rebuild all diagrams.


## Tooling

- iredis: https://github.com/laixintao/iredis
