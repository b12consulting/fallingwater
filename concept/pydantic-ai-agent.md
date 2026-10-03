# Pydantic AI agents: specs and conversation turns

An **agent spec** describes how to construct an agent: its model, instructions,
settings, output shape, and capabilities. Pydantic AI can load an `AgentSpec`
from YAML, JSON, or a Python dictionary and build an `Agent` with
`Agent.from_spec()`. Python code can add tools and custom capabilities when a
spec alone is not enough. The spec defines the agent's behavior; it does not
hold a conversation's history.

## One run per user turn

Calling `agent.run()` (async) or `agent.run_sync()` (sync) starts a run with a
user prompt. The run may make several model requests and tool calls before it
returns a final output. The result contains both that output and the complete
Pydantic AI message history. Pass the history as `message_history` to the next
run to continue the conversation, even with a newly constructed `Agent`.

```python
from pydantic_ai import Agent, AgentSpec
from pydantic_ai.messages import ModelMessagesTypeAdapter

spec = AgentSpec.from_dict({
    "name": "helper",
    "model": "openai:gpt-4o-mini",
    "instructions": "Answer concisely.",
})

agent = Agent.from_spec(spec)
first = agent.run_sync("What is a Redis Stream?")
print(first.output)

# Save these bytes after the run; load them when the user returns.
history_json = first.all_messages_json()
history = ModelMessagesTypeAdapter.validate_json(history_json)

agent = Agent.from_spec(spec)  # This could happen in a different worker.
second = agent.run_sync("How do consumers share work?", message_history=history)
print(second.output)
```

The example needs the Pydantic AI OpenAI extra and provider credentials to run.
In a real conversation, the serialized history would be stored durably between
the two calls.

## Loading an agent from an application

A simple CLI test, given the OpenAI provider extra and valid OpenAI credentials,
is:

```console
pai -m openai:gpt-6-luna
```

Without `--agent`, `pai` runs its built-in generic `Agent`. It gives that agent
a short system prompt for concise Markdown answers and includes the current
date, time, and platform. The `-m` option selects the model; it does not select
a custom agent or add tools.

To use an application-defined agent, `pai` accepts `module:variable`, much like
an ASGI app path:

```console
pai --agent myapp.agents:assistant
pai web --agent myapp.agents:assistant
```

Here `assistant` is an importable `Agent` object. The application can construct
it in Python with its own tools, toolsets, and capabilities. `--agent` also
accepts a YAML or JSON agent spec file. The import path loads a complete agent;
it does not discover tools or capabilities globally. Fallingwater can adopt
the same pattern for application-defined agents while keeping its Redis-backed
conversation flow.

This repository has a small example at `fallingwater.demo.haiku_master`. Try it
with `pai --agent fallingwater.demo.haiku_master`; use `-m` to choose a model.

## How Fallingwater uses this

A queued user message starts one turn. A worker reconstructs the chosen agent
from its spec, loads the conversation history, runs the agent, and records the
new output and history in Redis Streams. It can then stop. The next turn may
run on another VM. The CLI and web interface can follow conversation events
without owning the agent instance. Pydantic AI's streaming APIs can expose
output and tool activity while a turn runs.

This resumes **between completed turns**, not in the middle of a run. A crash
may cause a queued turn to be retried, so tools and external effects need
idempotency. Store the spec identity and version with each turn so a resumed
conversation has a defined agent configuration after deployments.
