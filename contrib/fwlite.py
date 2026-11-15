"""In-memory, FIFO agent executor: no Redis, workers, or recovery.

Install with ``pip install -e '.[lite]'``. Run a single prompt, or omit it
for an interactive session::

    python contrib/fwlite.py --model openai:gpt-6-luna "Hello"
    python contrib/fwlite.py --model openai:gpt-6-luna \
        --agent fallingwater.demo:editor

Persistent subagents reuse parent-scoped names and history. Turns within a
session run serially; different sessions share the same FIFO job queue.
Exceptions stop the executor. All state disappears when the process exits.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from textwrap import indent
from typing import Any
from uuid import uuid4

from pydantic import TypeAdapter
from pydantic_core import to_jsonable_python
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.messages import ModelMessage, ToolCallPart
from pydantic_ai.usage import RunUsage
from tomli_w import dumps

from fallingwater.agent_proxy import (
    AgentRequest,
    _execute_tool,
    _find_attached_tool,
    run,
)
from fallingwater.catalog import reify_agent


def print_trace(type: str, **info: Any) -> None:
    """Print indented TOML; represent null values as the string '<none>'."""

    def normalize(value: Any) -> Any:
        if value is None:
            return "<none>"
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value

    data = normalize(to_jsonable_python(info, fallback=repr))
    print("\n" + dumps({type: data}), file=sys.stderr)


@dataclass
class Session:
    agent: Agent
    model: str | None = None
    deps: Any = None
    history: list[ModelMessage] = field(default_factory=list)
    turns: deque[str] = field(default_factory=deque)


@dataclass
class Turn:
    session_id: str
    prompt: str | None
    parent: tuple[str, str] | None = None
    messages: list[ModelMessage] = field(default_factory=list)
    usage: RunUsage | None = None
    requests: DeferredToolRequests | None = None
    results: dict[str, Any] = field(default_factory=dict)
    output: Any = None


@dataclass
class RunAgent:
    turn_id: str


@dataclass
class CallTool:
    turn_id: str
    call: ToolCallPart
    context: RunContext[Any]
    args: dict[str, Any]


@dataclass
class ToolResult:
    turn_id: str
    call_id: str
    value: Any


class Executor:
    def __init__(self) -> None:
        self.registry: dict[str, Session] = {}
        self.ns: dict[tuple[str, str], str] = {}
        self.turns: dict[str, Turn] = {}
        self.queue: deque[RunAgent | CallTool | ToolResult] = deque()

    def create(
        self,
        agent: Agent | str,
        *,
        model: str | None = None,
        deps: Any = None,
    ) -> str:
        if isinstance(agent, str):
            agent = reify_agent(agent)
        session_id = str(uuid4())
        self.registry[session_id] = Session(agent, model, deps)
        return session_id

    def send(
        self,
        session_id: str,
        prompt: str,
        *,
        parent: tuple[str, str] | None = None,
    ) -> str:
        session = self.registry[session_id]
        turn_id = str(uuid4())
        self.turns[turn_id] = Turn(session_id, prompt, parent)
        session.turns.append(turn_id)
        if len(session.turns) == 1:
            self.queue.appendleft(RunAgent(turn_id))
        return turn_id

    def drain(self) -> None:
        while self.queue:
            job = self.queue.pop()
            if isinstance(job, RunAgent):
                turn = self.turns[job.turn_id]
                agent = self.registry[turn.session_id].agent
                print_trace(
                    "agent",
                    name=agent.name or "unnamed",
                    **(
                        {"resume": True}
                        if turn.prompt is None
                        else {"prompt": turn.prompt}
                    ),
                )
                self.run_agent(job)
            elif isinstance(job, CallTool):
                print_trace("tool", name=job.call.tool_name, args=job.args)
                self.call_tool(job)
            else:
                print_trace("result", value=job.value)
                self.receive_result(job)

    def run_agent(self, job: RunAgent) -> None:
        turn = self.turns[job.turn_id]
        session = self.registry[turn.session_id]
        if turn.prompt is not None:
            turn.messages = list(session.history)
        tool_results = (
            turn.requests.build_results(calls=turn.results)
            if turn.requests is not None
            else None
        )
        result = run(
            session.agent,
            turn.prompt,
            message_history=turn.messages,
            deferred_tool_results=tool_results,
            deps=session.deps,
            model=session.model,
            usage=turn.usage,
            conversation_id=turn.session_id,
        )
        turn.prompt = None
        turn.messages.extend(result.new_messages)
        turn.usage = result.usage
        turn.results = {}
        if isinstance(result.output, DeferredToolRequests):
            if result.output.approvals or not result.output.calls:
                raise NotImplementedError("Only deferred tool calls supported")
            turn.requests = result.output
            for call in result.output.calls:
                context, args = result.tool_contexts[call.tool_call_id]
                self.queue.appendleft(
                    CallTool(job.turn_id, call, context, args)
                )
            return

        turn.output = result.output
        session.history = list(turn.messages)
        if turn.parent is None:
            print(turn.output)
        else:
            parent_turn_id, call_id = turn.parent
            self.queue.appendleft(
                ToolResult(parent_turn_id, call_id, turn.output)
            )
        session.turns.popleft()
        if session.turns:
            self.queue.appendleft(RunAgent(session.turns[0]))

    def call_tool(self, job: CallTool) -> None:
        turn = self.turns[job.turn_id]
        session = self.registry[turn.session_id]
        tool = _find_attached_tool(session.agent, job.call.tool_name)
        value = _execute_tool(tool, job.args, job.context)
        if isinstance(value, AgentRequest):
            child_id = self.subagent(turn.session_id, value)
            self.send(
                child_id,
                value.prompt,
                parent=(job.turn_id, job.call.tool_call_id),
            )
        else:
            self.queue.appendleft(
                ToolResult(job.turn_id, job.call.tool_call_id, value)
            )

    def receive_result(self, job: ToolResult) -> None:
        turn = self.turns[job.turn_id]
        turn.results[job.call_id] = job.value
        assert turn.requests is not None
        if len(turn.results) == len(turn.requests.calls):
            self.queue.appendleft(RunAgent(job.turn_id))

    def subagent(self, parent_id: str, request: AgentRequest) -> str:
        name = request.name
        if request.persistent and name is not None:
            existing = self.ns.get((parent_id, name))
            if existing is not None:
                return existing
        if request.agent_path is None:
            raise ValueError(f"Unknown persistent agent: {name!r}")
        agent = reify_agent(request.agent_path)
        if request.persistent:
            name = name or agent.name
            if name is None:
                raise ValueError("Persistent agents need a name")
            existing = self.ns.get((parent_id, name))
            if existing is not None:
                return existing
        deps = (
            None
            if not request.deps and agent.deps_type is type(None)
            else TypeAdapter(agent.deps_type).validate_python(request.deps)
        )
        child_id = self.create(
            agent,
            model=request.model or self.registry[parent_id].model,
            deps=deps,
        )
        if request.persistent:
            self.ns[parent_id, name] = child_id
        return child_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prompt", nargs="?")
    parser.add_argument(
        "--agent", default=os.getenv("FW_AGENT", "pydantic_ai:Agent")
    )
    parser.add_argument("--model", default=os.getenv("FW_MODEL"))
    args = parser.parse_args()
    executor = Executor()
    session_id = executor.create(args.agent, model=args.model)
    if args.prompt is not None:
        executor.send(session_id, args.prompt)
        executor.drain()
        return
    while True:
        try:
            prompt = input("> ")
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if prompt.strip():
            executor.send(session_id, prompt)
            executor.drain()


if __name__ == "__main__":
    main()
