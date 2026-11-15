"""Exercise the contrib toy executor without Redis or a model provider."""

import tomllib
from textwrap import dedent

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from contrib.fwlite import Executor, print_trace
from fallingwater.agent_proxy import AgentRequest


def test_fifo_tools_and_session_history(capsys) -> None:
    order = []

    def respond(messages, info):
        if isinstance(messages[-1], ModelRequest) and any(
            isinstance(part, ToolReturnPart) for part in messages[-1].parts
        ):
            values = [
                part.content
                for part in messages[-1].parts
                if isinstance(part, ToolReturnPart)
            ]
            assert values == ["deps:first", "second"]
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(
            parts=[
                ToolCallPart("first", {"label": "first"}, tool_call_id="first"),
                ToolCallPart("second", {}, tool_call_id="second"),
            ]
        )

    agent = Agent(FunctionModel(respond), deps_type=str)

    @agent.tool
    def first(ctx: RunContext[str], label: str) -> str:
        order.append("first")
        return f"{ctx.deps}:{label}"

    @agent.tool_plain
    async def second() -> str:
        order.append("second")
        return "second"

    executor = Executor()
    first_id = executor.create(agent, deps="deps")
    other_id = executor.create(
        Agent(TestModel(custom_output_text="other session"))
    )
    executor.send(first_id, "one")
    executor.send(other_id, "independent")
    executor.send(first_id, "two")
    executor.drain()

    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "other session",
        "done",
        "done",
    ]
    assert all(
        line.startswith("    ") for line in captured.err.splitlines() if line
    )
    traces = [
        tomllib.loads(dedent(block))
        for block in captured.err.split("\n\n")
        if block.strip()
    ]
    assert traces[0]["agent"][0]["prompt"] == "one"
    assert any(
        entry.get("resume")
        for trace in traces
        for entry in trace.get("agent", [])
    )
    assert {
        "tool": [{"name": "first", "args": {"label": "first"}}],
    } in traces
    assert {"result": [{"value": "deps:first"}]} in traces
    assert len(traces) == 13
    assert order == ["first", "second", "first", "second"]
    prompts = [
        part.content
        for message in executor.registry[first_id].history
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]
    assert prompts == ["one", "two"]
    assert not executor.queue
    assert not executor.registry[first_id].turns


def test_toml_trace_handles_nested_values_and_multiline_text(capsys) -> None:
    print_trace(
        "tool",
        name="example",
        args={"text": 'Hello "writer"\nNext line', "options": [None, True]},
    )
    print_trace("result", value=None)

    traces = [
        tomllib.loads(dedent(block))
        for block in capsys.readouterr().err.split("\n\n")
        if block.strip()
    ]
    assert traces[0]["tool"][0]["args"] == {
        "text": 'Hello "writer"\nNext line',
        "options": ["<none>", True],
    }
    assert traces[1] == {"result": [{"value": "<none>"}]}


def test_nested_agents_fan_in_and_named_reuse(monkeypatch, capsys) -> None:
    writer_prompts = []

    def write(messages, info):
        prompts = [
            part.content
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart)
        ]
        writer_prompts.append(prompts)
        return ModelResponse(parts=[TextPart("/".join(prompts))])

    writer = Agent(FunctionModel(write), name="writer")
    middle = Agent(TestModel(call_tools=["ask_writer"]))

    @middle.tool_plain
    def ask_writer() -> AgentRequest:
        return AgentRequest(agent_path="demo:writer", prompt="nested")

    def coordinate(messages, info):
        if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
            values = [
                part.content
                for part in messages[-1].parts
                if isinstance(part, ToolReturnPart)
            ]
            if len(values) == 3:
                assert values[:2] == ["draft", "draft/revise"]
                assert "nested" in values[2]
                return ModelResponse(
                    parts=[ToolCallPart("follow_up", {}, tool_call_id="d")]
                )
            return ModelResponse(parts=[TextPart("|".join(values))])
        return ModelResponse(
            parts=[
                ToolCallPart("delegate", {"prompt": "draft"}, tool_call_id="a"),
                ToolCallPart(
                    "delegate", {"prompt": "revise"}, tool_call_id="b"
                ),
                ToolCallPart("nested", {}, tool_call_id="c"),
            ]
        )

    root = Agent(FunctionModel(coordinate))

    @root.tool_plain
    def delegate(prompt: str) -> AgentRequest:
        return AgentRequest(
            agent_path="demo:writer",
            persistent=True,
            prompt=prompt,
        )

    @root.tool_plain
    def nested() -> AgentRequest:
        return AgentRequest(agent_path="demo:middle", prompt="delegate")

    @root.tool_plain
    def follow_up() -> AgentRequest:
        return AgentRequest(name="writer", persistent=True, prompt="again")

    monkeypatch.setattr(
        "contrib.fwlite.reify_agent",
        lambda path: {"demo:writer": writer, "demo:middle": middle}[path],
    )
    executor = Executor()
    root_id = executor.create(root)
    turn_id = executor.send(root_id, "start")
    executor.drain()

    assert executor.turns[turn_id].output == "draft/revise/again"
    assert len(capsys.readouterr().out.splitlines()) == 1
    assert ["draft", "revise"] in writer_prompts
    assert ["draft", "revise", "again"] in writer_prompts
    assert ["nested"] in writer_prompts
    assert len(executor.registry) == 4
    writer_id = executor.ns[root_id, "writer"]
    assert (
        executor.subagent(
            root_id,
            AgentRequest(name="writer", persistent=True, prompt="again"),
        )
        == writer_id
    )
    assert all(not session.turns for session in executor.registry.values())


def test_name_scope_ephemeral_sessions_and_child_settings(monkeypatch) -> None:
    writer = Agent(TestModel(), name="writer", deps_type=dict[str, int])
    monkeypatch.setattr("contrib.fwlite.reify_agent", lambda path: writer)
    executor = Executor()
    parent = executor.create(Agent(TestModel()), model="test")
    other_parent = executor.create(Agent(TestModel()))
    request = AgentRequest(
        agent_path="demo:writer",
        prompt="hello",
        name="alice",
        persistent=True,
        deps={"number": 42},
    )
    child = executor.subagent(parent, request)
    assert executor.registry[child].deps == {"number": 42}
    assert executor.registry[child].model == "test"
    assert executor.subagent(parent, request) == child
    assert executor.subagent(other_parent, request) != child
    ephemeral = request.model_copy(update={"persistent": False})
    assert executor.subagent(parent, ephemeral) != executor.subagent(
        parent, ephemeral
    )
    with pytest.raises(ValueError, match="Unknown persistent agent"):
        executor.subagent(
            parent,
            AgentRequest(name="unknown", persistent=True, prompt="hello"),
        )


def test_tool_errors_propagate() -> None:
    agent = Agent(TestModel(call_tools=["fail"]))

    @agent.tool_plain
    def fail() -> str:
        raise ValueError("broken tool")

    executor = Executor()
    executor.send(executor.create(agent), "fail")
    with pytest.raises(ValueError, match="broken tool"):
        executor.drain()
