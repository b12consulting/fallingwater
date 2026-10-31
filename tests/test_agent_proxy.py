"""Checks that the runner executes exactly one agent step."""

import pytest
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.models.test import TestModel

from fallingwater.agent_proxy import _execute_tool, _find_attached_tool, run


def test_run_returns_tool_request_without_executing_tool() -> None:
    calls: list[tuple[int, int]] = []
    agent = Agent(
        TestModel(call_tools=["multiply"]),
        name="plain-tool-agent",
    )

    @agent.tool_plain
    def multiply(left: int, /, right: int) -> int:
        calls.append((left, right))
        return left * right

    result = run(agent, "multiply")

    assert isinstance(result.output, DeferredToolRequests)
    assert result.output.calls[0].tool_name == "multiply"
    assert calls == []
    assert result.tool_contexts


def test_run_captures_context_for_a_deferred_tool() -> None:
    agent = Agent(
        TestModel(call_tools=["contextual_tool"]),
        deps_type=str,
        name="context-tool-agent",
    )

    @agent.tool
    def contextual_tool(ctx: RunContext[str], value: int) -> str:
        return f"{ctx.deps}:{value}"

    result = run(
        agent,
        "use the tool",
        deps="dependency",
        conversation_id="conversation-1",
    )

    assert isinstance(result.output, DeferredToolRequests)
    call = result.output.calls[0]
    ctx, args = result.tool_contexts[call.tool_call_id]
    assert ctx.deps == "dependency"
    assert ctx.conversation_id == "conversation-1"
    assert ctx.tool_call_id == call.tool_call_id
    assert ctx.tool_name == "contextual_tool"
    assert args


def test_run_returns_final_output_without_a_tool_request() -> None:
    agent = Agent(TestModel(), name="simple-agent")

    result = run(agent, "hello")

    assert not isinstance(result.output, DeferredToolRequests)
    assert result.output
    assert result.tool_contexts == {}


def test_async_attached_tool_can_be_executed_by_caller() -> None:
    calls: list[int] = []
    agent = Agent(
        TestModel(call_tools=["async_tool"]),
        name="async-tool-agent",
    )

    @agent.tool_plain
    async def async_tool(value: int) -> str:
        calls.append(value)
        return f"value={value}"

    result = run(agent, "use the async tool")
    assert isinstance(result.output, DeferredToolRequests)
    call = result.output.calls[0]
    tool = _find_attached_tool(agent, call.tool_name)
    ctx, args = result.tool_contexts[call.tool_call_id]

    value = _execute_tool(tool, args, ctx)

    assert calls
    assert value == f"value={calls[0]}"


def test_unattached_tool_fails_clearly() -> None:
    agent = Agent(TestModel(), name="tool-less-agent")

    with pytest.raises(ValueError, match="No directly attached function tool"):
        _find_attached_tool(agent, "missing")
