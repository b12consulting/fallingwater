"""Conversation-backed prototype checks without Redis or model providers."""

from dataclasses import dataclass, field

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

from contrib.fwflash import Executor
from fallingwater.agent_proxy import AgentRequest
from fallingwater.conversation import Conversation
from fallingwater.models import (
    Event,
    ToolResultPayload,
    TurnCompletedPayload,
)


def prompts(messages) -> list[str]:
    return [
        part.content
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]


@dataclass
class ToolDeps:
    prefix: str
    order: list[str] = field(default_factory=list)


@dataclass
class RequestDeps:
    request: AgentRequest


def fifo_response(messages, info):
    if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
        assert [part.content for part in messages[-1].parts] == [
            "deps:first",
            "second",
        ]
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(
        parts=[
            ToolCallPart("first", {}, tool_call_id="first"),
            ToolCallPart("second", {}, tool_call_id="second"),
        ]
    )


fifo_agent = Agent(FunctionModel(fifo_response), deps_type=ToolDeps)
independent_agent = Agent(TestModel(custom_output_text="independent"))


@fifo_agent.tool
def first(ctx: RunContext[ToolDeps]) -> str:
    ctx.deps.order.append("first")
    return f"{ctx.deps.prefix}:first"


@fifo_agent.tool
async def second(ctx: RunContext[ToolDeps]) -> str:
    ctx.deps.order.append("second")
    return "second"


def writer_response(messages, info):
    return ModelResponse(parts=[TextPart("/".join(prompts(messages)))])


writer_agent = Agent(FunctionModel(writer_response), name="writer")
middle_agent = Agent(TestModel(call_tools=["ask_writer"]))


@middle_agent.tool_plain
def ask_writer() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:writer_agent", prompt="nested")


def coordinator_response(messages, info):
    if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
        values = [part.content for part in messages[-1].parts]
        if len(values) == 3:
            assert values[:2] == ["draft", "draft/revise"]
            assert "nested" in values[2]
            return ModelResponse(
                parts=[ToolCallPart("follow_up", {}, tool_call_id="d")]
            )
        return ModelResponse(parts=[TextPart(values[0])])
    return ModelResponse(
        parts=[
            ToolCallPart("delegate", {"prompt": "draft"}, tool_call_id="a"),
            ToolCallPart("delegate", {"prompt": "revise"}, tool_call_id="b"),
            ToolCallPart("nested", {}, tool_call_id="c"),
        ]
    )


coordinator_agent = Agent(FunctionModel(coordinator_response))


@coordinator_agent.tool_plain
def delegate(prompt: str) -> AgentRequest:
    return AgentRequest(
        agent_path=f"{__name__}:writer_agent", persistent=True, prompt=prompt
    )


@coordinator_agent.tool_plain
def nested() -> AgentRequest:
    return AgentRequest(
        agent_path=f"{__name__}:middle_agent", prompt="delegate"
    )


@coordinator_agent.tool_plain
def follow_up() -> AgentRequest:
    return AgentRequest(name="writer", persistent=True, prompt="again")


answer_agent = Agent(TestModel(call_tools=["answer"]))


@answer_agent.tool_plain
def answer() -> str:
    return "answer"


settings_writer_agent = Agent(
    TestModel(custom_output_text="draft"),
    name="writer",
    deps_type=dict[str, int],
)


def settings_response(messages, info):
    if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(parts=[ToolCallPart("ask", {}, tool_call_id="ask")])


settings_agent = Agent(FunctionModel(settings_response), deps_type=RequestDeps)


@settings_agent.tool
def ask(ctx: RunContext[RequestDeps]) -> AgentRequest:
    return ctx.deps.request


failing_agent = Agent(TestModel(call_tools=["fail"]))


@failing_agent.tool_plain
def fail() -> str:
    raise ValueError("broken tool")


def test_fifo_tools_and_serial_history_are_conversation_events(capsys) -> None:
    executor = Executor()
    session_id = executor.create(
        f"{__name__}:fifo_agent", deps={"prefix": "deps"}
    )
    other_id = executor.create(f"{__name__}:independent_agent")
    first_id = executor.send(session_id, "one")
    executor.send(other_id, "other")
    second_id = executor.send(session_id, "two")
    session = executor.registry[session_id]
    assert second_id not in session.conversation.turn_state

    executor.drain()

    assert capsys.readouterr().out.splitlines() == [
        "independent",
        "done",
        "done",
    ]
    assert session.deps.order == ["first", "second", "first", "second"]
    assert session.conversation.settings.agent == f"{__name__}:fifo_agent"
    first_turn = session.conversation.turn_state[first_id]
    second_turn = session.conversation.turn_state[second_id]
    assert first_turn.completed and second_turn.completed
    assert prompts(first_turn.history) == []
    assert prompts(second_turn.history) == ["one"]
    assert prompts(second_turn.turn_messages) == ["two"]
    assert [event.type for event in session.events.values()] == [
        Event.CONVERSATION_CREATED,
        Event.TURN_QUEUED,
        Event.TURN_TOOL_REQUESTED,
        Event.TURN_TOOL_RESULT,
        Event.TURN_TOOL_RESULT,
        Event.TURN_COMPLETED,
        Event.TURN_QUEUED,
        Event.TURN_TOOL_REQUESTED,
        Event.TURN_TOOL_RESULT,
        Event.TURN_TOOL_RESULT,
        Event.TURN_COMPLETED,
    ]
    replayed = Conversation(session.events.values(), conversation_id=session_id)
    assert replayed.turn_state[second_id].completed
    assert prompts(replayed.turn_state[second_id].history) == ["one"]
    assert not executor.queue
    assert not session.pending
    assert not session.tool_contexts


def test_nested_fan_in_named_reuse_and_multiple_rounds(capsys):
    executor = Executor()
    root_id = executor.create(f"{__name__}:coordinator_agent")
    turn_id = executor.send(root_id, "start")
    executor.drain()

    assert capsys.readouterr().out.splitlines() == ["draft/revise/again"]
    writer_histories = [
        prompts([*turn.history, *turn.turn_messages])
        for session in executor.registry.values()
        if session.conversation.settings.agent == f"{__name__}:writer_agent"
        for turn in session.conversation.turn_state.values()
    ]
    assert ["draft", "revise", "again"] in writer_histories
    assert ["nested"] in writer_histories
    assert len(executor.registry) == 4
    conversation = executor.registry[root_id].conversation
    root_turn = conversation.turn_state[turn_id]
    assert root_turn.completed
    assert len(root_turn.step_ids) == 2
    writer_id = conversation.agent_by_name["writer"]
    child_turns = executor.registry[writer_id].conversation.turn_state.values()
    assert len(list(child_turns)) == 3
    assert {turn.parent.tool_call_id for turn in child_turns} == {"a", "b", "d"}
    for session in executor.registry.values():
        replayed = Conversation(
            session.events.values(),
            conversation_id=session.conversation.conversation_id,
        )
        assert replayed.agent_by_name == session.conversation.agent_by_name
        assert all(turn.completed for turn in replayed.turn_state.values())
        assert not session.pending
        assert not session.tool_contexts
    assert not executor.queue


def test_invalid_transitions_are_rejected_before_application() -> None:
    executor = Executor()
    session_id = executor.create(f"{__name__}:answer_agent")
    turn_id = executor.send(session_id, "question")
    session = executor.registry[session_id]
    executor.run_agent(session, executor.queue.pop())
    turn = session.conversation.turn_state[turn_id]
    event_ids = list(session.events)
    assert turn.request is not None
    assert turn.usage is not None

    with pytest.raises(ValueError, match="pending tool call"):
        executor.record(
            session_id,
            ToolResultPayload(
                turn_id=turn_id,
                step_id=turn.request.step_id,
                tool_call_id="unknown",
                result="wrong",
            ),
        )
    with pytest.raises(ValueError, match="wait for all tool results"):
        executor.record(
            session_id,
            TurnCompletedPayload(
                turn_id=turn_id,
                message="premature",
                turn_messages=[],
                usage=turn.usage,
            ),
        )
    assert list(session.events) == event_ids
    assert not turn.results
    executor.drain()
    assert turn.completed
    terminal = list(session.events.values())[-1]
    with pytest.raises(ValueError, match="already terminal"):
        executor.record(session_id, terminal.payload)


def test_persistent_scope_saved_settings_and_conflict_validation():
    deps = {
        "request": {
            "agent_path": f"{__name__}:settings_writer_agent",
            "prompt": "draft",
            "name": "alice",
            "persistent": True,
            "model": "test",
            "deps": {"number": 42},
        }
    }
    executor = Executor()
    parent = executor.create(f"{__name__}:settings_agent", deps=deps)
    other_parent = executor.create(f"{__name__}:settings_agent", deps=deps)
    executor.send(parent, "first")
    executor.send(other_parent, "independent")
    executor.drain()
    first = executor.registry[parent].conversation
    second = executor.registry[other_parent].conversation
    child_id = first.agent_by_name["alice"]
    assert child_id != second.agent_by_name["alice"]
    child = executor.registry[child_id]
    assert child.conversation.settings.model == "test"
    assert child.deps == {"number": 42}

    executor.registry[parent].deps.request = AgentRequest(
        name="alice", persistent=True, prompt="revise"
    )
    executor.send(parent, "again")
    executor.drain()
    assert len(child.conversation.turn_state) == 2
    executor.registry[parent].deps.request = AgentRequest(
        name="alice", persistent=True, prompt="conflict", deps={"number": 0}
    )
    executor.send(parent, "conflict")
    with pytest.raises(ValueError, match="different dependencies"):
        executor.drain()
    assert len(child.conversation.turn_state) == 2


def test_tool_errors_stop_execution() -> None:
    executor = Executor()
    session_id = executor.create(f"{__name__}:failing_agent")
    turn_id = executor.send(session_id, "fail")
    with pytest.raises(ValueError, match="broken tool"):
        executor.drain()
    assert (
        executor.registry[session_id].conversation.turn_state[turn_id].running
    )
