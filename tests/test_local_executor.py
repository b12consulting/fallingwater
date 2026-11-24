"""Embedded FIFO scheduling, delegation, failure, and replay checks."""

import pytest
from pydantic_ai import Agent
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

from fallingwater.conversation import Conversation
from fallingwater.local_executor import LocalExecutor
from fallingwater.models import (
    AgentRequest,
    ToolResultPayload,
    TurnFailedPayload,
)


def prompts(messages) -> list[str]:
    return [
        part.content
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]


def writer_response(messages, _info):
    return ModelResponse(parts=[TextPart("/".join(prompts(messages)))])


writer = Agent(FunctionModel(writer_response), name="writer")
middle = Agent(TestModel(call_tools=["ask_writer"]))


@middle.tool_plain
def ask_writer() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:writer", prompt="nested")


def editor_response(messages, _info):
    if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
        return ModelResponse(
            parts=[
                TextPart(
                    "|".join(str(part.content) for part in messages[-1].parts)
                )
            ]
        )
    return ModelResponse(
        parts=[
            ToolCallPart("delegate", {"prompt": "draft"}, tool_call_id="a"),
            ToolCallPart("delegate", {"prompt": "revise"}, tool_call_id="b"),
            ToolCallPart("nested", {}, tool_call_id="c"),
        ]
    )


editor = Agent(FunctionModel(editor_response))


@editor.tool_plain
def delegate(prompt: str) -> AgentRequest:
    return AgentRequest(
        agent_path=f"{__name__}:writer", persistent=True, prompt=prompt
    )


@editor.tool_plain
def nested() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:middle", prompt="delegate")


failing = Agent(TestModel(call_tools=["fail", "unused"]))


@failing.tool_plain
def fail() -> str:
    raise ValueError("broken tool")


@failing.tool_plain
def unused() -> str:
    pytest.fail("A failed turn must not execute leftover tool actions")


parent = Agent(TestModel(call_tools=["ask_failure"]))


@parent.tool_plain
def ask_failure() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:failing", prompt="fail")


def test_serial_history_events_and_independent_sessions(capsys) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        other = executor.create(f"{__name__}:writer")
        first = executor.send(root, "first")
        second = executor.send(root, "second")
        independent = executor.send(other, "other")
        assert second not in executor.registry[root].conversation.turn_state

        executor.drain()

        conversation = executor.registry[root].conversation
        assert conversation.turn_state[first].state.message == "first"
        assert conversation.turn_state[second].state.message == "first/second"
        assert (
            executor.registry[other]
            .conversation.turn_state[independent]
            .state.message
            == "other"
        )
        replayed = Conversation(
            executor.registry[root].events.values(), conversation_id=root
        )
        assert replayed.turn_state[second].state.message == "first/second"
        assert not executor.queue
        assert not executor.registry[root].pending
        assert capsys.readouterr() == ("", "")


def test_nested_fan_in_and_named_child_reuse() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        first = executor.send(root, "edit")
        executor.drain()
        conversation = executor.registry[root].conversation
        answer = conversation.turn_state[first].state.message
        assert "draft|draft/revise|" in answer
        assert "nested" in answer
        child_id = conversation.agent_by_name["writer"]
        second = executor.send(root, "edit again")
        executor.drain()
        assert "draft/revise/draft" in (
            conversation.turn_state[second].state.message
        )
        assert conversation.agent_by_name["writer"] == child_id
        assert len(executor.registry[child_id].conversation.turn_state) == 4
        for session in executor.registry.values():
            replayed = Conversation(
                session.events.values(),
                conversation_id=session.conversation.conversation_id,
            )
            assert all(turn.completed for turn in replayed.turn_state.values())
            assert not session.pending
            assert not session.tool_contexts


def test_failed_child_propagates_and_next_turns_continue() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:parent")
        first = executor.send(root, "first")
        second = executor.send(root, "second")
        other = executor.create(f"{__name__}:writer")
        independent = executor.send(other, "unaffected")
        executor.drain()

        session = executor.registry[root]
        for turn_id in (first, second):
            payload = session.conversation.turn_state[turn_id].state
            assert isinstance(payload, TurnFailedPayload)
            assert payload.error == "Subagent failed: broken tool"
        assert (
            executor.registry[other]
            .conversation.turn_state[independent]
            .completed
        )
        assert not executor.queue
        assert all(not item.pending for item in executor.registry.values())
        assert all(
            not item.tool_contexts for item in executor.registry.values()
        )


def test_invalid_record_does_not_change_event_log() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        turn_id = executor.send(root, "hello")
        before = list(executor.registry[root].events)
        with pytest.raises(ValueError):
            executor.record(
                root,
                ToolResultPayload(
                    turn_id=turn_id,
                    step_id="missing",
                    tool_call_id="missing",
                    result="invalid",
                ),
            )
        assert list(executor.registry[root].events) == before
        executor.drain()
