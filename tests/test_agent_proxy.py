"""Agent steps, tool invocation, and subagent resolution checks."""

from copy import deepcopy
from unittest.mock import Mock
from uuid import NAMESPACE_URL, uuid5

import pytest
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
    ToolCallPart,
)
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from fallingwater.agent_proxy import (
    AgentRequest,
    AgentRunResult,
    execute_tool,
    invoke_tool,
    resolve_subagent,
    run,
    run_agent_step,
)
from fallingwater.conversation import Conversation
from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationEvent,
    ConversationPayloadType,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnQueuedPayload,
    get_cnv_event,
)
from fallingwater.turn_state import TurnStateMachine, requests_adapter


def _apply(conversation: Conversation, payload: ConversationPayloadType):
    conversation.apply_one(
        ConversationEvent(
            entry_id=f"{int(conversation.last_event_id.split('-')[0]) + 1}-0",
            type=get_cnv_event(type(payload)),
            payload=payload,
        )
    )


def _parent() -> tuple[Conversation, ToolCallPayload]:
    conversation = Conversation(conversation_id="parent")
    _apply(
        conversation,
        ConversationCreatedPayload(agent="example:parent", model="test"),
    )
    _apply(conversation, TurnQueuedPayload(turn_id="turn", message="delegate"))
    call = ToolCallPart("delegate", {}, tool_call_id="call")
    _apply(
        conversation,
        ToolRequestedPayload(
            turn_id="turn",
            step_id="step",
            requests=requests_adapter.dump_python(
                DeferredToolRequests(calls=[call]), mode="json"
            ),
            step_messages=ModelMessagesTypeAdapter.dump_python(
                [ModelResponse(parts=[call])], mode="json"
            ),
            usage=RunUsage(),
        ),
    )
    return conversation, ToolCallPayload(
        conversation_id="parent",
        turn_id="turn",
        step_id="step",
        tool_call_id="call",
    )


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
    ctx, args = result.tool_contexts[call.tool_call_id]

    value = invoke_tool(agent, call.tool_name, args, ctx)

    assert calls
    assert value == f"value={calls[0]}"


def test_unattached_tool_fails_clearly() -> None:
    agent = Agent(TestModel(), name="tool-less-agent")

    with pytest.raises(ValueError, match="No directly attached function tool"):
        invoke_tool(agent, "missing", {})


@pytest.mark.parametrize("args", [{}, "{}"])
def test_execute_tool_restores_context_from_explicit_runtime_inputs(
    args,
) -> None:
    agent = Agent(TestModel(), deps_type=dict[str, int])
    usage = RunUsage()

    @agent.tool
    def value(ctx: RunContext[dict[str, int]]) -> int:
        assert ctx.agent is agent
        assert ctx.conversation_id == "conversation"
        assert ctx.tool_call_id == "call-1"
        assert ctx.prompt == "read value"
        assert ctx.usage is usage
        return ctx.deps["value"]

    result = execute_tool(
        agent,
        ToolCallPart("value", args, tool_call_id="call-1"),
        prompt="read value",
        messages=[],
        usage=usage,
        deps={"value": 42},
        model="test",
        conversation_id="conversation",
    )

    assert result == 42


def test_public_tool_invocation_supports_context_and_positional_arguments():
    agent = Agent(TestModel(), deps_type=str)

    @agent.tool
    def combine(ctx: RunContext[str], first: str, /, *rest: str) -> str:
        return ":".join([ctx.deps, first, *rest])

    ctx = RunContext(deps="prefix", model=TestModel(), usage=RunUsage())
    args = {"first": "a", "rest": ["b", "c"]}
    assert invoke_tool(agent, "combine", args, ctx) == "prefix:a:b:c"
    assert args == {"first": "a", "rest": ["b", "c"]}
    with pytest.raises(ValueError, match="requires a RunContext"):
        invoke_tool(agent, "combine", args)


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"name": "writer"},
        {"persistent": True},
        {"agent_path": "example:writer", "name": "  "},
    ],
)
def test_agent_request_rejects_invalid_session_selection(fields) -> None:
    with pytest.raises(ValueError):
        AgentRequest(prompt="write", **fields)


def test_agent_request_accepts_name_only_persistent_followup() -> None:
    request = AgentRequest(name="writer", prompt="revise", persistent=True)

    assert request.agent_path is None


def test_agent_step_proposes_events_and_captures_tools_without_mutation():
    agent = Agent(TestModel(call_tools=["answer"]))

    @agent.tool_plain
    def answer() -> str:
        return "forty-two"

    turn = TurnStateMachine("session", "turn")
    turn.transition(TurnQueuedPayload(turn_id="turn", message="question"))
    queued = turn.state
    outcome = run_agent_step(agent, turn, prompt=turn.prompt)

    assert turn.state is queued
    assert turn.request is None
    assert isinstance(outcome.payload, ToolRequestedPayload)
    turn.transition(outcome.payload)
    call = next(iter(turn.calls.values()))
    ctx, args = outcome.tool_contexts[call.tool_call_id]
    value = invoke_tool(agent, call.tool_name, args, ctx)
    turn.transition(
        ToolResultPayload(
            turn_id=turn.turn_id,
            step_id=outcome.payload.step_id,
            tool_call_id=call.tool_call_id,
            result=value,
        )
    )
    messages = list(turn.turn_messages)
    usage = deepcopy(turn.usage)
    final = run_agent_step(agent, turn, prompt=None)

    assert isinstance(final.payload, TurnCompletedPayload)
    assert "forty-two" in final.payload.message
    assert turn.turn_messages == messages
    assert turn.usage == usage
    assert turn.running
    turn.transition(final.payload)
    assert turn.completed


def test_resume_preserves_history_usage_and_deferred_metadata(monkeypatch):
    conversation, _action = _parent()
    turn = conversation.turn_state["turn"]
    assert turn.request is not None
    assert turn.requests is not None
    request = turn.requests
    request.metadata = {"call": {"checkpoint": "retained"}}
    turn.request.requests = requests_adapter.dump_python(request, mode="json")
    turn.transition(
        ToolResultPayload(
            turn_id="turn",
            step_id="step",
            tool_call_id="call",
            result="answer",
        )
    )
    final_messages = [ModelResponse(parts=[TextPart("done")])]
    execute = Mock(
        return_value=AgentRunResult(
            output="done",
            new_messages=final_messages,
            usage=RunUsage(requests=2),
            tool_contexts={},
        )
    )
    monkeypatch.setattr("fallingwater.agent_proxy.run", execute)
    outcome = run_agent_step(Agent(TestModel()), turn, prompt=None)

    kwargs = execute.call_args.kwargs
    assert kwargs["message_history"] == [*turn.history, *turn.turn_messages]
    assert kwargs["usage"] == turn.usage
    assert kwargs["usage"] is not turn.usage
    assert kwargs["conversation_id"] == "parent"
    assert kwargs["deferred_tool_results"].calls == {"call": "answer"}
    assert kwargs["deferred_tool_results"].metadata == request.metadata
    assert isinstance(outcome.payload, TurnCompletedPayload)
    assert ModelMessagesTypeAdapter.validate_python(
        outcome.payload.turn_messages
    ) == [*turn.turn_messages, *final_messages]


def test_subagent_resolution_reuses_settings_and_session_with_new_turn():
    conversation, action = _parent()
    child = AgentRequest(
        agent_path="example:writer",
        prompt="draft",
        persistent=True,
        deps={"style": "short"},
    )
    request = resolve_subagent(
        conversation,
        action,
        child,
        default_name="writer",
    )

    assert conversation.agent_by_name == {}
    assert request.model == "test"
    assert (
        resolve_subagent(conversation, action, child, default_name="writer")
        == request
    )
    next_action = action.model_copy(update={"tool_call_id": "next-call"})
    fresh = resolve_subagent(
        conversation, next_action, child, default_name="writer"
    )
    assert fresh.child_conversation_id != request.child_conversation_id
    assert fresh.child_turn_id != request.child_turn_id
    _apply(conversation, request)
    followup = resolve_subagent(
        conversation,
        next_action,
        AgentRequest(name="writer", persistent=True, prompt="revise"),
    )

    assert followup.child_conversation_id == request.child_conversation_id
    assert followup.child_turn_id != request.child_turn_id
    assert followup.child_turn_id == str(
        uuid5(
            NAMESPACE_URL,
            f"fallingwater:{request.child_conversation_id}:turn:parent:turn:step:next-call",
        )
    )
    assert followup.agent_path == "example:writer"
    assert followup.deps == {"style": "short"}
    assert followup.prompt == "revise"


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"agent_path": "example:other"}, "different agent_path"),
        ({"model": ""}, "different model"),
        ({"deps": {}}, "different dependencies"),
    ],
)
def test_subagent_conflicts_preserve_registration(fields, message):
    conversation, action = _parent()
    request = resolve_subagent(
        conversation,
        action,
        AgentRequest(
            agent_path="example:writer",
            name="writer",
            persistent=True,
            prompt="draft",
            deps={"style": "short"},
        ),
    )
    _apply(conversation, request)
    registrations = dict(conversation.agent_by_name)
    with pytest.raises(ValueError, match=message):
        resolve_subagent(
            conversation,
            action,
            AgentRequest(
                name="writer", persistent=True, prompt="revise", **fields
            ),
        )
    assert conversation.agent_by_name == registrations
