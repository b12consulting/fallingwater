"""Pure production turn transitions, parallel dispatch, and conversation replay."""

import ast
from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import TypeAdapter, ValidationError
from pydantic_ai import Agent, DeferredToolRequests
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.conversation import Conversation, reconstruct_conversation
from fallingwater.models import (
    AgentResumePayload,
    ConversationCreatedPayload,
    ConversationEvent,
    ConversationPayloadType,
    ConversationPokePayload,
    PAYLOAD_MODELS,
    ParentToolReference,
    AgentRequestedPayload,
    SubAgentResultPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from fallingwater.turn_state import (
    AnswerTurn,
    ChildCompleted,
    ChildFailed,
    DelegateTool,
    FailTurn,
    FinishTurn,
    MessageHistory,
    ParentNotification,
    RequestTools,
    ResolveTool,
    StartTurn,
    TurnStateMachine,
    child_transition_adapter,
    transition_adapter,
)


def _queued(turn_id="turn-1"):
    return TurnQueuedPayload(turn_id=turn_id, message=f"prompt {turn_id}")


def _request(*call_ids, step_id="step-1", turn_id="turn-1", approvals=()):
    calls = [
        ToolCallPart("lookup", {"key": call_id}, tool_call_id=call_id)
        for call_id in call_ids
    ]
    return ToolRequestedPayload(
        turn_id=turn_id,
        step_id=step_id,
        requests=TypeAdapter(DeferredToolRequests).dump_python(
            DeferredToolRequests(calls=calls, approvals=list(approvals)),
            mode="json",
        ),
        step_messages=ModelMessagesTypeAdapter.dump_python(
            [ModelResponse(parts=calls)], mode="json"
        ),
        usage=RunUsage(requests=1, input_tokens=12),
    )


def _result(call_id="call-1", *, step_id="step-1", turn_id="turn-1"):
    return ToolResultPayload(
        turn_id=turn_id,
        step_id=step_id,
        tool_call_id=call_id,
        result={"value": call_id},
    )


def _delegation(call_id="call-1"):
    return AgentRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id=call_id,
        child_conversation_id="child",
        child_turn_id="child-turn",
        agent_path="unimportable:child",
        model="test",
        prompt="child prompt",
        deps={"key": "value"},
    )


def _completed(turn_id="turn-1", *, requests=2):
    return TurnCompletedPayload(
        turn_id=turn_id,
        message=f"answer {turn_id}",
        turn_messages=ModelMessagesTypeAdapter.dump_python(
            [
                ModelRequest(
                    parts=[UserPromptPart(content=f"prompt {turn_id}")]
                ),
                ModelResponse(parts=[TextPart(content=f"answer {turn_id}")]),
            ],
            mode="json",
        ),
        usage=RunUsage(requests=requests, input_tokens=20 * requests),
    )


def _event(entry_id, payload: ConversationPayloadType):
    event_type = next(
        kind
        for kind, model in PAYLOAD_MODELS.items()
        if isinstance(payload, model)
    )
    return ConversationEvent(
        entry_id=entry_id, type=event_type, payload=payload
    )


def _machine(*call_ids):
    machine = TurnStateMachine("conversation", "turn-1")
    machine.transition(_queued())
    if call_ids:
        machine.transition(_request(*call_ids))
    return machine


def _child_action(**updates):
    return SubAgentResultPayload(
        **{
            "conversation_id": "conversation",
            "turn_id": "turn-1",
            "step_id": "step-1",
            "tool_call_id": "call-1",
            "child_conversation_id": "child",
            "child_turn_id": "child-turn",
            "child_terminal_event_id": "42-7",
            **updates,
        }
    )


@pytest.mark.parametrize(
    ("start", "stop", "transition_type"),
    [
        (None, _queued(), StartTurn),
        (_queued(), _request("call-1"), RequestTools),
        (_request("call-1"), _result(), ResolveTool),
        (_result("call-2"), _delegation(), DelegateTool),
        (_delegation(), _result(), ResolveTool),
        (_result(), _request("call-2", step_id="step-2"), RequestTools),
        (_queued(), _completed(), AnswerTurn),
        (_result(), _completed(), AnswerTurn),
        (
            _delegation(),
            TurnFailedPayload(turn_id="turn-1", error="failed"),
            FailTurn,
        ),
        (_completed(), None, FinishTurn),
        (TurnFailedPayload(turn_id="turn-1", error="failed"), None, FinishTurn),
    ],
)
def test_type_adapter_selects_transition_from_serialized_endpoints(
    start, stop, transition_type
):
    assert isinstance(transition_adapter, TypeAdapter)
    edge = transition_adapter.validate_python(
        {
            "start": start.model_dump(mode="json")
            if start is not None
            else None,
            "stop": stop.model_dump(mode="json") if stop is not None else None,
        }
    )
    assert type(edge) is transition_type
    assert edge.start == start
    assert edge.outcome == stop


@pytest.mark.parametrize(
    "stop",
    [
        _request("call-1", turn_id="other"),
        _completed("other"),
        TurnFailedPayload(turn_id="other", error="wrong turn"),
    ],
)
def test_transition_endpoints_must_belong_to_same_turn(stop):
    with pytest.raises(ValidationError, match="same turn"):
        transition_adapter.validate_python({"start": _queued(), "stop": stop})


@pytest.mark.parametrize(
    ("payloads", "running", "completed", "failed"),
    [
        ([], True, False, False),
        ([_queued()], True, False, False),
        ([_queued(), _request("call-1")], True, False, False),
        ([_queued(), _request("call-1"), _result()], True, False, False),
        ([_queued(), _request("call-1"), _delegation()], True, False, False),
        ([_queued(), _completed()], False, True, False),
        (
            [_queued(), TurnFailedPayload(turn_id="turn-1", error="failed")],
            False,
            False,
            True,
        ),
        (
            [
                _queued(),
                _request("call-1", "call-2"),
                _result(),
                TurnFailedPayload(turn_id="turn-1", error="failed"),
            ],
            False,
            False,
            True,
        ),
    ],
)
def test_machine_status_properties(payloads, running, completed, failed):
    machine = TurnStateMachine("conversation", "turn-1")
    for payload in payloads:
        machine.transition(payload)
    assert machine.running is running
    assert machine.completed is completed
    assert machine.failed is failed


def test_start_and_terminal_transitions():
    machine = TurnStateMachine("conversation", "turn-1")
    poke = ConversationPokePayload(
        conversation_id="conversation", turn_id="turn-1"
    )
    assert machine.transition(_queued()) == (poke,)
    assert machine.prompt == "prompt turn-1"
    assert machine.dispatch_status(poke) == "ready"
    answer = _completed()
    assert machine.transition(answer) == ()
    assert machine.completed
    assert not machine.running
    assert machine.state == answer
    assert machine.turn_messages == ModelMessagesTypeAdapter.validate_python(
        answer.turn_messages
    )
    assert machine.dispatch_status(poke) == "stale"
    assert FinishTurn(start=answer).emit(machine) == ()
    before = deepcopy(vars(machine))
    with pytest.raises(ValueError, match="already terminal"):
        machine.transition(TurnFailedPayload(turn_id="turn-1", error="late"))
    assert vars(machine) == before


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (_request(), "must contain a call"),
        (_request("call-1", "call-1"), "must be unique"),
        (
            _request(
                "call-1",
                approvals=[
                    ToolCallPart("approve", {}, tool_call_id="approval")
                ],
            ),
            "approvals",
        ),
        (
            _request("call-1").model_copy(
                update={"step_messages": [{"kind": "not-a-message"}]}
            ),
            None,
        ),
        (
            _request("call-1").model_copy(
                update={"requests": {"calls": [{"invalid": "call"}]}}
            ),
            None,
        ),
    ],
)
def test_request_validation_is_atomic(payload, message):
    machine = _machine()
    before = deepcopy(vars(machine))
    with pytest.raises(ValueError, match=message):
        machine.transition(payload)
    assert vars(machine) == before


@pytest.mark.parametrize(
    "payload",
    [
        _request("next", step_id="step-2"),
        _result("unknown"),
        _result(step_id="old-step"),
        _result(turn_id="other-turn"),
        _completed(),
        _completed().model_copy(
            update={"turn_messages": [{"kind": "invalid"}]}
        ),
        _delegation().model_copy(
            update={"child_conversation_id": "conversation"}
        ),
        _delegation().model_copy(update={"child_turn_id": "turn-1"}),
    ],
)
def test_invalid_transition_preserves_entire_machine(payload):
    machine = _machine("call-1", "call-2")
    machine.transition(_result("call-2"))
    before = deepcopy(vars(machine))
    with pytest.raises(ValueError):
        machine.transition(payload)
    assert vars(machine) == before


@pytest.mark.parametrize(
    ("call_ids", "payload", "edge_type"),
    [
        ((), _request("call-1"), RequestTools),
        (("call-1",), _result(), ResolveTool),
        (("call-1",), _delegation(), DelegateTool),
        ((), _completed(), AnswerTurn),
    ],
)
def test_preview_validates_context_before_emitting(
    monkeypatch, call_ids, payload, edge_type
):
    machine = _machine(*call_ids)
    before = deepcopy(vars(machine))
    calls = []
    validate_context = edge_type.validate_context
    emit = edge_type.emit

    def validate(edge, context):
        calls.append("validate_context")
        validate_context(edge, context)

    def emit_actions(edge, context):
        calls.append("emit")
        return emit(edge, context)

    monkeypatch.setattr(edge_type, "validate_context", validate)
    monkeypatch.setattr(edge_type, "emit", emit_actions)

    machine.preview(payload)
    assert calls == ["validate_context", "emit"]
    assert vars(machine) == before


@pytest.mark.parametrize(
    ("payload", "edge_type"),
    [
        (_request("next", step_id="step-2"), RequestTools),
        (_result(step_id="old-step"), ResolveTool),
        (
            _delegation().model_copy(
                update={"child_conversation_id": "conversation"}
            ),
            DelegateTool,
        ),
        (_completed(), AnswerTurn),
    ],
)
def test_invalid_context_prevents_emission_and_mutation(
    monkeypatch, payload, edge_type
):
    machine = _machine("call-1")
    before = deepcopy(vars(machine))
    emit = Mock(side_effect=AssertionError("invalid context must not emit"))
    monkeypatch.setattr(edge_type, "emit", emit)

    with pytest.raises(ValueError):
        machine.transition(payload)
    emit.assert_not_called()
    assert vars(machine) == before


@pytest.mark.parametrize("failed", [False, True])
def test_child_outcome_validates_context_without_emitting(monkeypatch, failed):
    machine = _machine("call-1")
    machine.transition(_delegation())
    edge_type = ChildFailed if failed else ChildCompleted
    terminal = (
        TurnFailedPayload(turn_id="child-turn", error="broken")
        if failed
        else _completed("child-turn")
    )
    before = deepcopy(vars(machine))
    contexts = []
    validate_context = edge_type.validate_context

    def validate(edge, context):
        contexts.append(context)
        validate_context(edge, context)

    emit = Mock(side_effect=AssertionError("normalization must not emit"))
    monkeypatch.setattr(edge_type, "validate_context", validate)
    monkeypatch.setattr(edge_type, "emit", emit)

    assert (
        machine.child_outcome(_child_action(), _event("42-7", terminal))
        is not None
    )
    assert contexts == [machine]
    emit.assert_not_called()
    assert vars(machine) == before


def test_parallel_fanout_out_of_order_results_and_interleaved_delegation():
    machine = _machine()
    request = _request("call-1", "call-2", "call-3", "call-4")
    actions = machine.transition(request)
    assert actions == tuple(
        ToolCallPayload(
            conversation_id="conversation",
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id=call_id,
        )
        for call_id in ("call-1", "call-2", "call-3", "call-4")
    )
    assert machine.request == request
    assert machine.requests == TypeAdapter(
        DeferredToolRequests
    ).validate_python(request.requests)
    assert list(machine.calls) == ["call-1", "call-2", "call-3", "call-4"]
    assert all(machine.dispatch_status(action) == "ready" for action in actions)
    resume = AgentResumePayload(
        conversation_id="conversation", turn_id="turn-1", step_id="step-1"
    )
    assert machine.dispatch_status(resume) == "wait"
    assert machine.resume_actions("step-1") == ()
    assert machine.transition(_result("call-3")) == ()
    assert machine.transition(_delegation()) == (
        ConversationPokePayload(conversation_id="child", turn_id="child-turn"),
    )
    assert machine.dispatch_status(actions[0]) == "stale"
    assert machine.dispatch_status(actions[1]) == "ready"
    assert machine.dispatch_status(_child_action()) == "ready"
    assert machine.transition(_result("call-4")) == ()
    assert machine.transition(_result("call-2")) == ()
    assert machine.dispatch_status(resume) == "wait"
    outcome = machine.child_outcome(
        _child_action(), _event("42-7", _completed("child-turn"))
    )
    assert isinstance(outcome, ToolResultPayload)
    assert machine.transition(outcome) == (resume,)
    assert list(machine.results) == ["call-3", "call-4", "call-2", "call-1"]
    assert all(
        isinstance(value, ToolResultPayload)
        for value in machine.results.values()
    )
    assert machine.tool_results["call-1"] == "answer child-turn"
    assert machine.dispatch_status(resume) == "ready"
    assert machine.resume_actions("step-1") == (resume,)
    assert machine.dispatch_status(_child_action()) == "stale"
    assert machine.transition(_completed()) == ()


def test_multiple_tool_rounds_reset_pending_state_and_stale_dispatch():
    machine = _machine("call-1")
    first_request = machine.request
    first_result = _result()
    machine.transition(_delegation())
    machine.transition(first_result)
    with pytest.raises(ValueError, match="Repeated"):
        machine.transition(_request("new-call", step_id="step-1"))
    second_request = _request("call-1", "call-2", step_id="step-2")
    actions = machine.transition(second_request)
    assert machine.request == second_request
    assert machine.results == {}
    assert machine.delegations == {}
    assert machine.step_ids == {"step-1", "step-2"}
    assert first_request is not None
    assert machine.turn_messages == ModelMessagesTypeAdapter.validate_python(
        first_request.step_messages + second_request.step_messages
    )
    for action in (
        ConversationPokePayload(
            conversation_id="conversation", turn_id="turn-1"
        ),
        AgentResumePayload(
            conversation_id="conversation", turn_id="turn-1", step_id="step-1"
        ),
        ToolCallPayload(
            conversation_id="conversation",
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
        ),
        _child_action(),
        actions[0].model_copy(update={"conversation_id": "other"}),
        actions[0].model_copy(update={"turn_id": "other"}),
        actions[0].model_copy(update={"tool_call_id": "unknown"}),
    ):
        assert machine.dispatch_status(action) == "stale"
    before = deepcopy(vars(machine))
    machine.replay(_event("9-0", first_result))
    assert vars(machine) == before
    assert machine.transition(_result("call-2", step_id="step-2")) == ()
    assert machine.transition(_result("call-1", step_id="step-2")) == (
        AgentResumePayload(
            conversation_id="conversation", turn_id="turn-1", step_id="step-2"
        ),
    )


def test_duplicate_and_stale_replay_does_not_change_results_or_terminal_id():
    machine = _machine("call-1", "call-2")
    delegation = _delegation()
    machine.replay(_event("3-0", delegation))
    machine.replay(_event("4-0", _result("call-2")))
    before = deepcopy(vars(machine))
    for payload in (
        _queued(),
        delegation,
        _result("call-2"),
        _result("call-2").model_copy(update={"result": "duplicate overwrite"}),
        _result(step_id="previous-step"),
        _result(turn_id="another-turn"),
        delegation.model_copy(update={"step_id": "previous-step"}),
    ):
        machine.replay(_event("5-0", payload))
        assert vars(machine) == before
    machine.replay(_event("6-0", _result()))
    completed = _completed()
    machine.replay(_event("7-0", completed))
    before = deepcopy(vars(machine))
    for payload in (completed, _result(), delegation, _queued()):
        machine.replay(_event("8-0", payload))
        assert vars(machine) == before
    assert machine.terminal_event_id == "7-0"


@pytest.mark.parametrize(
    ("first", "later"),
    [
        (
            _completed(),
            TurnFailedPayload(turn_id="turn-1", error="late failure"),
        ),
        (
            TurnFailedPayload(turn_id="turn-1", error="first failure"),
            _completed(),
        ),
        (
            _completed(requests=2),
            _completed(requests=9).model_copy(
                update={"message": "late answer"}
            ),
        ),
        (
            TurnFailedPayload(turn_id="turn-1", error="first failure"),
            TurnFailedPayload(turn_id="turn-1", error="late failure"),
        ),
    ],
)
def test_first_persisted_terminal_wins_replay_but_transition_stays_strict(
    first, later
):
    machine = _machine()
    machine.replay(_event("3-0", first))
    before = deepcopy(vars(machine))
    machine.replay(_event("4-0", later))
    assert vars(machine) == before
    assert machine.state == first
    assert machine.completed is isinstance(first, TurnCompletedPayload)
    assert machine.failed is isinstance(first, TurnFailedPayload)
    assert not machine.running
    assert machine.terminal_event_id == "3-0"
    with pytest.raises(ValueError, match="already terminal"):
        machine.transition(later)
    assert vars(machine) == before

    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", _queued()),
            _event("3-0", first),
            _event("4-0", later),
            _event("5-0", _queued("turn-2")),
        ],
        conversation_id="conversation",
    )
    assert conversation.turn_state["turn-1"].state == first
    assert conversation.turn_state["turn-1"].terminal_event_id == "3-0"
    assert conversation.last_event_id == "5-0"
    next_turn = conversation.turn_state["turn-2"]
    if isinstance(first, TurnCompletedPayload):
        assert next_turn.history == ModelMessagesTypeAdapter.validate_python(
            first.turn_messages
        )
        assert next_turn.usage == first.usage
    else:
        assert next_turn.history == []
        assert next_turn.usage is None


@pytest.mark.parametrize(
    "terminal",
    [_completed(), TurnFailedPayload(turn_id="turn-1", error="failed")],
)
@pytest.mark.parametrize(
    "late",
    [
        _request("new-call", step_id="step-2"),
        _result(),
        _delegation(),
    ],
)
def test_late_request_result_and_delegation_after_terminal_are_ignored(
    terminal, late
):
    machine = _machine("call-1")
    machine.replay(_event("3-0", _result()))
    machine.replay(_event("4-0", terminal))
    before = deepcopy(vars(machine))
    machine.replay(_event("5-0", late))
    assert vars(machine) == before
    assert machine.terminal_event_id == "4-0"
    with pytest.raises(ValueError, match="already terminal"):
        machine.transition(late)
    assert vars(machine) == before


@pytest.mark.parametrize("resolved_count", [0, 1, 2])
def test_duplicate_current_request_preserves_messages_results_and_delegations(
    resolved_count,
):
    machine = _machine()
    request = _request("call-1", "call-2")
    machine.replay(_event("3-0", request))
    machine.replay(_event("4-0", _delegation()))
    for index, call_id in enumerate(("call-2", "call-1")[:resolved_count]):
        machine.replay(_event(f"{index + 5}-0", _result(call_id)))
    before = deepcopy(vars(machine))
    duplicate = ToolRequestedPayload.model_validate(
        request.model_dump(mode="json")
    )
    assert duplicate == request
    assert duplicate is not request
    machine.replay(_event("8-0", duplicate))
    machine.replay(_event("9-0", duplicate))
    assert vars(machine) == before
    assert len(machine.turn_messages) == 1
    assert len(machine.results) == resolved_count
    assert machine.delegations == {"call-1": _delegation()}
    with pytest.raises(ValueError):
        machine.transition(duplicate)
    assert vars(machine) == before


@pytest.mark.parametrize("failed", [False, True])
def test_child_terminal_normalization_is_pure_and_preserves_parent_ids(failed):
    machine = _machine("call-1")
    delegation = _delegation()
    machine.transition(delegation)
    terminal = (
        TurnFailedPayload(turn_id="child-turn", error="broken")
        if failed
        else _completed("child-turn")
    )
    edge = child_transition_adapter.validate_python(
        {"start": delegation.model_dump(), "stop": terminal.model_dump()}
    )
    assert type(edge) is (ChildFailed if failed else ChildCompleted)
    before = deepcopy(vars(machine))
    outcome = machine.child_outcome(_child_action(), _event("42-7", terminal))
    assert vars(machine) == before
    if failed:
        assert outcome == TurnFailedPayload(
            turn_id="turn-1", error="Subagent failed: broken"
        )
    else:
        assert outcome == ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
            result="answer child-turn",
        )
    assert outcome is not None
    machine.transition(outcome)
    assert machine.running is (not failed)
    assert machine.failed is failed
    assert not machine.completed


@pytest.mark.parametrize(
    "updates",
    [
        {"conversation_id": "other"},
        {"turn_id": "other"},
        {"step_id": "other"},
        {"tool_call_id": "other"},
        {"child_conversation_id": "other"},
        {"child_turn_id": "other"},
    ],
)
def test_uncorrelated_child_dispatch_is_stale(updates):
    machine = _machine("call-1")
    machine.transition(_delegation())
    action = _child_action(**updates)
    before = deepcopy(vars(machine))
    assert machine.dispatch_status(action) == "stale"
    assert (
        machine.child_outcome(action, _event("42-7", _completed("child-turn")))
        is None
    )
    assert vars(machine) == before


@pytest.mark.parametrize(
    ("entry_id", "payload"),
    [
        ("42-8", _completed("child-turn")),
        ("42-7", _completed("wrong-child-turn")),
        ("42-7", TurnFailedPayload(turn_id="wrong-child-turn", error="bad")),
        ("42-7", _queued("child-turn")),
    ],
)
def test_invalid_child_terminal_fact_preserves_machine(entry_id, payload):
    machine = _machine("call-1")
    machine.transition(_delegation())
    before = deepcopy(vars(machine))
    with pytest.raises(ValueError):
        machine.child_outcome(_child_action(), _event(entry_id, payload))
    assert vars(machine) == before


def test_child_notification_requires_a_pending_delegation():
    machine = _machine("call-1")
    action = _child_action()
    terminal = _event("42-7", _completed("child-turn"))
    assert machine.dispatch_status(action) == "stale"
    assert machine.child_outcome(action, terminal) is None
    machine.transition(_delegation())
    before = deepcopy(vars(machine))
    with pytest.raises(ValueError, match="already delegated"):
        machine.transition(_delegation())
    assert vars(machine) == before
    machine.transition(_result())
    assert machine.child_outcome(action, terminal) is None


@pytest.mark.parametrize(
    "terminal",
    [_completed(), TurnFailedPayload(turn_id="turn-1", error="failed")],
)
def test_terminal_parent_emission_binds_only_the_persisted_redis_id(terminal):
    parent = ParentToolReference(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="parent-call",
    )
    machine = TurnStateMachine("conversation", "turn-1", parent=parent)
    machine.transition(_queued())
    emissions = machine.transition(terminal)
    assert emissions == (ParentNotification(parent, "conversation", "turn-1"),)
    notification = emissions[0]
    assert isinstance(notification, ParentNotification)
    assert machine.terminal_event_id is None
    assert notification.dispatch("900-12") == SubAgentResultPayload(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="parent-call",
        child_conversation_id="conversation",
        child_turn_id="turn-1",
        child_terminal_event_id="900-12",
    )
    assert notification.dispatch("901-0").child_terminal_event_id == "901-0"


def test_conversation_reconstructs_all_turns_with_queue_boundary_snapshots():
    settings = ConversationCreatedPayload(
        agent="example:agent", model="test", deps={"key": "value"}
    )
    one = _completed("turn-1", requests=1)
    two = _completed("turn-2", requests=2)
    events = [
        _event("1-0", settings),
        _event("2-0", _queued("turn-1")),
        _event("3-0", _queued("turn-2")),
        _event("9-9", two),
        _event("9-10", one),
        _event("10-0", _queued("turn-3")),
        _event("11-0", TurnFailedPayload(turn_id="turn-3", error="failure")),
        _event("12-0", _queued("turn-4")),
    ]
    conversation = reconstruct_conversation(
        iter(events), conversation_id="conversation"
    )
    assert isinstance(conversation, Conversation)
    assert conversation.settings == settings
    assert conversation.conversation_id == "conversation"
    assert conversation.last_event_id == "12-0"
    assert set(conversation.turn_state) == {
        "turn-1",
        "turn-2",
        "turn-3",
        "turn-4",
    }
    assert all(
        isinstance(key, str) and isinstance(machine, TurnStateMachine)
        for key, machine in conversation.turn_state.items()
    )
    first, second, third, fourth = (
        conversation.turn_state[f"turn-{number}"] for number in range(1, 5)
    )
    assert first.history == second.history == []
    assert first.terminal_event_id == "9-10"
    assert second.terminal_event_id == "9-9"
    expected_history = ModelMessagesTypeAdapter.validate_python(
        two.turn_messages + one.turn_messages
    )
    assert third.history == fourth.history == expected_history
    assert third.usage == fourth.usage == one.usage
    assert third.usage is not fourth.usage
    assert third.usage is not first.usage
    assert first.usage is not one.usage
    assert not third.running
    assert fourth.running
    assert fourth.prompt == "prompt turn-4"
    assert fourth.usage is not None
    fourth.usage.requests = 999
    assert fourth.history is third.history
    with pytest.raises(TypeError):
        fourth.history[0] = expected_history[-1]
    assert first.usage == third.usage == one.usage
    assert third.history == fourth.history == expected_history


def test_message_history_sequence_operations_across_shared_prefixes():
    messages = ModelMessagesTypeAdapter.validate_python(
        _completed().turn_messages + _completed("turn-2").turn_messages
    )
    empty = MessageHistory()
    prefix = MessageHistory(previous=empty, messages=tuple(messages[:2]))
    history = MessageHistory(previous=prefix, messages=tuple(messages[2:]))
    assert history.previous is prefix
    assert len(empty) == 0
    assert not empty
    assert len(prefix) == 2
    assert len(history) == 4
    assert list(iter(history)) == messages
    assert tuple(history) == tuple(messages)
    for index in range(-len(messages), len(messages)):
        assert history[index] is messages[index]
    for selection in (
        slice(None),
        slice(1, 3),
        slice(-3, -1),
        slice(None, None, -1),
        slice(None, None, 2),
        slice(20, 30),
    ):
        assert history[selection] == messages[selection]
    for index in (len(messages), -len(messages) - 1):
        with pytest.raises(IndexError):
            history[index]
    with pytest.raises(IndexError):
        empty[0]
    with pytest.raises(ValueError):
        history[::0]
    assert history == messages
    assert messages == history
    assert history == tuple(messages)
    assert history == MessageHistory(messages=tuple(messages))
    assert empty == []
    assert history != prefix
    assert history != list(reversed(messages))
    assert history != object()
    assert list(prefix) == messages[:2]


def test_message_history_prefix_structure_is_immutable():
    messages = ModelMessagesTypeAdapter.validate_python(
        _completed().turn_messages
    )
    prefix = MessageHistory(messages=(messages[0],))
    history = MessageHistory(previous=prefix, messages=(messages[1],))
    with pytest.raises(FrozenInstanceError):
        history.previous = MessageHistory()
    with pytest.raises(FrozenInstanceError):
        history.messages = ()
    with pytest.raises(FrozenInstanceError):
        history._length = 0
    with pytest.raises(TypeError):
        history[0] = messages[1]
    with pytest.raises(AttributeError):
        history.append(messages[0])
    copied_slice = history[:]
    copied_slice.clear()
    assert history == messages
    assert prefix == messages[:1]


def test_many_sequential_turns_share_linear_history_storage():
    conversation = reconstruct_conversation(
        [_event("1-0", ConversationCreatedPayload(agent="example:agent"))],
        conversation_id="conversation",
    )
    count = 128
    for index in range(count):
        turn_id = f"turn-{index}"
        conversation.apply(_event(f"{2 * index + 2}-0", _queued(turn_id)))
        conversation.apply(_event(f"{2 * index + 3}-0", _completed(turn_id)))
    conversation.apply(_event("1000-0", _queued("next-turn")))
    machines = [
        conversation.turn_state[f"turn-{index}"] for index in range(count)
    ]
    for previous, successor in zip(
        machines, [*machines[1:], conversation.turn_state["next-turn"]]
    ):
        assert successor.history.previous is previous.history
        assert successor.history.messages == tuple(previous.turn_messages)
        assert all(
            stored is original
            for stored, original in zip(
                successor.history.messages, previous.turn_messages
            )
        )
    latest = conversation.turn_state["next-turn"].history
    nodes = {}
    current = latest
    while current is not None:
        assert id(current) not in nodes
        nodes[id(current)] = current
        current = current.previous
    assert len(nodes) == count + 1
    assert sum(len(node.messages) for node in nodes.values()) == 2 * count
    assert len(latest) == 2 * count
    assert all(id(machine.history) in nodes for machine in machines)
    assert list(latest) == [
        message for machine in machines for message in machine.turn_messages
    ]


def test_overlapping_turns_share_prefixes_in_terminal_completion_order():
    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", _queued("turn-1")),
            _event("3-0", _queued("turn-2")),
            _event("4-0", _completed("turn-2")),
            _event("5-0", _queued("turn-3")),
            _event("6-0", _queued("turn-4")),
            _event("7-0", _completed("turn-1")),
            _event("8-0", _queued("turn-5")),
        ],
        conversation_id="conversation",
    )
    one, two, three, four, five = (
        conversation.turn_state[f"turn-{index}"] for index in range(1, 6)
    )
    assert one.history is two.history
    assert three.history is four.history
    assert three.history.previous is one.history
    assert three.history.messages == tuple(two.turn_messages)
    assert five.history.previous is three.history
    assert five.history.messages == tuple(one.turn_messages)
    assert five.history == two.turn_messages + one.turn_messages
    assert three.history == two.turn_messages
    assert five.usage == one.usage


def test_apply_canonical_terminal_after_checkpoint_updates_history_prefix():
    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", _queued()),
            _event("3-0", _queued("already-queued")),
        ],
        conversation_id="conversation",
    )
    machine = conversation.turn_state["turn-1"]
    prefix = machine.history
    terminal = _event("4-0", _completed())
    before = deepcopy(vars(machine))
    assert machine.preview(terminal.payload) == ()
    assert vars(machine) == before
    assert machine.terminal_event_id is None
    assert conversation.last_event_id == "3-0"
    conversation.apply(terminal)
    conversation.apply(_event("5-0", _queued("next-turn")))
    successor = conversation.turn_state["next-turn"]
    assert successor.history.previous is prefix
    assert successor.history == machine.turn_messages
    assert successor.usage == machine.usage
    assert conversation.turn_state["already-queued"].history is prefix
    assert prefix == []


@pytest.mark.parametrize("first_succeeded", [False, True])
def test_concurrent_first_terminal_wins_after_preview_and_incremental_refresh(
    first_succeeded,
):
    base = [
        _event("1-0", ConversationCreatedPayload(agent="example:agent")),
        _event("2-0", _queued()),
    ]
    conversation = reconstruct_conversation(
        base, conversation_id="conversation"
    )
    machine = conversation.turn_state["turn-1"]
    success = _completed(requests=7)
    failure = TurnFailedPayload(turn_id="turn-1", error="concurrent failure")
    first, later = (success, failure) if first_succeeded else (failure, success)
    persisted = [_event("3-0", first)]
    before = deepcopy(vars(machine))
    # The sibling's terminal fact is persisted but not yet in this snapshot.
    assert machine.preview(later) == ()
    assert vars(machine) == before
    assert machine.running
    assert machine.terminal_event_id is None
    persisted.append(_event("4-0", later))
    for event in persisted:
        conversation.apply(event)
    queued = _event("5-0", _queued("next-turn"))
    conversation.apply(queued)

    assert machine.state == first
    assert machine.terminal_event_id == "3-0"
    successor = conversation.turn_state["next-turn"]
    if first_succeeded:
        assert successor.history == ModelMessagesTypeAdapter.validate_python(
            success.turn_messages
        )
        assert successor.usage == success.usage
    else:
        assert successor.history == []
        assert successor.usage is None
        assert machine.turn_messages == []
        assert machine.usage is None
    fresh = reconstruct_conversation(
        [*base, *persisted, queued], conversation_id="conversation"
    )
    assert conversation.last_event_id == fresh.last_event_id == "5-0"
    assert conversation.settings == fresh.settings
    assert conversation.turn_state.keys() == fresh.turn_state.keys()
    for turn_id, incremental in conversation.turn_state.items():
        assert vars(incremental) == vars(fresh.turn_state[turn_id])


@pytest.mark.parametrize("same_entry_id", [False, True])
def test_duplicate_canonical_terminal_does_not_append_history_twice(
    same_entry_id,
):
    terminal = _event("3-0", _completed())
    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", _queued()),
            terminal,
            _event("4-0", _queued("before-duplicate")),
        ],
        conversation_id="conversation",
    )
    prefix = conversation.turn_state["before-duplicate"].history
    duplicate = _event(
        terminal.entry_id if same_entry_id else "5-0",
        terminal.payload.model_copy(deep=True),
    )
    conversation.apply(duplicate)
    conversation.apply(_event("6-0", _queued("after-duplicate")))
    assert conversation.turn_state["after-duplicate"].history is prefix
    assert prefix == conversation.turn_state["turn-1"].turn_messages
    assert conversation.turn_state["turn-1"].terminal_event_id == "3-0"


def test_history_does_not_gain_completions_after_a_turn_is_queued():
    settings = ConversationCreatedPayload(agent="example:agent")
    conversation = reconstruct_conversation(
        [
            _event("1-0", settings),
            _event("2-0", _queued()),
            _event("3-0", _queued("turn-2")),
            _event("4-0", _completed()),
            _event("5-0", _queued("turn-3")),
        ],
        conversation_id="conversation",
    )
    assert conversation.turn_state["turn-2"].history == []
    assert conversation.turn_state["turn-2"].usage is None
    assert conversation.turn_state["turn-3"].history == (
        conversation.turn_state["turn-1"].turn_messages
    )
    assert conversation.turn_state["turn-3"].usage == _completed().usage


def test_usage_snapshots_are_not_shared_with_request_or_constructor():
    usage = RunUsage(requests=3, input_tokens=40)
    machine = TurnStateMachine("conversation", "turn-1", usage=usage)
    assert machine.usage == usage
    assert machine.usage is not usage
    usage.requests = 100
    assert machine.usage.requests == 3
    machine.transition(_queued())
    request = _request("call-1")
    machine.transition(request)
    assert machine.usage == request.usage
    assert machine.usage is not request.usage
    request.usage.requests = 200
    assert machine.usage.requests == 1


@pytest.mark.parametrize(
    "events",
    [
        [],
        [_event("1-0", _queued())],
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", _completed()),
        ],
        [
            _event("1-0", ConversationCreatedPayload(agent="example:agent")),
            _event("2-0", ConversationCreatedPayload(agent="example:other")),
        ],
    ],
)
def test_reconstruction_rejects_missing_settings_or_missing_prompt(events):
    with pytest.raises(ValueError):
        reconstruct_conversation(events, conversation_id="conversation")


def test_replay_does_not_execute_tools_agents_or_redis_io(monkeypatch):
    forbidden = Mock(side_effect=AssertionError("replay attempted I/O"))
    monkeypatch.setattr(Redis, "execute_command", forbidden)
    monkeypatch.setattr(Agent, "run", forbidden)
    monkeypatch.setattr(Agent, "run_sync", forbidden)
    parent = ParentToolReference(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="parent-call",
    )
    conversation = reconstruct_conversation(
        [
            _event(
                "1-0",
                ConversationCreatedPayload(
                    agent="unimportable:agent", parent=parent
                ),
            ),
            _event("2-0", _queued()),
            _event("3-0", _request("call-1", "call-2")),
            _event("4-0", _result("call-2")),
            _event("5-0", _delegation()),
            _event("6-0", _result()),
            _event("7-0", _completed()),
        ],
        conversation_id="conversation",
    )
    machine = conversation.turn_state["turn-1"]
    assert machine.completed
    assert machine.parent == parent
    assert machine.terminal_event_id == "7-0"
    assert machine.tool_results == {
        "call-2": {"value": "call-2"},
        "call-1": {"value": "call-1"},
    }
    forbidden.assert_not_called()


@pytest.mark.parametrize(
    ("module", "allowed_dependencies"),
    [
        ("turn_state", {"models"}),
        ("conversation", {"models", "turn_state"}),
        ("stream", {"models", "stream_utils", "utils"}),
    ],
)
def test_layer_import_boundaries(module, allowed_dependencies):
    package = Path(__file__).resolve().parents[1] / "fallingwater"
    tree = ast.parse((package / f"{module}.py").read_text())
    dependencies = set()
    external_imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                dependencies.add((node.module or "").split(".")[0])
            elif node.module and node.module.startswith("fallingwater."):
                dependencies.add(node.module.split(".")[1])
            elif node.module == "fallingwater":
                dependencies.update(alias.name for alias in node.names)
            elif node.module:
                external_imports.add(node.module.split(".")[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("fallingwater."):
                    dependencies.add(alias.name.split(".")[1])
                else:
                    external_imports.add(alias.name.split(".")[0])
    assert dependencies <= allowed_dependencies
    if module in {"turn_state", "conversation"}:
        assert external_imports <= {
            "abc",
            "collections",
            "copy",
            "dataclasses",
            "typing",
            "pydantic",
            "pydantic_ai",
        }
    else:
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = (
                    node.func.id
                    if isinstance(node.func, ast.Name)
                    else node.func.attr
                    if isinstance(node.func, ast.Attribute)
                    else ""
                )
                assert not name.endswith("Payload")


def test_conversation_prepares_new_turns_without_registering_them():
    conversation = Conversation(
        conversation_id="conversation",
        settings=ConversationCreatedPayload(agent="unimportable:agent"),
    )
    for turn_id in ("first", "second"):
        queued = _queued(turn_id)
        prepared = conversation.prepare(queued)
        assert prepared.payload is queued
        assert prepared.dispatch("42-7") == (
            ConversationPokePayload(
                conversation_id="conversation", turn_id=turn_id
            ),
        )
    assert conversation.turn_state == {}
    assert conversation.last_event_id == "0-0"
    with pytest.raises(ValueError, match="No prompt found"):
        conversation.prepare(_completed())


def test_conversation_workflow_preparation_is_pure(monkeypatch):
    forbidden = Mock(side_effect=AssertionError("preparation attempted I/O"))
    monkeypatch.setattr(Redis, "execute_command", forbidden)
    monkeypatch.setattr(Agent, "run_sync", forbidden)
    monkeypatch.setattr(Agent, "run", forbidden)
    monkeypatch.setattr("fallingwater.catalog.reify_agent", forbidden)
    parent = ParentToolReference(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="parent-call",
    )
    conversation = reconstruct_conversation(
        [
            _event(
                "1-0",
                ConversationCreatedPayload(
                    agent="unimportable:agent", parent=parent
                ),
            ),
            _event("2-0", _queued()),
            _event("3-0", _request("call-1")),
        ],
        conversation_id="conversation",
    )
    machine = conversation.turn_state["turn-1"]
    before = deepcopy(vars(machine))
    delegation = _delegation()
    settings, queued, prepared = conversation.prepare_subagent(delegation)
    assert settings.parent == ParentToolReference(
        conversation_id="conversation",
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
    )
    assert settings.deps == {"key": "value"}
    assert queued == TurnQueuedPayload(
        turn_id="child-turn",
        message="child prompt",
        parent=settings.parent,
    )
    assert prepared.dispatch("4-0") == (
        ConversationPokePayload(conversation_id="child", turn_id="child-turn"),
    )
    assert vars(machine) == before
    assert conversation.last_event_id == "3-0"

    conversation.apply(_event("4-0", delegation))
    action = _child_action()
    assert conversation.dispatch_status(action) == "ready"
    result = conversation.child_outcome(
        action, _event("42-7", _completed("child-turn"))
    )
    assert isinstance(result, ToolResultPayload)
    assert conversation.accepts_result(result)
    conversation.prepare(result)
    assert conversation.resume_actions(result) == ()
    assert machine.results == {}
    conversation.apply(_event("5-0", result))
    assert not conversation.accepts_result(result)
    assert conversation.resume_actions(result) == (
        AgentResumePayload(
            conversation_id="conversation", turn_id="turn-1", step_id="step-1"
        ),
    )

    before = deepcopy(vars(machine))
    prepared = conversation.prepare(_completed())
    assert prepared.dispatch("99-3") == (
        SubAgentResultPayload(
            conversation_id="parent",
            turn_id="parent-turn",
            step_id="parent-step",
            tool_call_id="parent-call",
            child_conversation_id="conversation",
            child_turn_id="turn-1",
            child_terminal_event_id="99-3",
        ),
    )
    assert vars(machine) == before
    assert conversation.last_event_id == "5-0"
    forbidden.assert_not_called()


@pytest.mark.parametrize("fails", [False, True])
def test_agent_turns_overlap_and_route_each_result_independently(
    fails,
) -> None:
    parents = [
        ParentToolReference(
            conversation_id="editor",
            turn_id=f"editor-turn-{index}",
            step_id="step",
            tool_call_id=call_id,
        )
        for index, call_id in enumerate(("first", "second"), 1)
    ]
    conversation = reconstruct_conversation(
        [
            _event(
                "1-0",
                ConversationCreatedPayload(
                    agent="example:writer",
                    parent=parents[0],
                ),
            ),
            _event(
                "2-0",
                TurnQueuedPayload(
                    turn_id="turn-1", message="draft", parent=parents[0]
                ),
            ),
            _event("3-0", _request("call-1")),
        ],
        conversation_id="writer",
    )
    queued = TurnQueuedPayload(
        turn_id="turn-2", message="revise", parent=parents[1]
    )
    poke = ConversationPokePayload(conversation_id="writer", turn_id="turn-2")
    assert conversation.prepare(queued).emissions == (poke,)
    conversation.apply(_event("4-0", queued))
    assert conversation.dispatch_status(poke) == "ready"
    if not fails:
        conversation.apply(_event("5-0", _result()))
        assert conversation.dispatch_status(poke) == "ready"
    terminal = (
        TurnFailedPayload(turn_id="turn-1", error="failed")
        if fails
        else _completed()
    )
    prepared = conversation.prepare(terminal)
    assert len(prepared.emissions) == 1
    assert prepared.dispatch("6-0")[0].tool_call_id == "first"
    conversation.apply(_event("6-0", terminal))
    second = conversation.turn_state["turn-2"]
    assert conversation.dispatch_status(poke) == "ready"
    assert second.history == []
    assert second.usage is None
    assert (
        conversation.prepare(_completed("turn-2"))
        .dispatch("7-0")[0]
        .tool_call_id
        == "second"
    )


def test_agent_registry_survives_later_steps_and_parent_turns() -> None:
    delegation = _delegation().model_copy(
        update={"name": "alice", "persistent": True}
    )
    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:editor")),
            _event("2-0", _queued()),
            _event("3-0", _request("call-1")),
            _event("4-0", delegation),
            _event("5-0", _result()),
            _event("6-0", _request("call-2", step_id="step-2")),
            _event("7-0", _result("call-2", step_id="step-2")),
            _event("8-0", _completed()),
            _event("9-0", _queued("turn-2")),
        ],
        conversation_id="editor",
    )

    assert conversation.agent_by_name == {"alice": "child"}
    assert conversation.agent_spec("alice") == delegation
    assert conversation.turn_state["turn-1"].delegations == {}


def test_delegation_settings_ignore_identity_but_event_equality_does_not() -> None:
    original = _delegation()
    follow_up = original.model_copy(
        update={
            "turn_id": "another-parent-turn",
            "step_id": "another-step",
            "tool_call_id": "another-call",
            "child_turn_id": "another-agent-turn",
            "prompt": "revise",
            "name": "alice",
            "persistent": True,
        }
    )

    assert original.has_same_settings(follow_up)
    assert original != follow_up


@pytest.mark.parametrize(
    "changes",
    [
        {"agent_path": "example:other"},
        {"model": "another-model"},
        {"deps": {"key": "another-value"}},
    ],
)
def test_registration_rejects_changed_agent_settings(changes) -> None:
    original = _delegation().model_copy(
        update={"name": "alice", "persistent": True}
    )
    conversation = reconstruct_conversation(
        [
            _event("1-0", ConversationCreatedPayload(agent="example:editor")),
            _event("2-0", _queued()),
            _event("3-0", _request("call-1", "call-2")),
            _event("4-0", original),
        ],
        conversation_id="editor",
    )
    follow_up = original.model_copy(
        update={
            "tool_call_id": "call-2",
            "child_turn_id": "another-agent-turn",
            "prompt": "revise",
            **changes,
        }
    )

    with pytest.raises(ValueError, match="settings cannot change"):
        conversation.prepare(follow_up)


def test_conversation_settings_ignore_parent_routing() -> None:
    original = ConversationCreatedPayload(agent="example:writer", model="test")
    routed = original.model_copy(
        update={
            "parent": ParentToolReference(
                conversation_id="editor",
                turn_id="turn",
                step_id="step",
                tool_call_id="call",
            )
        }
    )

    assert original.has_same_settings(routed)
    assert original != routed
    assert not original.has_same_settings(
        original.model_copy(update={"model": "another-model"})
    )
