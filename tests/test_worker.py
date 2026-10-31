"""Checks fan-out, fan-in, and nonblocking child turns."""

from collections.abc import Mapping
from typing import Any
from unittest.mock import Mock

from pydantic import TypeAdapter
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    UserPromptPart,
)
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.worker import Worker
from fallingwater.agent_proxy import AgentRunResult, SubAgentRequest
from fallingwater.turn_state import reconstruct_turn
from fallingwater.stream_models import (
    ConversationCreatedPayload,
    ConversationPayload,
    Action,
    Event,
    InvalidDispatchAction,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
)
from fallingwater.stream_utils import (
    decode_dispatch_action,
    decode_conversation_event,
    encode_conversation_event,
)


class MemoryRedis(Redis):
    """Small multi-stream fake for worker tests."""

    def __init__(self, entries: list[tuple[str, Mapping[Any, Any]]]):
        self.streams: dict[str, list[tuple[str, Mapping[Any, Any]]]] = {
            "fw:conversation:abc:events": list(entries)
        }

    def xrange(self, stream: str, min: str = "-", count: int = 100):
        entries = self.streams.get(stream, [])
        cursor = min.removeprefix("(")
        exclusive = min.startswith("(")
        selected = [
            entry
            for entry in entries
            if min == "-"
            or (entry[0] > cursor if exclusive else entry[0] >= cursor)
        ]
        return selected[:count]

    def xadd(self, stream: str, fields: Mapping[Any, Any]):
        entries = self.streams.setdefault(stream, [])
        entry_id = f"{len(entries) + 1}-0"
        entries.append((entry_id, dict(fields)))
        return entry_id


def _entry(entry_id: str, event_type: Event, payload: ConversationPayload):
    return (entry_id, encode_conversation_event(event_type, payload))


def _messages(prompt: str) -> list[ModelRequest]:
    return [ModelRequest(parts=[UserPromptPart(content=prompt)])]


def _base_entries(
    conversation_id: str = "abc",
    *,
    agent_path: str = "example:agent",
    deps: dict[str, Any] | None = None,
    prompt: str = "hello",
    turn_id: str = "turn-1",
):
    return [
        _entry(
            "1-0",
            Event.CONVERSATION_CREATED,
            ConversationCreatedPayload(
                agent=agent_path, model="test", deps=deps or {}
            ),
        ),
        _entry(
            "2-0",
            Event.TURN_QUEUED,
            TurnQueuedPayload(turn_id=turn_id, message=prompt),
        ),
    ]


def _tool_request(calls: list[ToolCallPart], *, step_id: str = "step-1"):
    requests = DeferredToolRequests(calls=calls)
    messages = [ModelResponse(parts=[*calls])]
    return ToolRequestedPayload(
        turn_id="turn-1",
        step_id=step_id,
        requests=TypeAdapter(DeferredToolRequests).dump_python(
            requests, mode="json"
        ),
        history=ModelMessagesTypeAdapter.dump_python(messages, mode="json"),
        usage=RunUsage(),
    )


def _worker(redis: MemoryRedis) -> Worker:
    return Worker(redis)


def _process(worker: Worker, fields: dict[str, str]) -> bool:
    action = decode_dispatch_action("1-0", fields)
    assert not isinstance(action, InvalidDispatchAction)
    cnv_stream = worker.dsp_stream.conversation_stream(action.conversation_id)
    conversation = reconstruct_turn(
        cnv_stream.iter_events(),
        action.turn_id,
        conversation_stream=cnv_stream.name,
    )
    return worker.process(action, cnv_stream, conversation)


def _events(redis: MemoryRedis, conversation_id: str = "abc"):
    stream = f"fw:conversation:{conversation_id}:events"
    return [
        decode_conversation_event(fields) for _, fields in redis.streams[stream]
    ]


def _dispatch(redis: MemoryRedis):
    return [fields for _, fields in redis.streams.get("fw:dispatch", [])]


def test_turn_restores_prior_usage_and_latest_step_checkpoint() -> None:
    history = _messages("previous prompt")
    previous_usage = RunUsage(requests=4, input_tokens=100)
    redis = MemoryRedis(
        [
            *_base_entries(),
            _entry(
                "3-0",
                Event.TURN_COMPLETED,
                TurnCompletedPayload(
                    turn_id="turn-1",
                    message="previous answer",
                    history=ModelMessagesTypeAdapter.dump_python(
                        history, mode="json"
                    ),
                    usage=previous_usage,
                ),
            ),
            _entry(
                "4-0",
                Event.TURN_QUEUED,
                TurnQueuedPayload(turn_id="turn-2", message="next prompt"),
            ),
        ]
    )
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    conversation = reconstruct_turn(
        stream.iter_events(), "turn-2", conversation_stream=stream.name
    )
    assert conversation.history == history
    assert conversation.usage == previous_usage

    current_usage = RunUsage(requests=5, input_tokens=120)
    request = _tool_request(
        [ToolCallPart("multiply", {"left": 2, "right": 3}, tool_call_id="call-1")]
    ).model_copy(update={"turn_id": "turn-2", "usage": current_usage})
    stream.append_event(Event.TURN_TOOL_REQUESTED, request)

    resumed = reconstruct_turn(
        stream.iter_events(), "turn-2", conversation_stream=stream.name
    )
    assert resumed.history == history
    assert resumed.usage == current_usage


def test_tool_calls_fan_out_and_resume_after_out_of_order_results(
    monkeypatch,
) -> None:
    seen: list[tuple[int, int]] = []
    agent = Agent(TestModel(), name="multiply-agent")

    @agent.tool_plain
    def multiply(left: int, right: int) -> int:
        seen.append((left, right))
        return left * right

    requests = _tool_request(
        [
            ToolCallPart(
                "multiply", {"left": 6, "right": 7}, tool_call_id="call-1"
            ),
            ToolCallPart(
                "multiply", {"left": 2, "right": 3}, tool_call_id="call-2"
            ),
        ]
    )
    request_result = AgentRunResult(
        output=TypeAdapter(DeferredToolRequests).validate_python(
            requests.requests
        ),
        usage=RunUsage(),
        new_messages=_messages("requested multiply"),
        tool_contexts={},
    )
    final_result = AgentRunResult(
        output="42",
        usage=RunUsage(),
        new_messages=_messages("final answer"),
        tool_contexts={},
    )
    run_agent = Mock(side_effect=[request_result, final_result])
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis)

    assert _process(
        worker,
        {
            "type": Action.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    actions = _dispatch(redis)
    assert [action["type"] for action in actions] == [
        Action.TOOL_CALL
    ] * 2
    assert {action["tool_call_id"] for action in actions} == {
        "call-1",
        "call-2",
    }
    assert seen == []

    # Complete the second call first; only the unique final result queues resume.
    assert _process(worker, actions[1])
    assert _process(worker, actions[1])
    assert len(_dispatch(redis)) == 2
    assert _dispatch(redis)[-1]["type"] == Action.TOOL_CALL
    assert _process(worker, actions[0])
    resume = _dispatch(redis)[-1]
    assert resume["type"] == Action.AGENT_RESUME
    assert seen == [(2, 3), (6, 7)]

    assert _process(worker, resume)
    events = _events(redis)
    assert [event.type for event in events[-4:]] == [
        Event.TURN_TOOL_REQUESTED,
        Event.TURN_TOOL_RESULT,
        Event.TURN_TOOL_RESULT,
        Event.TURN_COMPLETED,
    ]
    result_payloads = [event.payload for event in events[-3:-1]]
    assert all(
        isinstance(payload, ToolResultPayload) for payload in result_payloads
    )
    assert {payload.result for payload in result_payloads} == {42, 6}
    assert isinstance(events[-1].payload, TurnCompletedPayload)
    assert events[-1].payload.message == "42"
    assert run_agent.call_count == 2
    assert run_agent.call_args_list[1].kwargs["deferred_tool_results"]


def test_tool_action_recovery_skips_saved_result_and_runs_missing_call(
    monkeypatch,
) -> None:
    contexts: list[tuple[Any, ...]] = []
    agent = Agent(TestModel(), deps_type=dict[str, str], name="context-agent")

    @agent.tool
    def contextual(ctx: RunContext[dict[str, str]], value: int) -> str:
        contexts.append(
            (ctx.deps, ctx.conversation_id, ctx.tool_call_id, ctx.tool_name)
        )
        return str(value)

    request = _tool_request(
        [
            ToolCallPart("contextual", {"value": 1}, tool_call_id="call-1"),
            ToolCallPart("contextual", {"value": 2}, tool_call_id="call-2"),
        ]
    )
    redis = MemoryRedis(
        [
            *_base_entries(deps={"answer": "dependency"}),
            _entry("3-0", Event.TURN_TOOL_REQUESTED, request),
            _entry(
                "4-0",
                Event.TURN_TOOL_RESULT,
                ToolResultPayload(
                    turn_id="turn-1",
                    step_id="step-1",
                    tool_call_id="call-1",
                    result="1",
                ),
            ),
        ]
    )
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    worker = _worker(redis)

    assert _process(
        worker,
        {
            "type": Action.TOOL_CALL,
            "turn_id": "turn-1",
            "conversation_id": "abc",
            "step_id": "step-1",
            "tool_call_id": "call-2",
        },
    )
    assert contexts == [
        ({"answer": "dependency"}, "abc", "call-2", "contextual")
    ]
    assert _dispatch(redis)[-1]["type"] == Action.AGENT_RESUME


def test_result_arriving_during_save_still_queues_resume(monkeypatch) -> None:
    request = _tool_request(
        [
            ToolCallPart("unused", {}, tool_call_id="call-1"),
            ToolCallPart("unused", {}, tool_call_id="call-2"),
        ]
    )
    redis = MemoryRedis(
        [*_base_entries(), _entry("3-0", Event.TURN_TOOL_REQUESTED, request)]
    )
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    append_event = stream.append_event

    def append_with_sibling(event_type, payload):
        entry_id = append_event(event_type, payload)
        append_event(
            Event.TURN_TOOL_RESULT,
            ToolResultPayload(
                turn_id="turn-1",
                step_id="step-1",
                tool_call_id="call-2",
                result="second",
            ),
        )
        return entry_id

    monkeypatch.setattr(stream, "append_event", append_with_sibling)
    conversation = reconstruct_turn(
        stream.iter_events(), "turn-1", conversation_stream=stream.name
    )
    assert conversation.last_event_id == "3-0"
    iter_events = stream.iter_events
    after_ids: list[str] = []

    def tracked_events(*, after_id="0-0", page_size=100):
        after_ids.append(after_id)
        yield from iter_events(after_id=after_id, page_size=page_size)

    monkeypatch.setattr(stream, "iter_events", tracked_events)

    assert stream.save_tool_result(
        ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
            result="first",
        ),
        ["call-1", "call-2"],
        conversation,
    )
    assert after_ids == ["3-0", "3-0"]
    assert [entry["type"] for entry in _dispatch(redis)] == [
        Action.AGENT_RESUME
    ]


def test_subagent_runs_in_its_own_conversation_and_notifies_parent(
    monkeypatch,
) -> None:
    parent = Agent(TestModel(), name="parent")

    @parent.tool_plain
    def ask_child(prompt: str) -> SubAgentRequest:
        return SubAgentRequest(agent_path="example:child", prompt=prompt)

    child = Agent(TestModel(), name="child")
    deferred = _tool_request(
        [
            ToolCallPart(
                "ask_child",
                {"prompt": "child prompt"},
                tool_call_id="child-call",
            )
        ]
    )
    runs = Mock(
        side_effect=[
            AgentRunResult(
                output=TypeAdapter(DeferredToolRequests).validate_python(
                    deferred.requests
                ),
                usage=RunUsage(),
                new_messages=_messages("delegate"),
                tool_contexts={},
            ),
            AgentRunResult(
                output="child answer",
                usage=RunUsage(),
                new_messages=_messages("child answer"),
                tool_contexts={},
            ),
            AgentRunResult(
                output="parent answer",
                usage=RunUsage(),
                new_messages=_messages("parent answer"),
                tool_contexts={},
            ),
        ]
    )
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent",
        lambda path: parent if path == "example:agent" else child,
    )
    monkeypatch.setattr("fallingwater.worker.run", runs)
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis)

    assert _process(
        worker,
        {
            "type": Action.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    tool_action = _dispatch(redis)[0]
    assert _process(worker, tool_action)
    parent_events = _events(redis)
    subagent_event = parent_events[-1]
    assert subagent_event.type == Event.TURN_SUBAGENT_REQUESTED
    subagent = subagent_event.payload
    assert isinstance(subagent, SubAgentRequestedPayload)

    child_poke = next(
        action
        for action in _dispatch(redis)
        if action["type"] == Action.CONVERSATION_POKE
    )
    assert child_poke["conversation_id"] == subagent.child_conversation_id
    assert _process(worker, child_poke)
    child_events = _events(redis, subagent.child_conversation_id)
    assert child_events[-1].type == Event.TURN_COMPLETED
    parent_notify = _dispatch(redis)[-1]
    assert parent_notify["type"] == Action.SUBAGENT_RESULT
    child_entries = redis.xrange(
        worker.dsp_stream.conversation_stream(
            subagent.child_conversation_id
        ).name
    )
    assert parent_notify["child_terminal_event_id"] == child_entries[-1][0]

    assert _process(worker, parent_notify)
    resume = _dispatch(redis)[-1]
    assert resume["type"] == Action.AGENT_RESUME
    assert _process(worker, resume)
    assert _events(redis)[-1].payload.message == "parent answer"
    assert runs.call_count == 3


def test_duplicate_resume_after_completion_does_not_run_agent(
    monkeypatch,
) -> None:
    redis = MemoryRedis(
        [
            *_base_entries(),
            _entry(
                "3-0",
                Event.TURN_COMPLETED,
                TurnCompletedPayload(
                    turn_id="turn-1",
                    message="already done",
                    history=ModelMessagesTypeAdapter.dump_python(
                        _messages("already done"), mode="json"
                    ),
                    usage=RunUsage(),
                ),
            ),
        ]
    )
    run_agent = Mock()
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent", lambda _path: object()
    )
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    worker = _worker(redis)

    assert _process(
        worker,
        {
            "type": Action.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    run_agent.assert_not_called()


def test_agent_failure_is_recorded_as_turn_failed(monkeypatch) -> None:
    agent = Agent(TestModel(), name="failing-agent")
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr(
        "fallingwater.worker.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("agent failed")
        ),
    )
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis)
    action = decode_dispatch_action(
        "1-0",
        {
            "type": Action.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    monkeypatch.setattr(
        worker.dsp_stream, "create_group", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        worker.dsp_stream,
        "claim",
        lambda *_args, **_kwargs: ("0-0", [], []),
    )
    monkeypatch.setattr(
        worker.dsp_stream,
        "read_group",
        lambda *_args, **_kwargs: [action],
    )
    acknowledge = Mock(side_effect=lambda *_args: worker.stop())
    monkeypatch.setattr(worker.dsp_stream, "acknowledge", acknowledge)

    worker.run()
    failed = _events(redis)[-1].payload
    assert isinstance(failed, TurnFailedPayload)
    assert failed.error == "agent failed"
    acknowledge.assert_called_once_with(worker.group, "1-0")


def test_failed_child_fails_parent_turn(monkeypatch) -> None:
    parent_agent = Agent(TestModel(), name="parent")
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis)
    parent_stream = worker.dsp_stream.conversation_stream("abc")
    request = _tool_request(
        [ToolCallPart("delegate", {"prompt": "child"}, tool_call_id="call-1")]
    )
    redis.xadd(
        parent_stream.name,
        encode_conversation_event(Event.TURN_TOOL_REQUESTED, request),
    )
    parent_request = SubAgentRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        child_conversation_id="child-conversation",
        child_turn_id="child-turn",
    )
    assert parent_stream.queue_subagent(
        parent_request=parent_request,
        child_agent="example:child",
        child_model="test",
        child_deps={},
        child_prompt="child",
    )
    child_stream = worker.dsp_stream.conversation_stream("child-conversation")
    child_created = decode_conversation_event(
        redis.xrange(child_stream.name)[0][1]
    ).payload
    assert child_created.parent is not None
    child_stream.finish_turn(
        Event.TURN_FAILED,
        TurnFailedPayload(turn_id="child-turn", error="child failed"),
        parent=child_created.parent,
    )
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent", lambda _path: parent_agent
    )
    notification = _dispatch(redis)[-1]

    assert _process(worker, notification)
    failed = _events(redis)[-1].payload
    assert isinstance(failed, TurnFailedPayload)
    assert failed.error == "Subagent failed: child failed"
