"""Checks checkpointing and resuming agent turns from conversation events."""

from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock, Mock

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
from fallingwater.proxy import Proxy
from fallingwater.runner import AgentRunResult
from fallingwater.streams import (
    ConversationCreatedPayload,
    ConversationPayload,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    TurnToolRequestedPayload,
    TurnToolResultPayload,
    decode_conversation_event,
    encode_conversation_event,
)


class MemoryRedis:
    """Small stream fake supporting the proxy's event reads and writes."""

    def __init__(self, entries: list[tuple[str, Mapping[Any, Any]]]):
        self.entries = list(entries)

    def xrange(self, _stream: str, min: str = "-", count: int = 100):
        cursor = min.removeprefix("(")
        exclusive = min.startswith("(")
        selected = [
            entry
            for entry in self.entries
            if min == "-" or (entry[0] > cursor if exclusive else entry[0] >= cursor)
        ]
        return selected[:count]

    def xadd(self, _stream: str, fields: Mapping[Any, Any]):
        entry_id = f"{len(self.entries) + 1}-0"
        self.entries.append((entry_id, dict(fields)))
        return entry_id


def _entry(entry_id: str, event_type: EventType, payload: ConversationPayload):
    return (entry_id, encode_conversation_event(event_type, payload))


def _messages(prompt: str) -> list[ModelRequest]:
    return [ModelRequest(parts=[UserPromptPart(content=prompt)])]


def _base_entries(*, deps: dict[str, Any] | None = None):
    return [
        _entry(
            "1-0",
            EventType.CONVERSATION_CREATED,
            ConversationCreatedPayload(
                agent="example:agent", model="test", deps=deps or {}
            ),
        ),
        _entry(
            "2-0",
            EventType.TURN_QUEUED,
            TurnQueuedPayload(turn_id="turn-1", message="hello"),
        ),
    ]


def _tool_request(
    calls: list[ToolCallPart], *, step_id: str = "step-1"
) -> TurnToolRequestedPayload:
    requests = DeferredToolRequests(calls=calls)
    messages = [ModelResponse(parts=[*calls])]
    return TurnToolRequestedPayload(
        turn_id="turn-1",
        step_id=step_id,
        requests=TypeAdapter(DeferredToolRequests).dump_python(requests, mode="json"),
        history=ModelMessagesTypeAdapter.dump_python(messages, mode="json"),
        usage=RunUsage(),
    )


def _redis_with(entries):
    memory = MemoryRedis(entries)
    redis = MagicMock(spec=Redis)
    redis.xrange.side_effect = memory.xrange
    redis.xadd.side_effect = memory.xadd
    return redis, memory


def test_proxy_persists_each_tool_result_and_completes_turn(monkeypatch) -> None:
    calls: list[tuple[int, int]] = []
    agent = Agent(TestModel(), name="multiply-agent")

    @agent.tool_plain
    def multiply(left: int, right: int) -> int:
        calls.append((left, right))
        return left * right

    request = _tool_request(
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
        output=TypeAdapter(DeferredToolRequests).validate_python(request.requests),
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
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.proxy.run", run_agent)
    redis, memory = _redis_with(_base_entries())
    proxy = Proxy(redis)
    fields = {"turn_id": "turn-1", "conversation_id": "abc"}

    assert not proxy.process(fields)
    events = [decode_conversation_event(fields) for _, fields in memory.entries]
    assert events[-1].type == EventType.TURN_TOOL_REQUESTED
    assert calls == []
    assert run_agent.call_count == 1

    assert not proxy.process(fields)
    events = [decode_conversation_event(fields) for _, fields in memory.entries]
    assert events[-1].type == EventType.TURN_TOOL_RESULT
    assert calls == [(6, 7)]
    assert run_agent.call_count == 1

    assert not proxy.process(fields)
    events = [decode_conversation_event(fields) for _, fields in memory.entries]
    assert events[-1].type == EventType.TURN_TOOL_RESULT
    assert calls == [(6, 7), (2, 3)]
    assert run_agent.call_count == 1

    assert proxy.process(fields)

    events = [decode_conversation_event(fields) for _, fields in memory.entries]
    assert [event.type for event in events[-4:]] == [
        EventType.TURN_TOOL_REQUESTED,
        EventType.TURN_TOOL_RESULT,
        EventType.TURN_TOOL_RESULT,
        EventType.TURN_COMPLETED,
    ]
    result_payloads = [event.payload for event in events[-3:-1]]
    assert all(
        isinstance(payload, TurnToolResultPayload)
        for payload in result_payloads
    )
    assert [payload.result for payload in result_payloads] == [42, 6]
    completed = events[-1].payload
    assert isinstance(completed, TurnCompletedPayload)
    assert completed.message == "42"
    assert len(completed.history) == 2
    assert run_agent.call_count == 2
    assert run_agent.call_args_list[1].kwargs["deferred_tool_results"]


def test_proxy_resumes_missing_tool_call_with_rebuilt_context(monkeypatch) -> None:
    contexts: list[tuple[Any, ...]] = []
    agent = Agent(
        TestModel(),
        deps_type=dict[str, str],
        name="context-agent",
    )

    @agent.tool
    def contextual(ctx: RunContext[dict[str, str]], value: int) -> str:
        contexts.append(
            (ctx.deps, ctx.conversation_id, ctx.tool_call_id, ctx.tool_name)
        )
        return str(value)

    calls = [
        ToolCallPart("contextual", {"value": 1}, tool_call_id="call-1"),
        ToolCallPart("contextual", {"value": 2}, tool_call_id="call-2"),
    ]
    request = _tool_request(calls)
    redis, memory = _redis_with(
        [
            *_base_entries(deps={"answer": "dependency"}),
            _entry("3-0", EventType.TURN_TOOL_REQUESTED, request),
            _entry(
                "4-0",
                EventType.TURN_TOOL_RESULT,
                TurnToolResultPayload(
                    turn_id="turn-1",
                    step_id="step-1",
                    tool_call_id="call-1",
                    result="1",
                ),
            ),
        ]
    )
    final_result = AgentRunResult(
        output="done",
        usage=RunUsage(),
        new_messages=_messages("done"),
        tool_contexts={},
    )
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)
    monkeypatch.setattr(
        "fallingwater.proxy.run", lambda *_args, **_kwargs: final_result
    )

    proxy = Proxy(redis)
    fields = {"turn_id": "turn-1", "conversation_id": "abc"}
    assert not proxy.process(fields)
    assert proxy.process(fields)

    assert contexts == [({"answer": "dependency"}, "abc", "call-2", "contextual")]
    events = [decode_conversation_event(fields) for _, fields in memory.entries]
    completed = events[-1].payload
    assert isinstance(completed, TurnCompletedPayload)
    assert completed.message == "done"


def test_stopping_after_request_leaves_turn_recoverable(monkeypatch) -> None:
    calls: list[int] = []
    agent = Agent(TestModel(), name="stop-agent")

    @agent.tool_plain
    def use_tool(value: int) -> int:
        calls.append(value)
        return value

    request = _tool_request(
        [ToolCallPart("use_tool", {"value": 3}, tool_call_id="call-1")]
    )
    request_result = AgentRunResult(
        output=TypeAdapter(DeferredToolRequests).validate_python(request.requests),
        usage=RunUsage(),
        new_messages=_messages("request"),
        tool_contexts={},
    )
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)
    monkeypatch.setattr(
        "fallingwater.proxy.run", lambda *_args, **_kwargs: request_result
    )
    redis, memory = _redis_with(_base_entries())
    proxy = Proxy(redis)
    fields = {"turn_id": "turn-1", "conversation_id": "abc"}
    assert not proxy.process(fields)
    proxy.stop()
    assert calls == []
    assert (
        decode_conversation_event(memory.entries[-1][1]).type
        == EventType.TURN_TOOL_REQUESTED
    )

    final_result = AgentRunResult(
        output="finished",
        usage=RunUsage(),
        new_messages=_messages("finished"),
        tool_contexts={},
    )
    monkeypatch.setattr(
        "fallingwater.proxy.run", lambda *_args, **_kwargs: final_result
    )
    restarted_proxy = Proxy(redis)
    assert not restarted_proxy.process(fields)
    assert restarted_proxy.process(fields)
    assert calls == [3]


def test_duplicate_dispatch_after_completion_does_not_rerun(monkeypatch) -> None:
    redis, _memory = _redis_with(
        [
            *_base_entries(),
            _entry(
                "3-0",
                EventType.TURN_COMPLETED,
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
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: object())
    monkeypatch.setattr("fallingwater.proxy.run", run_agent)

    assert Proxy(redis).process({"turn_id": "turn-1", "conversation_id": "abc"})
    run_agent.assert_not_called()


def test_agent_failure_is_recorded_as_turn_failed(monkeypatch) -> None:
    agent = object()
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)
    monkeypatch.setattr(
        "fallingwater.proxy.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("agent failed")),
    )
    redis, memory = _redis_with(_base_entries())

    assert Proxy(redis).process({"turn_id": "turn-1", "conversation_id": "abc"})
    failed = decode_conversation_event(memory.entries[-1][1]).payload
    assert isinstance(failed, TurnFailedPayload)
    assert failed.error == "agent failed"


def test_proxy_run_repeats_steps_for_the_same_dispatch(monkeypatch) -> None:
    redis = MagicMock(spec=Redis)
    proxy = Proxy(redis)
    fields = {
        "type": EventType.CONVERSATION_POKE,
        "turn_id": "turn-1",
        "conversation_id": "abc",
    }
    proxy.client.create_group = Mock()
    proxy.client.claim_dispatch = Mock(return_value=("0-0", [], []))
    proxy.client.read_dispatch_group = Mock(return_value=[("1-0", fields)])
    proxy.client.acknowledge = Mock()

    outcomes = iter([False, False, True])

    def process_step(_fields):
        complete = next(outcomes)
        if complete:
            proxy.stop()
        return complete

    monkeypatch.setattr(proxy, "process", process_step)
    proxy.run()

    assert proxy.client.read_dispatch_group.call_count == 1
    assert proxy.client.acknowledge.call_count == 1
    assert proxy.client.acknowledge.call_args.args == (
        proxy.dispatch_stream,
        proxy.group,
        "1-0",
    )
