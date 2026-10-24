"""Checks fan-out, fan-in, and nonblocking child turns."""

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from threading import Barrier, RLock
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
from redis.exceptions import WatchError

from fallingwater.worker import Worker
from fallingwater.agent_proxy import AgentRunResult, SubAgentRequest
from fallingwater.stream import ConversationStream
from fallingwater.turn_state import reconstruct_turn
from fallingwater.stream_utils import (
    ConversationCreatedPayload,
    ConversationPayload,
    DispatchType,
    EventType,
    InvalidDispatchAction,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    decode_dispatch_action,
    decode_conversation_event,
    encode_conversation_event,
)


class MemoryPipeline:
    def __init__(
        self, redis: "MemoryRedis", *, transaction: bool = False
    ) -> None:
        self.redis = redis
        self.watched: dict[str, int] = {}
        self.commands: list[tuple[str, Mapping[Any, Any]]] = []
        self.in_multi = transaction
        self.barrier: Barrier | None = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.watched.clear()

    def watch(self, *streams: str):
        self.watched = {
            stream: self.redis.versions.get(stream, 0) for stream in streams
        }
        with self.redis.guard:
            if (
                self.redis.watch_barrier is not None
                and self.redis.barrier_watch_count < 2
            ):
                self.redis.barrier_watch_count += 1
                self.barrier = self.redis.watch_barrier

    def unwatch(self):
        self.watched.clear()

    def xrange(self, stream: str, min: str = "-", count: int = 100):
        entries = self.redis.xrange(stream, min=min, count=count)
        if self.barrier is not None:
            self.barrier.wait(timeout=2)
            self.barrier = None
        return entries

    def multi(self):
        self.in_multi = True

    def xadd(self, stream: str, fields: Mapping[Any, Any]):
        if self.in_multi:
            self.commands.append((stream, fields))
            return self
        return self.redis.xadd(stream, fields)

    def execute(self):
        with self.redis.guard:
            if any(
                self.redis.versions.get(stream, 0) != version
                for stream, version in self.watched.items()
            ):
                raise WatchError
            result = [
                self.redis.xadd(stream, fields)
                for stream, fields in self.commands
            ]
        self.commands.clear()
        self.in_multi = False
        return result


class MemoryRedis(Redis):
    """Small multi-stream fake with transactional writes and WATCH checks."""

    def __init__(self, entries: list[tuple[str, Mapping[Any, Any]]]):
        self.streams: dict[str, list[tuple[str, Mapping[Any, Any]]]] = {
            "fw:conversation:abc:events": list(entries)
        }
        self.versions: dict[str, int] = {}
        self.guard = RLock()
        self.watch_barrier: Barrier | None = None
        self.barrier_watch_count = 0

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
        with self.guard:
            entries = self.streams.setdefault(stream, [])
            entry_id = f"{len(entries) + 1}-0"
            entries.append((entry_id, dict(fields)))
            self.versions[stream] = self.versions.get(stream, 0) + 1
            return entry_id

    def pipeline(self, *, transaction: bool = False, **_kwargs):
        return MemoryPipeline(self, transaction=transaction)


def _entry(entry_id: str, event_type: EventType, payload: ConversationPayload):
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
            EventType.CONVERSATION_CREATED,
            ConversationCreatedPayload(
                agent=agent_path, model="test", deps=deps or {}
            ),
        ),
        _entry(
            "2-0",
            EventType.TURN_QUEUED,
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


def _worker(redis: MemoryRedis, monkeypatch) -> Worker:
    worker = Worker(redis)
    monkeypatch.setattr(
        ConversationStream,
        "lock_agent_run",
        lambda *_args, **_kwargs: nullcontext(Mock()),
    )
    return worker


def _process(worker: Worker, fields: dict[str, str]) -> bool:
    action = decode_dispatch_action("1-0", fields)
    assert not isinstance(action, InvalidDispatchAction)
    cnv_stream = worker.dsp_stream.conversation_stream(action.conversation_id)
    conversation = reconstruct_turn(
        worker._conversation_events(cnv_stream),
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
    worker = _worker(redis, monkeypatch)

    assert _process(
        worker,
        {
            "type": DispatchType.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    actions = _dispatch(redis)
    assert [action["type"] for action in actions] == [
        DispatchType.TOOL_CALL
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
    assert _dispatch(redis)[-1]["type"] == DispatchType.TOOL_CALL
    assert _process(worker, actions[0])
    resume = _dispatch(redis)[-1]
    assert resume["type"] == DispatchType.AGENT_RESUME
    assert seen == [(2, 3), (6, 7)]

    assert _process(worker, resume)
    events = _events(redis)
    assert [event.type for event in events[-4:]] == [
        EventType.TURN_TOOL_REQUESTED,
        EventType.TURN_TOOL_RESULT,
        EventType.TURN_TOOL_RESULT,
        EventType.TURN_COMPLETED,
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
            _entry("3-0", EventType.TURN_TOOL_REQUESTED, request),
            _entry(
                "4-0",
                EventType.TURN_TOOL_RESULT,
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
    worker = _worker(redis, monkeypatch)

    assert _process(
        worker,
        {
            "type": DispatchType.TOOL_CALL,
            "turn_id": "turn-1",
            "conversation_id": "abc",
            "step_id": "step-1",
            "tool_call_id": "call-2",
        },
    )
    assert contexts == [
        ({"answer": "dependency"}, "abc", "call-2", "contextual")
    ]
    assert _dispatch(redis)[-1]["type"] == DispatchType.AGENT_RESUME


def test_concurrent_tool_results_queue_one_resume() -> None:
    request = _tool_request(
        [
            ToolCallPart("unused", {}, tool_call_id="call-1"),
            ToolCallPart("unused", {}, tool_call_id="call-2"),
        ]
    )
    redis = MemoryRedis(_base_entries())
    redis.xadd(
        "fw:conversation:abc:events",
        encode_conversation_event(EventType.TURN_TOOL_REQUESTED, request),
    )
    redis.watch_barrier = Barrier(2)
    streams = [
        Worker(redis).dsp_stream.conversation_stream("abc") for _ in range(2)
    ]
    payloads = [
        ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id=call_id,
            result=call_id,
        )
        for call_id in ("call-1", "call-2")
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        saved = list(
            executor.map(
                lambda args: args[0].save_tool_result(
                    args[1],
                    ["call-1", "call-2"],
                ),
                zip(streams, payloads, strict=True),
            )
        )

    assert all(saved)
    dispatch = _dispatch(redis)
    assert [entry["type"] for entry in dispatch] == [DispatchType.AGENT_RESUME]
    results = [
        event
        for event in _events(redis)
        if event.type == EventType.TURN_TOOL_RESULT
    ]
    assert {event.payload.tool_call_id for event in results} == {
        "call-1",
        "call-2",
    }


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
    worker = _worker(redis, monkeypatch)

    assert _process(
        worker,
        {
            "type": DispatchType.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    tool_action = _dispatch(redis)[0]
    assert _process(worker, tool_action)
    parent_events = _events(redis)
    subagent_event = parent_events[-1]
    assert subagent_event.type == EventType.TURN_SUBAGENT_REQUESTED
    subagent = subagent_event.payload
    assert isinstance(subagent, SubAgentRequestedPayload)

    child_poke = next(
        action
        for action in _dispatch(redis)
        if action["type"] == DispatchType.CONVERSATION_POKE
    )
    assert child_poke["conversation_id"] == subagent.conversation_id
    assert _process(worker, child_poke)
    child_events = _events(redis, subagent.conversation_id)
    assert child_events[-1].type == EventType.TURN_COMPLETED
    parent_notify = _dispatch(redis)[-1]
    assert parent_notify["type"] == DispatchType.SUBAGENT_RESULT

    assert _process(worker, parent_notify)
    resume = _dispatch(redis)[-1]
    assert resume["type"] == DispatchType.AGENT_RESUME
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
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent", lambda _path: object()
    )
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    worker = _worker(redis, monkeypatch)

    assert _process(
        worker,
        {
            "type": DispatchType.CONVERSATION_POKE,
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
    worker = _worker(redis, monkeypatch)
    action = decode_dispatch_action(
        "1-0",
        {
            "type": DispatchType.CONVERSATION_POKE,
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
        lambda *_args, **_kwargs: [("1-0", action)],
    )
    acknowledge = Mock(side_effect=lambda *_args: worker.stop())
    monkeypatch.setattr(worker.dsp_stream, "acknowledge", acknowledge)

    worker.run()
    failed = _events(redis)[-1].payload
    assert isinstance(failed, TurnFailedPayload)
    assert failed.error == "agent failed"
    acknowledge.assert_called_once_with(worker.group, "1-0")


def test_duplicate_agent_resume_waits_for_existing_run_lease(
    monkeypatch,
) -> None:
    agent = Agent(TestModel(), name="leased-agent")
    run_agent = Mock()
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis, monkeypatch)
    monkeypatch.setattr(
        ConversationStream,
        "lock_agent_run",
        lambda *_args, **_kwargs: nullcontext(None),
    )

    assert not _process(
        worker,
        {
            "type": DispatchType.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )
    run_agent.assert_not_called()


def test_failed_child_fails_parent_turn(monkeypatch) -> None:
    parent_agent = Agent(TestModel(), name="parent")
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis, monkeypatch)
    parent_stream = worker.dsp_stream.conversation_stream("abc")
    request = _tool_request(
        [ToolCallPart("delegate", {"prompt": "child"}, tool_call_id="call-1")]
    )
    redis.xadd(
        parent_stream.name,
        encode_conversation_event(EventType.TURN_TOOL_REQUESTED, request),
    )
    parent_request = SubAgentRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        conversation_id="child-conversation",
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
        EventType.TURN_FAILED,
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
