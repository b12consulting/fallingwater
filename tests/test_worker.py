"""Checks fan-out, fan-in, and nonblocking child turns."""

import shutil
import subprocess
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from time import monotonic, sleep
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
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
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    RedisError,
    ResponseError,
)

from fallingwater.worker import Worker
from fallingwater.agent_proxy import AgentRunResult, SubAgentRequest
from fallingwater.conversation import Conversation, reconstruct_conversation
from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationPayload,
    Action,
    Event,
    InvalidDispatchEvent,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
)
from fallingwater.stream_utils import (
    decode_dispatch_event,
    decode_conversation_event,
    encode_conversation_event,
)


class MemoryRedis(Redis):
    """Small multi-stream fake for worker tests."""

    def __init__(self, entries: list[tuple[str, Mapping[Any, Any]]]):
        self.streams: dict[str, list[tuple[str, Mapping[Any, Any]]]] = {
            "fw:conversation:abc:events": list(entries)
        }

    def xrange(
        self,
        stream: str,
        min: str = "-",
        max: str = "+",
        count: int | None = 100,
    ):
        entries = self.streams.get(stream, [])
        cursor = min.removeprefix("(")
        exclusive = min.startswith("(")
        upper = max.removeprefix("(")
        upper_exclusive = max.startswith("(")
        selected = [
            entry
            for entry in entries
            if (
                min == "-"
                or (entry[0] > cursor if exclusive else entry[0] >= cursor)
            )
            and (
                max == "+"
                or (entry[0] < upper if upper_exclusive else entry[0] <= upper)
            )
        ]
        return selected[:count]

    def xadd(self, stream: str, fields: Mapping[Any, Any]):
        entries = self.streams.setdefault(stream, [])
        entry_id = f"{len(entries) + 1}-0"
        entries.append((entry_id, dict(fields)))
        return entry_id

    def eval(self, script, numkeys, stream, after_id, event_type, payload):
        events = self.xrange(stream, min=f"({after_id}", count=100)
        entry_id = None
        if not events:
            fields = {"type": event_type, "payload": payload}
            entry_id = self.xadd(stream, fields)
            events = [(entry_id, fields)]
        return [
            entry_id,
            [
                [event_id, [value for pair in fields.items() for value in pair]]
                for event_id, fields in events
            ],
        ]


@pytest.fixture
def isolated_redis(tmp_path: Path) -> Iterator[Redis]:
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("redis-server is required for Lua integration tests")
    log = tmp_path / "redis.log"
    with TemporaryDirectory(prefix="fw-") as directory, subprocess.Popen(
        [
            executable,
            "--port",
            "0",
            "--unixsocket",
            str(Path(directory) / "redis.sock"),
            "--save",
            "",
            "--appendonly",
            "no",
            "--logfile",
            str(log),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    ) as process:
        redis = Redis(unix_socket_path=str(Path(directory) / "redis.sock"))
        try:
            deadline = monotonic() + 5
            while True:
                if process.poll() is not None:
                    raise RuntimeError(log.read_text())
                try:
                    redis.ping()
                    break
                except RedisConnectionError:
                    if monotonic() >= deadline:
                        raise
                    sleep(0.01)
            yield redis
        finally:
            redis.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


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
        step_messages=ModelMessagesTypeAdapter.dump_python(
            messages, mode="json"
        ),
        usage=RunUsage(),
    )


def _worker(redis: MemoryRedis) -> Worker:
    return Worker(redis)


def _process(worker: Worker, fields: dict[str, str]) -> bool:
    action = decode_dispatch_event("1-0", fields)
    assert not isinstance(action, InvalidDispatchEvent)
    cnv_stream = worker.dsp_stream.conversation_stream(
        action.payload.conversation_id
    )
    conversation = reconstruct_conversation(
        cnv_stream.iter_events(),
        conversation_id=action.payload.conversation_id,
    )
    return worker.process(action, cnv_stream, conversation)


def _events(redis: MemoryRedis, conversation_id: str = "abc"):
    stream = f"fw:conversation:{conversation_id}:events"
    return [
        decode_conversation_event(entry_id, fields)
        for entry_id, fields in redis.streams[stream]
    ]


def _dispatch(redis: MemoryRedis):
    return [fields for _, fields in redis.streams.get("fw:dispatch", [])]


def test_checkpoint_rejects_empty_tool_request_set_without_writing() -> None:
    redis = MemoryRedis(_base_entries())
    worker = Worker(redis)
    stream = worker.dsp_stream.conversation_stream("abc")
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    payload = ToolRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        requests={},
        step_messages=[],
        usage=RunUsage(),
    )

    with pytest.raises(ValueError, match="must contain a call"):
        worker.checkpoint(stream, conversation, payload)

    assert len(_events(redis)) == 2
    assert _dispatch(redis) == []


def test_agent_step_loads_conversation_dependencies(monkeypatch) -> None:
    agent = Agent(TestModel(), deps_type=dict[str, int])
    run_agent = Mock(
        return_value=AgentRunResult(
            output="answer",
            usage=RunUsage(),
            new_messages=_messages("answer"),
            tool_contexts={},
        )
    )
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    redis = MemoryRedis(_base_entries(deps={"value": 42}))

    assert _process(
        _worker(redis),
        {
            "type": Action.CONVERSATION_POKE,
            "turn_id": "turn-1",
            "conversation_id": "abc",
        },
    )

    assert run_agent.call_args.kwargs["deps"] == {"value": 42}
    assert _events(redis)[-1].type == Event.TURN_COMPLETED


@pytest.mark.parametrize("child_model", [None, "child-model"])
def test_subagent_request_persists_resolved_model(
    monkeypatch, child_model
) -> None:
    child = SubAgentRequest(
        agent_path="example:child", prompt="child prompt", model=child_model
    )
    monkeypatch.setattr(
        "fallingwater.worker.execute_tool", Mock(return_value=child)
    )
    redis = MemoryRedis(
        [
            *_base_entries(),
            _entry(
                "3-0",
                Event.TURN_TOOL_REQUESTED,
                _tool_request(
                    [ToolCallPart("ask_child", {}, tool_call_id="call-1")]
                ),
            ),
        ]
    )

    assert _process(
        _worker(redis),
        {
            "type": Action.TOOL_CALL,
            "conversation_id": "abc",
            "turn_id": "turn-1",
            "step_id": "step-1",
            "tool_call_id": "call-1",
        },
    )

    delegation = _events(redis)[-1].payload
    assert isinstance(delegation, SubAgentRequestedPayload)
    settings = _events(redis, delegation.child_conversation_id)[0].payload
    assert isinstance(settings, ConversationCreatedPayload)
    assert delegation.model == settings.model == (child_model or "test")


def test_queue_subagent_writes_recoverable_child_state_before_dispatch() -> None:
    redis = MagicMock(spec=Redis)
    redis.xadd.return_value = b"1-0"
    worker = Worker(redis)
    stream = worker.dsp_stream.conversation_stream("parent")
    conversation = Conversation(
        conversation_id="parent",
        settings=ConversationCreatedPayload(
            agent="example:parent", model="test"
        ),
    )
    conversation.apply(
        decode_conversation_event(
            "1-0",
            encode_conversation_event(
                Event.TURN_QUEUED,
                TurnQueuedPayload(turn_id="turn-1", message="hello"),
            ),
        )
    )
    conversation.apply(
        decode_conversation_event(
            "2-0",
            encode_conversation_event(
                Event.TURN_TOOL_REQUESTED,
                _tool_request(
                    [ToolCallPart("child", {}, tool_call_id="call-1")]
                ),
            ),
        )
    )
    request = SubAgentRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        child_conversation_id="child",
        child_turn_id="child-turn",
    )

    worker.queue_subagent(
        stream,
        conversation,
        request,
        SubAgentRequest(
            agent_path="example:child", model="test", prompt="hello"
        ),
    )

    calls = redis.xadd.call_args_list
    assert [call.args[0] for call in calls] == [
        "fw:conversation:child:events",
        "fw:conversation:child:events",
        "fw:conversation:parent:events",
        "fw:dispatch",
    ]
    assert [
        decode_conversation_event("1-0", call.args[1]).type
        for call in calls[:3]
    ] == [
        Event.CONVERSATION_CREATED,
        Event.TURN_QUEUED,
        Event.TURN_SUBAGENT_REQUESTED,
    ]
    assert calls[-1].args[1] == {
        "conversation_id": "child",
        "turn_id": "child-turn",
        "type": Action.CONVERSATION_POKE,
    }
    assert conversation.turn_state["turn-1"].delegations == {}


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
                    turn_messages=ModelMessagesTypeAdapter.dump_python(
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
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    assert conversation.turn_state["turn-2"].history == history
    assert conversation.turn_state["turn-2"].usage == previous_usage

    current_usage = RunUsage(requests=5, input_tokens=120)
    request = _tool_request(
        [
            ToolCallPart(
                "multiply", {"left": 2, "right": 3}, tool_call_id="call-1"
            )
        ]
    ).model_copy(update={"turn_id": "turn-2", "usage": current_usage})
    stream.append_event(Event.TURN_TOOL_REQUESTED, request)

    resumed = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    assert resumed.turn_state["turn-2"].history == history
    assert resumed.turn_state["turn-2"].usage == current_usage


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
    assert [action["type"] for action in actions] == [Action.TOOL_CALL] * 2
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
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    sibling_stream = Worker(redis).dsp_stream.conversation_stream("abc")
    sibling_conversation = reconstruct_conversation(
        sibling_stream.iter_events(), conversation_id="abc"
    )
    append_if_current = stream.append_if_current
    attempts = []

    def append_with_sibling(event_type, payload, *, after_id):
        attempts.append(after_id)
        if len(attempts) == 1:
            assert Worker(redis).save_tool_result(
                sibling_stream,
                sibling_conversation,
                ToolResultPayload(
                    turn_id="turn-1",
                    step_id="step-1",
                    tool_call_id="call-2",
                    result="second",
                ),
            )
        return append_if_current(event_type, payload, after_id=after_id)

    monkeypatch.setattr(stream, "append_if_current", append_with_sibling)
    assert conversation.last_event_id == "3-0"
    iter_events = stream.iter_events
    after_ids: list[str] = []

    def tracked_events(*, after_id="0-0", page_size=100):
        after_ids.append(after_id)
        yield from iter_events(after_id=after_id, page_size=page_size)

    monkeypatch.setattr(stream, "iter_events", tracked_events)

    assert Worker(redis).save_tool_result(
        stream,
        conversation,
        ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
            result="first",
        ),
    )
    assert after_ids == ["3-0"]
    assert attempts == ["3-0", "4-0"]
    assert [entry["type"] for entry in _dispatch(redis)] == [
        Action.AGENT_RESUME
    ]


def test_optimistic_append_returns_bounded_unseen_events(
    isolated_redis,
) -> None:
    stream = Worker(isolated_redis).dsp_stream.conversation_stream("abc")
    cursor = stream.append_payload(
        ConversationCreatedPayload(agent="example:agent")
    )
    queued = [
        TurnQueuedPayload(turn_id=f"turn-{index}", message="hello")
        for index in range(105)
    ]
    for payload in queued:
        stream.append_payload(payload)
    result = TurnQueuedPayload(turn_id="new-turn", message="new prompt")

    entry_id, events = stream.append_if_current(
        Event.TURN_QUEUED, result, after_id=cursor
    )
    assert entry_id is None
    assert [event.payload for event in events] == queued[:100]
    assert isolated_redis.xlen(stream.name) == 106

    entry_id, tail = stream.append_if_current(
        Event.TURN_QUEUED, result, after_id=events[-1].entry_id
    )
    assert entry_id is None
    assert [event.payload for event in tail] == queued[100:]

    entry_id, written = stream.append_if_current(
        Event.TURN_QUEUED, result, after_id=tail[-1].entry_id
    )
    assert entry_id is not None
    assert [event.payload for event in written] == [result]
    assert written[0].entry_id == entry_id
    assert isolated_redis.xlen(stream.name) == 107


def test_optimistic_append_propagates_redis_errors(isolated_redis) -> None:
    stream = Worker(isolated_redis).dsp_stream.conversation_stream("abc")
    isolated_redis.set(stream.name, "not a stream")

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        stream.append_if_current(
            Event.TURN_QUEUED,
            TurnQueuedPayload(turn_id="turn-1", message="hello"),
            after_id="0-0",
        )

    assert isolated_redis.get(stream.name) == b"not a stream"


@pytest.mark.parametrize("same_call", [False, True])
def test_concurrent_result_saves_emit_one_resume(
    isolated_redis, monkeypatch, same_call
) -> None:
    calls = ["call-1"] if same_call else ["call-1", "call-2"]
    request = _tool_request(
        [ToolCallPart("unused", {}, tool_call_id=call_id) for call_id in calls]
    )
    worker = Worker(isolated_redis)
    stream = worker.dsp_stream.conversation_stream("abc")
    for _, fields in [
        *_base_entries(),
        _entry("3-0", Event.TURN_TOOL_REQUESTED, request),
    ]:
        isolated_redis.xadd(stream.name, fields)
    streams = [worker.dsp_stream.conversation_stream("abc") for _ in range(2)]
    conversations = [
        reconstruct_conversation(item.iter_events(), conversation_id="abc")
        for item in streams
    ]
    first_attempt = Barrier(2)
    committed = Barrier(2)
    attempts = [0, 0]

    def wrap_append(index):
        append_if_current = streams[index].append_if_current

        def append(event_type, payload, *, after_id):
            attempts[index] += 1
            if attempts[index] == 1:
                first_attempt.wait(timeout=5)
            result = append_if_current(event_type, payload, after_id=after_id)
            if result[0] is not None and not same_call:
                # Both writes precede either worker's resumption decision.
                committed.wait(timeout=5)
            return result

        return append

    for index in range(2):
        monkeypatch.setattr(
            streams[index], "append_if_current", wrap_append(index)
        )

    def save(index):
        return Worker(isolated_redis).save_tool_result(
            streams[index],
            conversations[index],
            ToolResultPayload(
                turn_id="turn-1",
                step_id="step-1",
                tool_call_id="call-1" if same_call else f"call-{index + 1}",
                result=f"result-{index}",
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        saved = list(executor.map(save, range(2)))

    assert sorted(saved) == ([False, True] if same_call else [True, True])
    assert sorted(attempts) == ([1, 1] if same_call else [1, 2])
    results = [
        event.payload
        for event in stream.iter_events()
        if isinstance(event.payload, ToolResultPayload)
    ]
    assert len(results) == len(calls)
    actions = worker.dsp_stream.redis.xrange(worker.dsp_stream.name)
    assert len(actions) == 1
    entry_id, fields = actions[0]
    action = decode_dispatch_event(entry_id, fields)
    assert not isinstance(action, InvalidDispatchEvent)
    assert action.type == Action.AGENT_RESUME
    if not same_call:
        assert sorted(
            len(conversation.turn_state["turn-1"].results)
            for conversation in conversations
        ) == [1, 2]


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

    with monkeypatch.context() as context:
        load_agent = Mock(side_effect=ModuleNotFoundError("unavailable agent"))
        context.setattr("fallingwater.worker.reify_agent", load_agent)
        assert _process(worker, parent_notify)
        load_agent.assert_not_called()
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
                    turn_messages=ModelMessagesTypeAdapter.dump_python(
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
    def fail(*_args, **_kwargs):
        try:
            raise ValueError("underlying failure")
        except ValueError as error:
            raise RuntimeError("agent failed") from error

    agent = Agent(TestModel(), name="failing-agent")
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.worker.run", fail)
    redis = MemoryRedis(_base_entries())
    worker = _worker(redis)
    action = decode_dispatch_event(
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
    assert failed.traceback is not None
    assert "Traceback (most recent call last):" in failed.traceback
    assert "in fail" in failed.traceback
    assert "ValueError: underlying failure" in failed.traceback
    assert "The above exception was the direct cause" in failed.traceback
    assert failed.traceback.endswith("RuntimeError: agent failed\n")
    acknowledge.assert_called_once_with(worker.group, "1-0")


@pytest.mark.parametrize("traceback", [None, "child traceback"])
def test_failed_child_fails_parent_turn(monkeypatch, traceback) -> None:
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
    worker.queue_subagent(
        parent_stream,
        reconstruct_conversation(
            parent_stream.iter_events(), conversation_id="abc"
        ),
        parent_request,
        SubAgentRequest(
            agent_path="example:child", model="test", prompt="child"
        ),
    )
    child_stream = worker.dsp_stream.conversation_stream("child-conversation")
    child_conversation = reconstruct_conversation(
        child_stream.iter_events(), conversation_id="child-conversation"
    )
    assert child_conversation.settings.parent is not None
    worker.checkpoint(
        child_stream,
        child_conversation,
        TurnFailedPayload(
            turn_id="child-turn", error="child failed", traceback=traceback
        ),
    )
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent", lambda _path: parent_agent
    )
    notification = _dispatch(redis)[-1]

    assert _process(worker, notification)
    failed = _events(redis)[-1].payload
    assert isinstance(failed, TurnFailedPayload)
    assert failed.error == "Subagent failed: child failed"
    assert failed.traceback == traceback


@pytest.mark.parametrize(
    ("fields", "acknowledge"),
    [
        ({"type": Action.CONVERSATION_POKE}, True),
        ({"type": Action.AGENT_RESUME, "step_id": "step-1"}, False),
        ({"type": Action.AGENT_RESUME, "step_id": "old-step"}, True),
        (
            {
                "type": Action.TOOL_CALL,
                "step_id": "old-step",
                "tool_call_id": "call-1",
            },
            True,
        ),
        (
            {
                "type": Action.TOOL_CALL,
                "step_id": "step-1",
                "tool_call_id": "unknown-call",
            },
            True,
        ),
    ],
)
def test_ineligible_actions_do_not_load_agent(
    monkeypatch, fields, acknowledge
) -> None:
    request = _tool_request([ToolCallPart("unused", {}, tool_call_id="call-1")])
    redis = MemoryRedis(
        [*_base_entries(), _entry("3-0", Event.TURN_TOOL_REQUESTED, request)]
    )
    load_agent = Mock(side_effect=ModuleNotFoundError("unavailable agent"))
    monkeypatch.setattr("fallingwater.worker.reify_agent", load_agent)

    assert (
        _process(
            Worker(redis),
            {"conversation_id": "abc", "turn_id": "turn-1", **fields},
        )
        is acknowledge
    )
    load_agent.assert_not_called()
    assert len(_events(redis)) == 3
    assert _dispatch(redis) == []


@pytest.mark.parametrize("terminal", [False, True])
def test_result_save_refreshes_before_writing(terminal) -> None:
    request = _tool_request([ToolCallPart("unused", {}, tool_call_id="call-1")])
    redis = MemoryRedis(
        [*_base_entries(), _entry("3-0", Event.TURN_TOOL_REQUESTED, request)]
    )
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    result = ToolResultPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        result="answer",
    )
    if terminal:
        stream.append_event(
            Event.TURN_FAILED,
            TurnFailedPayload(turn_id="turn-1", error="sibling failed"),
        )
    else:
        stream.append_event(Event.TURN_TOOL_RESULT, result)

    assert not Worker(redis).save_tool_result(stream, conversation, result)
    assert len(_events(redis)) == 4
    assert _dispatch(redis) == []


@pytest.mark.parametrize("next_step", [False, True])
def test_result_save_rejects_obsolete_result_before_append(
    monkeypatch, next_step
) -> None:
    request = _tool_request([ToolCallPart("unused", {}, tool_call_id="call-1")])
    redis = MemoryRedis(
        [*_base_entries(), _entry("3-0", Event.TURN_TOOL_REQUESTED, request)]
    )
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    append_if_current = stream.append_if_current

    def append_with_new_state(event_type, payload, *, after_id):
        if next_step:
            stream.append_event(Event.TURN_TOOL_RESULT, payload)
            stream.append_event(
                Event.TURN_TOOL_REQUESTED,
                _tool_request(
                    [ToolCallPart("unused", {}, tool_call_id="call-2")],
                    step_id="step-2",
                ),
            )
        else:
            stream.append_event(
                Event.TURN_FAILED,
                TurnFailedPayload(turn_id="turn-1", error="sibling failed"),
            )
        return append_if_current(event_type, payload, after_id=after_id)

    monkeypatch.setattr(stream, "append_if_current", append_with_new_state)
    assert not Worker(redis).save_tool_result(
        stream,
        conversation,
        ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
            result="answer",
        ),
    )
    assert _dispatch(redis) == []
    assert len(_events(redis)) == (5 if next_step else 4)


def test_result_save_keeps_first_persisted_value_during_race(
    monkeypatch,
) -> None:
    request = _tool_request([ToolCallPart("unused", {}, tool_call_id="call-1")])
    redis = MemoryRedis(
        [*_base_entries(), _entry("3-0", Event.TURN_TOOL_REQUESTED, request)]
    )
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    append_if_current = stream.append_if_current

    def append_with_earlier_result(event_type, payload, *, after_id):
        stream.append_event(
            Event.TURN_TOOL_RESULT,
            payload.model_copy(update={"result": "earlier result"}),
        )
        return append_if_current(event_type, payload, after_id=after_id)

    monkeypatch.setattr(stream, "append_if_current", append_with_earlier_result)
    assert not Worker(redis).save_tool_result(
        stream,
        conversation,
        ToolResultPayload(
            turn_id="turn-1",
            step_id="step-1",
            tool_call_id="call-1",
            result="later result",
        ),
    )
    fresh = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    assert conversation.turn_state["turn-1"].tool_results == {
        "call-1": "earlier result"
    }
    assert fresh.turn_state["turn-1"].tool_results == (
        conversation.turn_state["turn-1"].tool_results
    )
    assert len(_events(redis)) == 4
    assert _dispatch(redis) == []


@pytest.mark.parametrize("first_succeeds", [False, True])
def test_checkpoint_and_refresh_preserve_first_concurrent_terminal(
    first_succeeds,
) -> None:
    redis = MemoryRedis(_base_entries())
    stream = Worker(redis).dsp_stream.conversation_stream("abc")
    conversation = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )
    completed = TurnCompletedPayload(
        turn_id="turn-1",
        message="answer",
        turn_messages=ModelMessagesTypeAdapter.dump_python(
            _messages("answer"), mode="json"
        ),
        usage=RunUsage(requests=3),
    )
    failed = TurnFailedPayload(turn_id="turn-1", error="failed")
    stream.append_event(
        Event.TURN_COMPLETED if first_succeeds else Event.TURN_FAILED,
        completed if first_succeeds else failed,
    )
    worker = Worker(redis)
    worker.checkpoint(
        stream,
        conversation,
        failed if first_succeeds else completed,
    )
    assert conversation.turn_state["turn-1"].running
    worker.checkpoint(
        stream,
        conversation,
        TurnQueuedPayload(turn_id="turn-2", message="next prompt"),
    )
    worker.refresh(stream, conversation)
    fresh = reconstruct_conversation(
        stream.iter_events(), conversation_id="abc"
    )

    assert conversation.turn_state["turn-1"].state == (
        completed if first_succeeds else failed
    )
    assert conversation.turn_state["turn-2"].history == (
        ModelMessagesTypeAdapter.validate_python(completed.turn_messages)
        if first_succeeds
        else []
    )
    assert conversation.turn_state["turn-2"].usage == (
        completed.usage if first_succeeds else None
    )
    assert fresh.turn_state["turn-2"].history == (
        conversation.turn_state["turn-2"].history
    )
    assert fresh.turn_state["turn-2"].usage == (
        conversation.turn_state["turn-2"].usage
    )


def test_redis_failure_leaves_dispatch_pending(monkeypatch) -> None:
    redis = MemoryRedis(_base_entries())
    worker = Worker(redis)
    action = decode_dispatch_event(
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
        lambda *_args, **_kwargs: ("0-0", [action], []),
    )
    monkeypatch.setattr(
        "fallingwater.worker.reify_agent",
        lambda _path: Agent(TestModel()),
    )
    monkeypatch.setattr(
        "fallingwater.worker.run",
        Mock(side_effect=RedisError("connection lost")),
    )
    acknowledge = Mock()
    monkeypatch.setattr(worker.dsp_stream, "acknowledge", acknowledge)

    with pytest.raises(RedisError, match="connection lost"):
        worker.run()
    acknowledge.assert_not_called()
    assert len(_events(redis)) == 2


def test_agent_resume_preserves_history_messages_and_usage_across_steps(
    monkeypatch,
) -> None:
    agent = Agent(TestModel(), name="multi-step-agent")

    @agent.tool_plain
    def echo(value: str) -> str:
        return value

    prior_messages = _messages("previous answer")
    prior_usage = RunUsage(requests=4, input_tokens=100)
    step_messages = [
        _tool_request(
            [ToolCallPart("echo", {"value": value}, tool_call_id=call_id)]
        )
        for value, call_id in [("first", "call-1"), ("second", "call-2")]
    ]
    steps = [
        AgentRunResult(
            output=TypeAdapter(DeferredToolRequests).validate_python(
                request.requests
            ),
            usage=RunUsage(requests=5 + index, input_tokens=120 + index),
            new_messages=ModelMessagesTypeAdapter.validate_python(
                request.step_messages
            ),
            tool_contexts={},
        )
        for index, request in enumerate(step_messages)
    ]
    final = AgentRunResult(
        output="final answer",
        usage=RunUsage(requests=7, input_tokens=150),
        new_messages=_messages("final answer"),
        tool_contexts={},
    )
    run_agent = Mock(side_effect=[*steps, final])
    monkeypatch.setattr("fallingwater.worker.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.worker.run", run_agent)
    redis = MemoryRedis(
        [
            *_base_entries(turn_id="turn-0"),
            _entry(
                "3-0",
                Event.TURN_COMPLETED,
                TurnCompletedPayload(
                    turn_id="turn-0",
                    message="previous answer",
                    turn_messages=ModelMessagesTypeAdapter.dump_python(
                        prior_messages, mode="json"
                    ),
                    usage=prior_usage,
                ),
            ),
            _entry(
                "4-0",
                Event.TURN_QUEUED,
                TurnQueuedPayload(turn_id="turn-1", message="next question"),
            ),
        ]
    )
    worker = Worker(redis)
    assert _process(
        worker,
        {
            "type": Action.CONVERSATION_POKE,
            "conversation_id": "abc",
            "turn_id": "turn-1",
        },
    )
    for _step in steps:
        assert _dispatch(redis)[-1]["type"] == Action.TOOL_CALL
        assert _process(worker, _dispatch(redis)[-1])
        assert _dispatch(redis)[-1]["type"] == Action.AGENT_RESUME
        assert _process(worker, _dispatch(redis)[-1])

    expected_history = list(prior_messages)
    for call, usage, messages in zip(
        run_agent.call_args_list,
        [prior_usage, steps[0].usage, steps[1].usage],
        [steps[0].new_messages, steps[1].new_messages, final.new_messages],
    ):
        assert call.kwargs["message_history"] == expected_history
        assert call.kwargs["usage"] == usage
        expected_history.extend(messages)
    assert run_agent.call_args_list[0].args[1] == "next question"
    assert all(call.args[1] is None for call in run_agent.call_args_list[1:])
    completed = _events(redis)[-1].payload
    assert isinstance(completed, TurnCompletedPayload)
    assert ModelMessagesTypeAdapter.validate_python(
        completed.turn_messages
    ) == [
        *steps[0].new_messages,
        *steps[1].new_messages,
        *final.new_messages,
    ]
    assert completed.usage == final.usage
