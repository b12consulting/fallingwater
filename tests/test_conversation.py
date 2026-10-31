"""Checks conversation and dispatch stream access."""

from unittest.mock import MagicMock

import pytest
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import ResponseError

from fallingwater.stream import DispatchStream
from fallingwater.stream_models import (
    ConversationPokeAction,
    ConversationStreamEvent,
    Action,
    Event,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_event,
    encode_conversation_event,
)


def test_append_and_read_event_use_the_conversation_codec() -> None:
    redis = MagicMock(spec=Redis)
    redis.xadd.return_value = b"2-0"
    redis.xrange.return_value = [
        (
            b"2-0",
            encode_conversation_event(
                Event.TURN_QUEUED,
                TurnQueuedPayload(turn_id="turn-1", message="hello"),
            ),
        )
    ]
    stream = DispatchStream(redis).conversation_stream("abc")
    payload = TurnQueuedPayload(turn_id="turn-1", message="hello")

    assert stream.append_event(Event.TURN_QUEUED, payload) == "2-0"
    fields = redis.xadd.call_args.args[1]
    assert decode_conversation_event(fields).payload == payload
    assert stream.read_conversation_range() == [
        ConversationStreamEvent(
            entry_id="2-0",
            type=Event.TURN_QUEUED,
            payload=payload,
        )
    ]


def test_dispatch_append_event_writes_action_fields() -> None:
    redis = MagicMock(spec=Redis)
    redis.xadd.return_value = b"3-0"
    dispatch = DispatchStream(redis)

    assert dispatch.append_event(
        Action.CONVERSATION_POKE,
        conversation_id="abc",
        turn_id="turn-1",
    ) == "3-0"
    redis.xadd.assert_called_once_with(
        "fw:dispatch",
        {
            "type": Action.CONVERSATION_POKE,
            "conversation_id": "abc",
            "turn_id": "turn-1",
        },
    )


def test_queue_tool_requests_rejects_empty_set_without_writing() -> None:
    redis = MagicMock(spec=Redis)
    conversation = DispatchStream(redis).conversation_stream("abc")
    payload = ToolRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        requests={},
        history=[],
        usage=RunUsage(),
    )

    with pytest.raises(ValueError, match="must contain a call"):
        conversation.queue_tool_requests(payload, [])

    redis.xadd.assert_not_called()


def test_queue_subagent_writes_recoverable_child_state_before_dispatch() -> None:
    redis = MagicMock(spec=Redis)
    redis.xadd.return_value = b"1-0"
    parent = DispatchStream(redis).conversation_stream("parent")
    request = SubAgentRequestedPayload(
        turn_id="parent-turn",
        step_id="step-1",
        tool_call_id="call-1",
        child_conversation_id="child",
        child_turn_id="child-turn",
    )

    assert parent.queue_subagent(
        parent_request=request,
        child_agent="example:child",
        child_model="test",
        child_deps={},
        child_prompt="hello",
    )

    calls = redis.xadd.call_args_list
    assert [call.args[0] for call in calls] == [
        "fw:conversation:child:events",
        "fw:conversation:child:events",
        "fw:conversation:parent:events",
        "fw:dispatch",
    ]
    assert [decode_conversation_event(call.args[1]).type for call in calls[:3]] == [
        Event.CONVERSATION_CREATED,
        Event.TURN_QUEUED,
        Event.TURN_SUBAGENT_REQUESTED,
    ]
    assert calls[-1].args[1] == {
        "conversation_id": "child",
        "turn_id": "child-turn",
        "type": Action.CONVERSATION_POKE,
    }


def test_group_operations_keep_stream_group_and_consumer_explicit() -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    fields = encode_conversation_event(
        Event.TURN_QUEUED,
        TurnQueuedPayload(turn_id="turn-1", message="hello"),
    )
    redis.xreadgroup.return_value = [(stream, [(b"2-0", fields)])]
    conversation = DispatchStream(redis).conversation_stream("abc")

    events = conversation.read_group(
        "terminal-group",
        "terminal-reader",
        "0",
        count=10,
    )

    assert events[0].entry_id == "2-0"
    assert events[0].payload == TurnQueuedPayload(
        turn_id="turn-1", message="hello"
    )
    redis.xreadgroup.assert_called_once_with(
        "terminal-group", "terminal-reader", {stream: "0"}, count=10, block=None
    )


def test_dispatch_group_read_and_autoclaim_decode_entries_and_cursor() -> None:
    redis = MagicMock(spec=Redis)
    fields = {
        b"type": b"conversation.poke",
        b"conversation_id": b"abc",
        b"turn_id": b"turn-1",
    }
    redis.xreadgroup.return_value = [("fw:dispatch", [(b"3-0", fields)])]
    redis.xautoclaim.return_value = (
        b"4-0",
        [(b"3-0", fields)],
        [b"2-0"],
    )
    dispatch = DispatchStream(redis)

    assert dispatch.read_group(
        "workers", "worker-1", ">", count=1, block=500
    ) == [
        ConversationPokeAction(
            entry_id="3-0",
            type=Action.CONVERSATION_POKE,
            conversation_id="abc",
            turn_id="turn-1",
        ),
    ]
    assert dispatch.claim(
        "workers",
        "worker-1",
        min_idle_time=300_000,
        start_id="0-0",
        count=1,
    ) == (
        "4-0",
        [
            ConversationPokeAction(
                entry_id="3-0",
                type=Action.CONVERSATION_POKE,
                conversation_id="abc",
                turn_id="turn-1",
            ),
        ],
        ["2-0"],
    )
    redis.xautoclaim.assert_called_once_with(
        "fw:dispatch",
        "workers",
        "worker-1",
        min_idle_time=300_000,
        start_id="0-0",
        count=1,
    )


def test_group_creation_and_acknowledgement_target_the_given_stream() -> None:
    redis = MagicMock(spec=Redis)
    dispatch = DispatchStream(redis)
    conversation = dispatch.conversation_stream("abc")

    dispatch.create_group("workers", mkstream=True)
    conversation.create_group("terminal")
    conversation.acknowledge("terminal", "2-0", "3-0")

    redis.xgroup_create.assert_any_call(
        "fw:dispatch", "workers", id="0", mkstream=True
    )
    redis.xgroup_create.assert_any_call(
        "fw:conversation:abc:events", "terminal", id="0"
    )
    redis.xack.assert_called_once_with(
        "fw:conversation:abc:events", "terminal", "2-0", "3-0"
    )


def test_group_creation_ignores_only_existing_group_errors() -> None:
    redis = MagicMock(spec=Redis)
    dispatch = DispatchStream(redis)
    conversation = dispatch.conversation_stream("abc")
    redis.xgroup_create.side_effect = ResponseError("BUSYGROUP already exists")

    dispatch.create_group("workers", mkstream=True)
    conversation.create_group("terminal")

    redis.xgroup_create.side_effect = ResponseError("ERR other failure")
    with pytest.raises(ResponseError, match="other failure"):
        dispatch.create_group("workers", mkstream=True)
    with pytest.raises(ResponseError, match="other failure"):
        conversation.create_group("terminal")
