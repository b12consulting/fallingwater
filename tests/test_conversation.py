"""Checks conversation and dispatch stream access."""

from unittest.mock import MagicMock

from redis import Redis

from fallingwater.stream import DispatchStream
from fallingwater.stream_models import (
    ConversationPokeAction,
    ConversationStreamEvent,
    Action,
    Event,
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
