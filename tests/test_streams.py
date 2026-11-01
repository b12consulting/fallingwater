"""Conversation event encoding and Redis key helpers."""

import pytest
import zstandard as zstd

from fallingwater.stream_models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    InvalidDispatchAction,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_event,
    decode_dispatch_fields,
    decode_dispatch_action,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from pydantic_ai.usage import RunUsage


def test_conversation_event_round_trips_as_compressed_payload() -> None:
    encoded = encode_conversation_event(
        Event.TURN_QUEUED,
        TurnQueuedPayload(turn_id="turn-1", message="hello"),
    )

    assert encoded["type"] == Event.TURN_QUEUED.value
    assert isinstance(encoded["payload"], bytes)
    assert zstd.ZstdDecompressor().decompress(encoded["payload"]) == (
        TurnQueuedPayload(turn_id="turn-1", message="hello")
        .model_dump_json()
        .encode("utf-8")
    )

    event = decode_conversation_event(
        "1-0",
        {b"type": encoded["type"].encode(), b"payload": encoded["payload"]}
    )
    assert event.type == Event.TURN_QUEUED
    assert event.payload == TurnQueuedPayload(turn_id="turn-1", message="hello")


@pytest.mark.parametrize("entry_id", ["1-0", b"1-0"])
def test_conversation_stream_event_decodes_its_id_and_event(entry_id) -> None:
    fields = encode_conversation_event(
        Event.CONVERSATION_CREATED,
        ConversationCreatedPayload(agent="example:agent", model="test"),
    )

    event = decode_conversation_event(entry_id, fields)

    assert isinstance(event, ConversationEvent)
    assert event.entry_id == "1-0"
    assert event.type == Event.CONVERSATION_CREATED
    assert event.payload.agent == "example:agent"
    assert event.model_dump(mode="json")["entry_id"] == "1-0"


@pytest.mark.parametrize("entry_id", ["", b""])
def test_conversation_stream_event_requires_nonempty_id(entry_id) -> None:
    fields = encode_conversation_event(
        Event.TURN_QUEUED,
        TurnQueuedPayload(turn_id="turn-1", message="hello"),
    )
    with pytest.raises(ValueError, match="entry_id"):
        decode_conversation_event(entry_id, fields)


def test_conversation_stream_event_rejects_invalid_compressed_payload() -> None:
    with pytest.raises(ValueError, match="invalid compressed payload"):
        decode_conversation_event(
            "1-0", {"type": Event.TURN_QUEUED.value, "payload": b"not zstd"}
        )


def test_tool_request_and_result_events_round_trip() -> None:
    requested = ToolRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        requests={"calls": [{"tool_name": "multiply", "args": {"value": 2}}]},
        step_messages=[{"kind": "request"}],
        usage=RunUsage(),
    )
    request_event = decode_conversation_event(
        "1-0",
        encode_conversation_event(Event.TURN_TOOL_REQUESTED, requested)
    )
    assert request_event.type == Event.TURN_TOOL_REQUESTED
    assert request_event.payload == requested
    assert requested.model_dump(mode="json")["step_messages"] == [
        {"kind": "request"}
    ]
    assert "history" not in requested.model_dump(mode="json")

    result = ToolResultPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        result={"value": 2},
    )
    result_event = decode_conversation_event(
        "2-0",
        encode_conversation_event(Event.TURN_TOOL_RESULT, result)
    )
    assert result_event.type == Event.TURN_TOOL_RESULT
    assert result_event.payload == result


def test_completed_turn_messages_round_trip_and_format() -> None:
    completed = TurnCompletedPayload(
        turn_id="turn-1",
        message="answer",
        turn_messages=[{"kind": "response"}],
        usage=RunUsage(),
    )
    event = decode_conversation_event(
        "3-0",
        encode_conversation_event(Event.TURN_COMPLETED, completed)
    )

    assert event.type == Event.TURN_COMPLETED
    assert event.payload == completed
    assert completed.fmt() == {"turn_id": "turn-1", "message": "answer"}
    assert completed.fmt(full=True)["turn_messages"] == [{"kind": "response"}]
    assert "usage" in completed.fmt(full=True)
    assert "history" not in completed.fmt(full=True)


def test_dispatch_fields_decode_text_without_compression() -> None:
    assert decode_dispatch_fields(
        {b"type": b"conversation.poke", b"conversation_id": b"abc"}
    ) == {"type": "conversation.poke", "conversation_id": "abc"}


def test_dispatch_action_without_type_is_invalid() -> None:
    fields = {b"conversation_id": b"abc", b"turn_id": b"turn-1"}

    action = decode_dispatch_action(b"2-0", fields)

    assert isinstance(action, InvalidDispatchAction)
    assert action.entry_id == "2-0"
    assert action.fields == fields
    assert "type" in action.error


def test_stream_name_helpers_validate_components() -> None:
    assert fmt_dispatch_stream("fw-test") == "fw-test:dispatch"
    assert (
        fmt_conversation_stream("fw-test", "abc")
        == "fw-test:conversation:abc:events"
    )

    with pytest.raises(ValueError):
        fmt_dispatch_stream("FW")
    with pytest.raises(ValueError):
        fmt_conversation_stream("fw", "bad:id")
