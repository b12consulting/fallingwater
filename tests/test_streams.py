"""Conversation event encoding and Redis key helpers."""

import pytest

from fallingwater.stream_models import (
    ConversationCreatedPayload,
    Event,
    InvalidDispatchAction,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_entry,
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

    event = decode_conversation_event(
        {b"type": encoded["type"].encode(), b"payload": encoded["payload"]}
    )
    assert event.type == Event.TURN_QUEUED
    assert event.payload == TurnQueuedPayload(turn_id="turn-1", message="hello")


def test_conversation_entry_decodes_its_id_and_event() -> None:
    fields = encode_conversation_event(
        Event.CONVERSATION_CREATED,
        ConversationCreatedPayload(agent="example:agent", model="test"),
    )

    entry_id, event = decode_conversation_entry((b"1-0", fields))

    assert entry_id == "1-0"
    assert event.type == Event.CONVERSATION_CREATED
    assert event.payload.agent == "example:agent"


def test_tool_request_and_result_events_round_trip() -> None:
    requested = ToolRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        requests={"calls": [{"tool_name": "multiply", "args": {"value": 2}}]},
        history=[{"kind": "request"}],
        usage=RunUsage(),
    )
    request_event = decode_conversation_event(
        encode_conversation_event(Event.TURN_TOOL_REQUESTED, requested)
    )
    assert request_event.type == Event.TURN_TOOL_REQUESTED
    assert request_event.payload == requested

    result = ToolResultPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        result={"value": 2},
    )
    result_event = decode_conversation_event(
        encode_conversation_event(Event.TURN_TOOL_RESULT, result)
    )
    assert result_event.type == Event.TURN_TOOL_RESULT
    assert result_event.payload == result


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
