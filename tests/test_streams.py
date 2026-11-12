"""Conversation event encoding and Redis key helpers."""

import json

import pytest
import zstandard as zstd

from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Action,
    AgentResumePayload,
    AgentRequestedPayload,
    ConversationPokePayload,
    DispatchEvent,
    SubAgentResultPayload,
    ToolCallPayload,
    Event,
    InvalidDispatchEvent,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_event,
    decode_dispatch_fields,
    decode_dispatch_event,
    encode_conversation_event,
    encode_dispatch_event,
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


def test_legacy_failed_event_without_traceback_decodes() -> None:
    event = decode_conversation_event(
        "1-0",
        {
            "type": Event.TURN_FAILED.value,
            "payload": zstd.compress(
                b'{"turn_id":"turn-1","error":"legacy failure"}'
            ),
        },
    )

    assert isinstance(event.payload, TurnFailedPayload)
    assert event.payload.error == "legacy failure"
    assert event.payload.traceback is None


def test_agent_request_event_round_trips_with_its_wire_name() -> None:
    payload = AgentRequestedPayload(
        turn_id="turn-1",
        step_id="step-1",
        tool_call_id="call-1",
        child_conversation_id="writer",
        child_turn_id="writer-turn",
        agent_path="example:writer",
        model="test",
        prompt="draft",
        name="alice",
        persistent=True,
    )

    fields = encode_conversation_event(Event.TURN_AGENT_REQUESTED, payload)

    assert fields["type"] == "turn.agent.requested"
    assert decode_conversation_event("1-0", fields).payload == payload


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

    action = decode_dispatch_event(b"2-0", fields)

    assert isinstance(action, InvalidDispatchEvent)
    assert action.entry_id == "2-0"
    assert action.fields == fields
    assert "type" in action.error


@pytest.mark.parametrize(
    ("action_type", "payload"),
    [
        (
            Action.CONVERSATION_POKE,
            ConversationPokePayload(conversation_id="abc", turn_id="turn-1"),
        ),
        (
            Action.AGENT_RESUME,
            AgentResumePayload(
                conversation_id="abc", turn_id="turn-1", step_id="step-1"
            ),
        ),
        (
            Action.TOOL_CALL,
            ToolCallPayload(
                conversation_id="abc",
                turn_id="turn-1",
                step_id="step-1",
                tool_call_id="call-1",
            ),
        ),
        (
            Action.SUBAGENT_RESULT,
            SubAgentResultPayload(
                conversation_id="abc",
                turn_id="turn-1",
                step_id="step-1",
                tool_call_id="call-1",
                child_conversation_id="child",
                child_turn_id="child-turn",
                child_terminal_event_id="5-0",
            ),
        ),
    ],
)
def test_dispatch_events_round_trip_with_typed_payloads(
    action_type, payload
) -> None:
    fields = encode_dispatch_event(action_type, payload)
    event = decode_dispatch_event(
        b"2-0", {key.encode(): value.encode() for key, value in fields.items()}
    )

    assert "entry_id" not in fields
    assert "payload" not in fields
    assert event == DispatchEvent(entry_id="2-0", type=action_type, payload=payload)
    assert DispatchEvent.model_validate_json(event.model_dump_json()) == event
    assert "type" not in payload.model_dump()
    assert "entry_id" not in payload.model_dump()


def test_dispatch_events_reject_mismatched_payload_type() -> None:
    payload = AgentResumePayload(
        conversation_id="abc", turn_id="turn-1", step_id="step-1"
    )
    with pytest.raises(ValueError, match="invalid payload"):
        DispatchEvent(
            entry_id="1-0", type=Action.CONVERSATION_POKE, payload=payload
        )
    with pytest.raises(ValueError, match="invalid payload"):
        DispatchEvent.model_validate_json(
            json.dumps({
                "entry_id": "1-0",
                "type": Action.CONVERSATION_POKE,
                "payload": payload.model_dump(),
            })
        )
    fields = encode_dispatch_event(Action.CONVERSATION_POKE, payload)
    assert isinstance(decode_dispatch_event("1-0", fields), InvalidDispatchEvent)


def test_conversation_events_reject_mismatched_payload_type() -> None:
    payload = TurnQueuedPayload(turn_id="turn-1", message="hello")
    with pytest.raises(ValueError, match="invalid payload"):
        ConversationEvent(
            entry_id="1-0", type=Event.TURN_COMPLETED, payload=payload
        )
    with pytest.raises(ValueError, match="invalid payload"):
        ConversationEvent.model_validate_json(
            json.dumps({
                "entry_id": "1-0",
                "type": Event.TURN_COMPLETED,
                "payload": payload.model_dump(),
            })
        )
    fields = encode_conversation_event(Event.TURN_COMPLETED, payload)
    with pytest.raises(ValueError, match="invalid payload"):
        decode_conversation_event("1-0", fields)


@pytest.mark.parametrize(
    "extra_fields",
    [
        {"turn_id": ""},
        {"unexpected": "field"},
        {"type": "unknown"},
    ],
)
def test_invalid_dispatch_events_preserve_id_and_fields(extra_fields) -> None:
    fields = {
        "type": Action.CONVERSATION_POKE.value,
        "conversation_id": "abc",
        "turn_id": "turn-1",
        **extra_fields,
    }
    event = decode_dispatch_event(b"2-0", fields)

    assert isinstance(event, InvalidDispatchEvent)
    assert event.entry_id == "2-0"
    assert event.fields == fields
    assert event.error


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
