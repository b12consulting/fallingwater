"""
Redis stream names, event payloads, and access helpers.
"""

import re
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai.usage import RunUsage

from .utils import compress, decompress


RawStreamEntry: TypeAlias = tuple[Any, Mapping[Any, Any]]


class EventType(StrEnum):
    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_POKE = "conversation.poke"
    TURN_QUEUED = "turn.queued"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"


class ConversationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def fmt(self, full: bool = False) -> dict[str, Any]:
        """
        Return the payload as JSON-compatible data.
        """
        return self.model_dump(mode="json")


class ConversationCreatedPayload(ConversationPayload):
    agent: str
    model: str = ""
    deps: dict[str, Any] = Field(default_factory=dict)


class TurnQueuedPayload(ConversationPayload):
    turn_id: str
    message: str


class TurnCompletedPayload(ConversationPayload):
    turn_id: str
    message: str
    history: list[dict[str, Any]]
    usage: RunUsage

    def fmt(self, full: bool = False) -> dict[str, Any]:
        """
        Return the payload, including history and usage only when requested.
        """
        payload = super().fmt(full=full)
        if not full:
            payload.pop("history")
            payload.pop("usage")
        return payload


class TurnFailedPayload(ConversationPayload):
    turn_id: str
    error: str


ConversationPayloadType: TypeAlias = (
    ConversationCreatedPayload
    | TurnQueuedPayload
    | TurnCompletedPayload
    | TurnFailedPayload
)

PAYLOAD_MODELS: dict[EventType, type[ConversationPayloadType]] = {
    EventType.CONVERSATION_CREATED: ConversationCreatedPayload,
    EventType.TURN_QUEUED: TurnQueuedPayload,
    EventType.TURN_COMPLETED: TurnCompletedPayload,
    EventType.TURN_FAILED: TurnFailedPayload,
}


class ConversationEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: EventType
    payload: ConversationPayloadType


def decode_text(value: str | bytes) -> str:
    """
    Normalize Redis text values when response decoding is disabled.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def validate_namespace(namespace: str) -> None:
    """
    Require a lowercase namespace containing letters, digits, or
    hyphens.
    """
    if (
        not isinstance(namespace, str)
        or re.fullmatch(r"[a-z0-9-]+", namespace) is None
    ):
        raise ValueError(
            "namespace must contain only lowercase letters, numbers, and hyphens"
        )


def validate_conversation_id(conversation_id: str) -> None:
    if not conversation_id or ":" in conversation_id:
        raise ValueError(
            "conversation ID must be nonempty and contain no colon"
        )


def fmt_dispatch_stream(namespace: str) -> str:
    """
    Format the dispatch stream name after validating its namespace.
    """
    validate_namespace(namespace)
    return f"{namespace}:dispatch"


def fmt_conversation_stream(namespace: str, conversation_id: str) -> str:
    """
    Format a conversation stream name after validating its components.
    """
    validate_namespace(namespace)
    validate_conversation_id(conversation_id)
    return f"{namespace}:conversation:{conversation_id}:events"


def decode_dispatch_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    """Decode a text-only dispatch entry returned by Redis."""
    return {
        decode_text(key): decode_text(value)
        for key, value in fields.items()
    }


def encode_conversation_event(
    event_type: EventType, payload: ConversationPayload
) -> dict[str, str | bytes]:
    """Validate and encode a compressed conversation stream event."""
    payload_model = PAYLOAD_MODELS.get(event_type)
    if payload_model is None or not isinstance(payload, payload_model):
        raise TypeError(f"invalid payload for conversation event {event_type}")
    compressed_payload = compress(payload.model_dump_json().encode("utf-8"))
    return {"type": event_type.value, "payload": compressed_payload}


def decode_conversation_event(fields: Mapping[Any, Any]) -> ConversationEvent:
    """Decode and validate one compressed conversation stream event."""
    text_fields = {decode_text(key): value for key, value in fields.items()}
    try:
        event_type = EventType(decode_text(text_fields["type"]))
        payload_data = text_fields["payload"]
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError("invalid conversation stream event envelope") from error
    if text_fields.keys() != {"type", "payload"}:
        raise ValueError(
            "conversation stream events must contain only type and payload"
        )

    payload_model = PAYLOAD_MODELS.get(event_type)
    if payload_model is None:
        raise ValueError(f"unsupported conversation event type: {event_type}")
    if not isinstance(payload_data, bytes):
        raise ValueError("conversation event payload must be compressed bytes")
    try:
        payload_json = decompress(payload_data)
        payload = payload_model.model_validate_json(payload_json)
    except Exception as error:
        raise ValueError(f"invalid compressed payload for {event_type}") from error
    return ConversationEvent(type=event_type, payload=payload)


def decode_conversation_entry(
    entry: RawStreamEntry,
) -> tuple[str, ConversationEvent]:
    """Decode one Redis conversation entry and its stream ID."""
    entry_id, fields = entry
    return decode_text(entry_id), decode_conversation_event(fields)
