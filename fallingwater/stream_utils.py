"""Redis stream names, event codecs, and access helpers."""

import re
from collections.abc import Mapping
from typing import Any

from pydantic import TypeAdapter
from zstandard import ZstdError

from . import stream_models
from .utils import compress, decompress


_dispatch_action_adapter = TypeAdapter(stream_models.DispatchAction)


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
        decode_text(key): decode_text(value) for key, value in fields.items()
    }


def decode_dispatch_action(
    entry_id: str | bytes,
    fields: Mapping[Any, Any],
) -> stream_models.DispatchAction | stream_models.InvalidDispatchAction:
    """Decode one dispatch entry, preserving malformed entries for acknowledgement."""
    entry_id = decode_text(entry_id)
    try:
        values = decode_dispatch_fields(fields)
        values["entry_id"] = entry_id
        return _dispatch_action_adapter.validate_python(values)
    except (ValueError, TypeError) as error:
        return stream_models.InvalidDispatchAction(
            entry_id=entry_id, fields=fields, error=str(error)
        )


def encode_conversation_event(
    event_type: stream_models.Event, payload: stream_models.ConversationPayload
) -> dict[str, str | bytes]:
    """Validate and encode a compressed conversation stream event."""
    payload_model = stream_models.PAYLOAD_MODELS.get(event_type)
    if payload_model is None or not isinstance(payload, payload_model):
        raise TypeError(f"invalid payload for conversation event {event_type}")
    compressed_payload = compress(payload.model_dump_json().encode("utf-8"))
    return {"type": event_type.value, "payload": compressed_payload}


def decode_conversation_event(
    entry_id: str | bytes, fields: Mapping[Any, Any]
) -> stream_models.ConversationEvent:
    """Decode and validate a compressed conversation entry with its Redis ID."""
    text_fields = {decode_text(key): value for key, value in fields.items()}
    try:
        event_type = stream_models.Event(decode_text(text_fields["type"]))
        payload_data = text_fields["payload"]
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError(
            "invalid conversation stream event envelope"
        ) from error
    if text_fields.keys() != {"type", "payload"}:
        raise ValueError(
            "conversation stream events must contain only type and payload"
        )

    payload_model = stream_models.PAYLOAD_MODELS.get(event_type)
    if payload_model is None:
        raise ValueError(f"unsupported conversation event type: {event_type}")
    if not isinstance(payload_data, bytes):
        raise ValueError("conversation event payload must be compressed bytes")
    try:
        payload_json = decompress(payload_data)
        payload = payload_model.model_validate_json(payload_json)
    except (ValueError, ZstdError) as error:
        raise ValueError(
            f"invalid compressed payload for {event_type}"
        ) from error
    return stream_models.ConversationEvent(
        entry_id=decode_text(entry_id), type=event_type, payload=payload
    )
