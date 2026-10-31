"""
Redis stream names, event payloads, and access helpers.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from pydantic_ai.usage import RunUsage

from .utils import compress, decompress


RawStreamEntry: TypeAlias = tuple[Any, Mapping[Any, Any]]


class Event(StrEnum):
    """
    Durable events stored in
    <namespace>:conversation:<conversation-id>:events
    streams
    """

    CONVERSATION_CREATED = "conversation.created"
    TURN_QUEUED = "turn.queued"
    TURN_TOOL_REQUESTED = "turn.tool.requested"
    TURN_TOOL_RESULT = "turn.tool.result"
    TURN_SUBAGENT_REQUESTED = "turn.subagent.requested"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"


class Action(StrEnum):
    """
    Recoverable worker actions stored in the shared
    <namespace>:dispatch stream.
    """

    CONVERSATION_POKE = "conversation.poke"
    TOOL_CALL = "turn.tool.call"
    AGENT_RESUME = "turn.agent.resume"
    SUBAGENT_RESULT = "turn.subagent.result"


class _DispatchAction(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    entry_id: str = Field(exclude=True, min_length=1)
    conversation_id: str = Field(min_length=1)
    turn_id: str = Field(min_length=1)


class ConversationPokeAction(_DispatchAction):
    type: Literal[Action.CONVERSATION_POKE]


class AgentResumeAction(_DispatchAction):
    type: Literal[Action.AGENT_RESUME]
    step_id: str = Field(min_length=1)


class ToolCallAction(_DispatchAction):
    type: Literal[Action.TOOL_CALL]
    step_id: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)


class SubAgentResultAction(_DispatchAction):
    type: Literal[Action.SUBAGENT_RESULT]
    step_id: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)
    child_conversation_id: str = Field(min_length=1)
    child_turn_id: str = Field(min_length=1)


DispatchAction: TypeAlias = Annotated[
    ConversationPokeAction
    | AgentResumeAction
    | ToolCallAction
    | SubAgentResultAction,
    Field(discriminator="type"),
]
_dispatch_action_adapter = TypeAdapter(DispatchAction)


@dataclass(frozen=True)
class InvalidDispatchAction:
    """A dispatch entry that cannot be routed or safely associated with a turn."""

    entry_id: str
    fields: Mapping[Any, Any]
    error: str


class ConversationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def fmt(self, full: bool = False) -> dict[str, Any]:
        """
        Return the payload as JSON-compatible data.
        """
        return self.model_dump(mode="json")


class ParentToolReference(ConversationPayload):
    conversation_id: str
    turn_id: str
    step_id: str
    tool_call_id: str


class ConversationCreatedPayload(ConversationPayload):
    agent: str
    model: str = ""
    deps: dict[str, Any] = Field(default_factory=dict)
    parent: ParentToolReference | None = None


class TurnQueuedPayload(ConversationPayload):
    turn_id: str
    message: str


class ToolRequestedPayload(ConversationPayload):
    turn_id: str
    step_id: str
    requests: dict[str, Any]
    history: list[dict[str, Any]]
    usage: RunUsage


class ToolResultPayload(ConversationPayload):
    turn_id: str
    step_id: str
    tool_call_id: str
    result: Any


class SubAgentRequestedPayload(ConversationPayload):
    turn_id: str
    step_id: str
    tool_call_id: str
    conversation_id: str
    child_turn_id: str


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
    | ToolRequestedPayload
    | ToolResultPayload
    | SubAgentRequestedPayload
    | TurnCompletedPayload
    | TurnFailedPayload
)

PAYLOAD_MODELS: dict[Event, type[ConversationPayloadType]] = {
    Event.CONVERSATION_CREATED: ConversationCreatedPayload,
    Event.TURN_QUEUED: TurnQueuedPayload,
    Event.TURN_TOOL_REQUESTED: ToolRequestedPayload,
    Event.TURN_TOOL_RESULT: ToolResultPayload,
    Event.TURN_SUBAGENT_REQUESTED: SubAgentRequestedPayload,
    Event.TURN_COMPLETED: TurnCompletedPayload,
    Event.TURN_FAILED: TurnFailedPayload,
}


class ConversationEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: Event
    payload: ConversationPayloadType


class ConversationStreamEvent(ConversationEvent):
    """A decoded conversation event with its Redis stream entry ID."""

    entry_id: str = Field(exclude=True, min_length=1)


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
) -> DispatchAction | InvalidDispatchAction:
    """Decode one dispatch entry, preserving malformed entries for acknowledgement."""
    entry_id = decode_text(entry_id)
    try:
        values = decode_dispatch_fields(fields)
        values["entry_id"] = entry_id
        return _dispatch_action_adapter.validate_python(values)
    except (ValueError, TypeError) as error:
        return InvalidDispatchAction(
            entry_id=entry_id, fields=fields, error=str(error)
        )


def encode_conversation_event(
    event_type: Event, payload: ConversationPayload
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
        event_type = Event(decode_text(text_fields["type"]))
        payload_data = text_fields["payload"]
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError(
            "invalid conversation stream event envelope"
        ) from error
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
        raise ValueError(
            f"invalid compressed payload for {event_type}"
        ) from error
    return ConversationEvent(type=event_type, payload=payload)


def decode_conversation_stream_event(
    entry_id: str | bytes, fields: Mapping[Any, Any]
) -> ConversationStreamEvent:
    """Decode a Redis conversation entry with its ID as read metadata."""
    event = decode_conversation_event(fields)
    return ConversationStreamEvent(
        entry_id=decode_text(entry_id), type=event.type, payload=event.payload
    )


def decode_conversation_entry(
    entry: RawStreamEntry,
) -> tuple[str, ConversationEvent]:
    """Decode one Redis conversation entry and its stream ID."""
    entry_id, fields = entry
    return decode_text(entry_id), decode_conversation_event(fields)
