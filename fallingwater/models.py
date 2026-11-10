"""Schemas for conversation events and worker dispatch actions."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from typing import Any, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_ai.usage import RunUsage


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


class DispatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    conversation_id: str = Field(min_length=1)
    turn_id: str = Field(min_length=1)


class ConversationPokePayload(DispatchPayload):
    pass


class AgentResumePayload(DispatchPayload):
    step_id: str = Field(min_length=1)


class ToolCallPayload(DispatchPayload):
    step_id: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)


class SubAgentResultPayload(DispatchPayload):
    step_id: str = Field(min_length=1)
    tool_call_id: str = Field(min_length=1)
    child_conversation_id: str = Field(min_length=1)
    child_turn_id: str = Field(min_length=1)
    child_terminal_event_id: str = Field(min_length=1)


DispatchPayloadType: TypeAlias = (
    ConversationPokePayload
    | AgentResumePayload
    | ToolCallPayload
    | SubAgentResultPayload
)

DISPATCH_PAYLOAD_MODELS: dict[Action, type[DispatchPayloadType]] = {
    Action.CONVERSATION_POKE: ConversationPokePayload,
    Action.AGENT_RESUME: AgentResumePayload,
    Action.TOOL_CALL: ToolCallPayload,
    Action.SUBAGENT_RESULT: SubAgentResultPayload,
}


@cache
def get_dispatch_action(payload_model: type[DispatchPayloadType]) -> Action:
    """
    Return dispatch action to send to dispatch stream based on
    payload model
    """
    for action, model in DISPATCH_PAYLOAD_MODELS.items():
        if issubclass(payload_model, model):
            return action


class DispatchEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    entry_id: str = Field(min_length=1)
    type: Action
    payload: DispatchPayloadType

    @model_validator(mode="after")
    def validate_payload_type(self) -> Self:
        if not isinstance(self.payload, DISPATCH_PAYLOAD_MODELS[self.type]):
            raise ValueError(f"invalid payload for dispatch event {self.type}")
        return self


@dataclass(frozen=True)
class InvalidDispatchEvent:
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
    step_messages: list[dict[str, Any]]
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
    child_conversation_id: str
    child_turn_id: str
    agent_path: str
    model: str
    prompt: str
    deps: dict[str, Any] = Field(default_factory=dict)


class TurnCompletedPayload(ConversationPayload):
    turn_id: str
    message: str
    turn_messages: list[dict[str, Any]]
    usage: RunUsage

    def fmt(self, full: bool = False) -> dict[str, Any]:
        """
        Return the payload, including turn messages and usage only when
        requested.
        """
        payload = super().fmt(full=full)
        if not full:
            payload.pop("turn_messages")
            payload.pop("usage")
        return payload


class TurnFailedPayload(ConversationPayload):
    turn_id: str
    error: str
    traceback: str | None = None


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


@cache
def get_cnv_event(cnv_payload: type[ConversationPayloadType]) -> Event:
    """
    Return event to put in conversation stream based on given
    payload type
    """
    for event, payload in PAYLOAD_MODELS.items():
        if issubclass(cnv_payload, payload):
            return event


class ConversationEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    entry_id: str = Field(min_length=1)
    type: Event
    payload: ConversationPayloadType

    @model_validator(mode="after")
    def validate_payload_type(self) -> Self:
        if not isinstance(self.payload, PAYLOAD_MODELS[self.type]):
            raise ValueError(f"invalid payload for conversation event {self.type}")
        return self
