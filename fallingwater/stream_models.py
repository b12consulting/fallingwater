"""Schemas for conversation events and worker dispatch actions."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field
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
    child_terminal_event_id: str = Field(min_length=1)


DispatchAction: TypeAlias = Annotated[
    ConversationPokeAction
    | AgentResumeAction
    | ToolCallAction
    | SubAgentResultAction,
    Field(discriminator="type"),
]
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
    child_conversation_id: str
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
