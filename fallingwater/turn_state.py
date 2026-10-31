"""Reconstruct one turn's state from conversation events."""

from collections.abc import Iterable, Sequence
from typing import Any, cast

from pydantic import BaseModel, ConfigDict
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage

from .stream_models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    ParentToolReference,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)


class Conversation(BaseModel):
    """The reconstructed state of one selected conversation turn."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    agent_path: str
    model: str
    next_prompt: str
    current_turn_id: str
    history: Sequence[ModelMessage]
    deps: dict[str, Any]
    usage: RunUsage | None
    turn_messages: Sequence[ModelMessage]
    tool_requests: dict[str, Any] | None
    tool_step_id: str | None
    tool_results: dict[str, Any]
    subagent_requests: dict[str, SubAgentRequestedPayload]
    parent: ParentToolReference | None = None
    completed: bool = False
    failed: TurnFailedPayload | None = None


def reconstruct_turn(
    events: Iterable[ConversationEvent],
    current_turn_id: str,
    *,
    conversation_stream: str,
) -> Conversation:
    """Fold ordered events into the state needed to process one turn."""
    history: list[ModelMessage] = []
    turn_messages: list[ModelMessage] = []
    agent_path: str | None = None
    model = ""
    deps: dict[str, Any] = {}
    parent: ParentToolReference | None = None
    next_prompt: str | None = None
    usage: RunUsage | None = None
    requests: dict[str, Any] | None = None
    step_id: str | None = None
    tool_results: dict[str, Any] = {}
    subagent_requests: dict[str, SubAgentRequestedPayload] = {}
    completed = False
    failed: TurnFailedPayload | None = None
    queued = False

    for event in events:
        if event.type == Event.CONVERSATION_CREATED:
            created = cast(ConversationCreatedPayload, event.payload)
            agent_path = created.agent
            model = created.model
            deps = created.deps
            parent = created.parent
            continue

        if event.type == Event.TURN_QUEUED:
            queued_payload = cast(TurnQueuedPayload, event.payload)
            if queued_payload.turn_id == current_turn_id and not queued:
                next_prompt = queued_payload.message
                queued = True
            continue

        if not queued:
            if event.type == Event.TURN_COMPLETED:
                previous = cast(TurnCompletedPayload, event.payload)
                history.extend(
                    ModelMessagesTypeAdapter.validate_python(previous.history)
                )
                usage = previous.usage
            continue

        payload = event.payload
        if getattr(payload, "turn_id", None) != current_turn_id:
            continue
        if event.type == Event.TURN_TOOL_REQUESTED:
            requested = cast(ToolRequestedPayload, payload)
            turn_messages.extend(
                ModelMessagesTypeAdapter.validate_python(requested.history)
            )
            usage = requested.usage
            requests = requested.requests
            step_id = requested.step_id
            tool_results = {}
            subagent_requests = {}
        elif event.type == Event.TURN_TOOL_RESULT:
            tool_result = cast(ToolResultPayload, payload)
            if tool_result.step_id == step_id:
                tool_results[tool_result.tool_call_id] = tool_result.result
        elif event.type == Event.TURN_SUBAGENT_REQUESTED:
            subagent = cast(SubAgentRequestedPayload, payload)
            if subagent.step_id == step_id:
                subagent_requests[subagent.tool_call_id] = subagent
        elif event.type == Event.TURN_COMPLETED:
            completed = True
        elif event.type == Event.TURN_FAILED:
            failed = cast(TurnFailedPayload, payload)

    if next_prompt is None:
        raise ValueError(f"No prompt found for turn: {current_turn_id}")
    if agent_path is None:
        raise ValueError(
            f"Conversation settings not found in {conversation_stream}"
        )

    return Conversation(
        agent_path=agent_path,
        model=model,
        next_prompt=next_prompt,
        current_turn_id=current_turn_id,
        history=history,
        deps=deps,
        usage=usage,
        turn_messages=turn_messages,
        tool_requests=requests,
        tool_step_id=step_id,
        tool_results=tool_results,
        subagent_requests=subagent_requests,
        parent=parent,
        completed=completed,
        failed=failed,
    )
