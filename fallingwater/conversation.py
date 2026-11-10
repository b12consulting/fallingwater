"""Pure conversation workflow decisions and replay."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from .models import (
    AgentResumePayload,
    ConversationCreatedPayload,
    ConversationEvent,
    DispatchPayloadType,
    ParentToolReference,
    SubAgentRequestedPayload,
    SubAgentResultPayload,
    ToolResultPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .turn_state import (
    Emission,
    MessageHistory,
    ParentNotification,
    TurnPayload,
    TurnStateMachine,
)


@dataclass(frozen=True)
class PreparedEvent:
    """A validated fact and intents, without advancing canonical state."""

    payload: TurnPayload
    emissions: tuple[Emission, ...]

    def dispatch(self, entry_id: str) -> tuple[DispatchPayloadType, ...]:
        return tuple(
            emission.dispatch(entry_id)
            if isinstance(emission, ParentNotification)
            else emission
            for emission in self.emissions
        )


class Conversation(BaseModel):
    """Pure workflow decisions, turn machines, and the replay cursor."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    conversation_id: str
    settings: ConversationCreatedPayload
    turn_state: dict[str, TurnStateMachine] = Field(default_factory=dict)
    last_event_id: str = "0-0"
    _history: MessageHistory = PrivateAttr(default_factory=MessageHistory)
    _latest_completed: TurnStateMachine | None = PrivateAttr(default=None)

    def prepare(self, payload: TurnPayload) -> PreparedEvent:
        """Validate a proposed fact without registering or advancing a turn."""
        turn = self.turn_state.get(payload.turn_id)
        if turn is None:
            if not isinstance(payload, TurnQueuedPayload):
                raise ValueError(f"No prompt found for turn: {payload.turn_id}")
            turn = TurnStateMachine(
                self.conversation_id,
                payload.turn_id,
                parent=self.settings.parent,
            )
        return PreparedEvent(payload, turn.preview(payload))

    def dispatch_status(
        self, action: DispatchPayloadType
    ) -> Literal["ready", "wait", "stale"]:
        return self.turn_state[action.turn_id].dispatch_status(action)

    def accepts_result(self, payload: ToolResultPayload) -> bool:
        return self.turn_state[payload.turn_id].accepts_result(payload)

    def resume_actions(
        self, payload: ToolResultPayload
    ) -> tuple[AgentResumePayload, ...]:
        """Decide fan-in after the caller has applied the persisted tail."""
        return self.turn_state[payload.turn_id].resume_actions(payload.step_id)

    def child_outcome(
        self, action: SubAgentResultPayload, event: ConversationEvent
    ) -> ToolResultPayload | TurnFailedPayload | None:
        return self.turn_state[action.turn_id].child_outcome(action, event)

    def prepare_subagent(
        self,
        request: SubAgentRequestedPayload,
        *,
        agent: str,
        model: str,
        deps: dict[str, Any],
        prompt: str,
    ) -> tuple[ConversationCreatedPayload, TurnQueuedPayload, PreparedEvent]:
        prepared = self.prepare(request)
        settings = ConversationCreatedPayload(
            agent=agent,
            model=model,
            deps=deps,
            parent=ParentToolReference(
                conversation_id=self.conversation_id,
                turn_id=request.turn_id,
                step_id=request.step_id,
                tool_call_id=request.tool_call_id,
            ),
        )
        queued = TurnQueuedPayload(
            turn_id=request.child_turn_id, message=prompt
        )
        return settings, queued, prepared

    def apply(self, event: ConversationEvent) -> None:
        payload = event.payload
        if isinstance(payload, ConversationCreatedPayload):
            raise ValueError("Conversation settings were already loaded")

        if payload.turn_id not in self.turn_state:
            if not isinstance(payload, TurnQueuedPayload):
                raise ValueError(f"No prompt found for turn: {payload.turn_id}")
            self.turn_state[payload.turn_id] = TurnStateMachine(
                self.conversation_id,
                payload.turn_id,
                parent=self.settings.parent,
                history=self._history,
                usage=(
                    self._latest_completed.usage
                    if self._latest_completed is not None
                    else None
                ),
            )
        turn = self.turn_state[payload.turn_id]
        was_running = turn.running
        turn.replay(event)
        if was_running and turn.completed:
            self._history = MessageHistory(
                previous=self._history, messages=tuple(turn.turn_messages)
            )
            self._latest_completed = turn
        self.last_event_id = event.entry_id


def reconstruct_conversation(
    events: Iterable[ConversationEvent], *, conversation_id: str
) -> Conversation:
    """Replay all turns without publishing the transitions' dispatch hints."""
    iterator = iter(events)
    first = next(iterator, None)
    if first is None or not isinstance(
        first.payload, ConversationCreatedPayload
    ):
        raise ValueError(
            f"Conversation settings not found in {conversation_id}"
        )
    conversation = Conversation(
        conversation_id=conversation_id,
        settings=first.payload,
        last_event_id=first.entry_id,
    )
    for event in iterator:
        conversation.apply(event)
    return conversation
