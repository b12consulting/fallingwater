"""Pure conversation workflow decisions and replay."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from .models import (
    AgentResumePayload,
    ConversationCreatedPayload,
    ConversationEvent,
    DispatchPayloadType,
    ParentToolReference,
    AgentRequestedPayload,
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
    agent_by_name: dict[str, str] = Field(default_factory=dict)
    last_event_id: str = "0-0"
    _history: MessageHistory = PrivateAttr(default_factory=MessageHistory)
    _latest_completed: TurnStateMachine | None = PrivateAttr(default=None)
    _agent_specs: dict[str, AgentRequestedPayload] = PrivateAttr(
        default_factory=dict
    )

    def prepare(self, payload: TurnPayload) -> PreparedEvent:
        """Validate a proposed fact without registering or advancing a turn."""
        turn = self.turn_state.get(payload.turn_id)
        if turn is None:
            if not isinstance(payload, TurnQueuedPayload):
                raise ValueError(f"No prompt found for turn: {payload.turn_id}")
            turn = TurnStateMachine(
                self.conversation_id,
                payload.turn_id,
                parent=payload.parent or self.settings.parent,
            )
        if isinstance(payload, AgentRequestedPayload):
            self._validate_registration(payload)
        return PreparedEvent(payload, turn.preview(payload))

    def _validate_registration(self, payload: AgentRequestedPayload) -> None:
        if payload.persistent and payload.name is not None:
            registered = self.agent_by_name.get(payload.name)
            if registered not in (None, payload.child_conversation_id):
                raise ValueError(
                    f"Agent name is already registered: {payload.name}"
                )
            previous = self._agent_specs.get(payload.name)
            if previous is not None and not previous.has_same_settings(payload):
                raise ValueError("Registered agent settings cannot change")

    def agent_spec(self, name: str) -> AgentRequestedPayload:
        return self._agent_specs[name]

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
        request: AgentRequestedPayload,
    ) -> tuple[ConversationCreatedPayload, TurnQueuedPayload, PreparedEvent]:
        prepared = self.prepare(request)
        settings, queued = self.subagent_payloads(request)
        return settings, queued, prepared

    def subagent_payloads(
        self,
        request: AgentRequestedPayload,
    ) -> tuple[ConversationCreatedPayload, TurnQueuedPayload]:
        parent = ParentToolReference(
            conversation_id=self.conversation_id,
            turn_id=request.turn_id,
            step_id=request.step_id,
            tool_call_id=request.tool_call_id,
        )
        settings = ConversationCreatedPayload(
            agent=request.agent_path,
            model=request.model,
            deps=request.deps,
            parent=None if request.persistent else parent,
        )
        queued = TurnQueuedPayload(
            turn_id=request.child_turn_id,
            message=request.prompt,
            parent=parent,
        )
        return settings, queued

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
                parent=payload.parent or self.settings.parent,
                history=self._history,
                usage=(
                    self._latest_completed.usage
                    if self._latest_completed is not None
                    else None
                ),
            )
        turn = self.turn_state[payload.turn_id]
        was_running = turn.running
        if isinstance(payload, AgentRequestedPayload) and was_running:
            self._validate_registration(payload)
        turn.replay(event)
        if (
            isinstance(payload, AgentRequestedPayload)
            and payload.persistent
            and payload.name is not None
            and turn.delegations.get(payload.tool_call_id) == payload
        ):
            self.agent_by_name[payload.name] = payload.child_conversation_id
            self._agent_specs.setdefault(payload.name, payload)
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
