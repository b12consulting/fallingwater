"""Payload transitions and pure reconstruction of conversation turns."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Literal, Self, TypeAlias, overload

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    model_validator,
)
from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ToolCallPart,
)
from pydantic_ai.usage import RunUsage

from .models import (
    AgentResumePayload,
    ConversationCreatedPayload,
    ConversationEvent,
    ConversationPokePayload,
    DispatchPayloadType,
    ParentToolReference,
    SubAgentRequestedPayload,
    SubAgentResultPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)


TurnPayload: TypeAlias = (
    TurnQueuedPayload
    | ToolRequestedPayload
    | ToolResultPayload
    | SubAgentRequestedPayload
    | TurnCompletedPayload
    | TurnFailedPayload
)
ActivePayload: TypeAlias = (
    TurnQueuedPayload
    | ToolRequestedPayload
    | ToolResultPayload
    | SubAgentRequestedPayload
)
requests_adapter = TypeAdapter(DeferredToolRequests)


@dataclass(frozen=True, eq=False)
class MessageHistory(Sequence[ModelMessage]):
    """An immutable prefix chain shared by queue-time history snapshots."""

    previous: "MessageHistory | None" = None
    messages: tuple[ModelMessage, ...] = ()
    _length: int = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "_length",
            (len(self.previous) if self.previous is not None else 0)
            + len(self.messages),
        )

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[ModelMessage]:
        prefixes: list[MessageHistory] = []
        current: MessageHistory | None = self
        while current is not None:
            prefixes.append(current)
            current = current.previous
        for prefix in reversed(prefixes):
            yield from prefix.messages

    @overload
    def __getitem__(self, index: int) -> ModelMessage: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[ModelMessage]: ...

    def __getitem__(
        self, index: int | slice
    ) -> ModelMessage | Sequence[ModelMessage]:
        return list(self)[index]

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Sequence)
            and len(self) == len(other)
            and all(left == right for left, right in zip(self, other))
        )


@dataclass(frozen=True)
class ParentNotification:
    """Bind the terminal Redis ID only after the outcome has been persisted."""

    parent: ParentToolReference
    conversation_id: str
    turn_id: str

    def dispatch(self, entry_id: str) -> SubAgentResultPayload:
        return SubAgentResultPayload(
            conversation_id=self.parent.conversation_id,
            turn_id=self.parent.turn_id,
            step_id=self.parent.step_id,
            tool_call_id=self.parent.tool_call_id,
            child_conversation_id=self.conversation_id,
            child_turn_id=self.turn_id,
            child_terminal_event_id=entry_id,
        )


Emission: TypeAlias = DispatchPayloadType | ParentNotification


class Transition(BaseModel, ABC):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    start: TurnPayload | None
    stop: TurnPayload | None

    @model_validator(mode="after")
    def validate_turn_ids(self) -> Self:
        if (
            self.start is not None
            and self.stop is not None
            and self.start.turn_id != self.stop.turn_id
        ):
            raise ValueError("Transition endpoints must belong to the same turn")
        return self

    @property
    def outcome(self) -> TurnPayload | None:
        return self.stop

    def validate_context(self, machine: "TurnStateMachine") -> None:
        """Check invariants requiring context beyond the payload endpoints."""

    @staticmethod
    def require_agent_ready(machine: "TurnStateMachine") -> None:
        if set(machine.calls) != set(machine.results):
            raise ValueError("Agent must wait for all tool results")

    @staticmethod
    def require_pending_call(
        machine: "TurnStateMachine", step_id: str, tool_call_id: str
    ) -> None:
        if (
            machine.request is None
            or machine.request.step_id != step_id
            or tool_call_id not in machine.calls
            or tool_call_id in machine.results
        ):
            raise ValueError("Outcome must match a pending tool call")

    @abstractmethod
    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        raise NotImplementedError


class StartTurn(Transition):
    start: None = None
    stop: TurnQueuedPayload

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return (
            ConversationPokePayload(
                conversation_id=machine.conversation_id,
                turn_id=self.stop.turn_id,
            ),
        )


class RequestTools(Transition):
    start: ActivePayload
    stop: ToolRequestedPayload

    @model_validator(mode="after")
    def validate_requests(self) -> Self:
        requests = requests_adapter.validate_python(self.stop.requests)
        if requests.approvals:
            raise ValueError(
                "Fallingwater does not handle deferred tool approvals"
            )
        ids = [call.tool_call_id for call in requests.calls]
        if not ids:
            raise ValueError("deferred tool request set must contain a call")
        if len(ids) != len(set(ids)):
            raise ValueError("deferred tool call IDs must be unique")
        ModelMessagesTypeAdapter.validate_python(self.stop.step_messages)
        return self

    def validate_context(self, machine: "TurnStateMachine") -> None:
        self.require_agent_ready(machine)
        if self.stop.step_id in machine.step_ids:
            raise ValueError("Repeated tool request step")

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return tuple(
            ToolCallPayload(
                conversation_id=machine.conversation_id,
                turn_id=self.stop.turn_id,
                step_id=self.stop.step_id,
                tool_call_id=call.tool_call_id,
            )
            for call in requests_adapter.validate_python(self.stop.requests).calls
        )


class ResolveTool(Transition):
    start: ToolRequestedPayload | ToolResultPayload | SubAgentRequestedPayload
    stop: ToolResultPayload

    def validate_context(self, machine: "TurnStateMachine") -> None:
        self.require_pending_call(
            machine, self.stop.step_id, self.stop.tool_call_id
        )

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        if set(machine.calls) == {*machine.results, self.stop.tool_call_id}:
            return (
                AgentResumePayload(
                    conversation_id=machine.conversation_id,
                    turn_id=self.stop.turn_id,
                    step_id=self.stop.step_id,
                ),
            )
        return ()


class DelegateTool(Transition):
    start: ToolRequestedPayload | ToolResultPayload | SubAgentRequestedPayload
    stop: SubAgentRequestedPayload

    def validate_context(self, machine: "TurnStateMachine") -> None:
        self.require_pending_call(
            machine, self.stop.step_id, self.stop.tool_call_id
        )
        if self.stop.tool_call_id in machine.delegations:
            raise ValueError("Tool call is already delegated")
        if self.stop.child_conversation_id == machine.conversation_id:
            raise ValueError("Child needs a separate conversation")
        if self.stop.child_turn_id == self.stop.turn_id:
            raise ValueError("Child needs a distinct turn ID")

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return (
            ConversationPokePayload(
                conversation_id=self.stop.child_conversation_id,
                turn_id=self.stop.child_turn_id,
            ),
        )


class ChildOutcome(Transition):
    start: SubAgentRequestedPayload
    stop: TurnCompletedPayload | TurnFailedPayload

    @model_validator(mode="after")
    def validate_turn_ids(self) -> Self:
        if self.start.child_turn_id != self.stop.turn_id:
            raise ValueError("Outcome must belong to the delegated child turn")
        return self

    def validate_context(self, machine: "TurnStateMachine") -> None:
        self.require_pending_call(
            machine, self.start.step_id, self.start.tool_call_id
        )


class ChildCompleted(ChildOutcome):
    stop: TurnCompletedPayload

    @property
    def outcome(self) -> ToolResultPayload:
        return ToolResultPayload(
            turn_id=self.start.turn_id,
            step_id=self.start.step_id,
            tool_call_id=self.start.tool_call_id,
            result=self.stop.message,
        )

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return ResolveTool(start=self.start, stop=self.outcome).emit(machine)


class ChildFailed(ChildOutcome):
    stop: TurnFailedPayload

    @property
    def outcome(self) -> TurnFailedPayload:
        return TurnFailedPayload(
            turn_id=self.start.turn_id, error=f"Subagent failed: {self.stop.error}"
        )

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return machine.notify_parent()


class AnswerTurn(Transition):
    start: ActivePayload
    stop: TurnCompletedPayload

    @model_validator(mode="after")
    def validate_messages(self) -> Self:
        ModelMessagesTypeAdapter.validate_python(self.stop.turn_messages)
        return self

    def validate_context(self, machine: "TurnStateMachine") -> None:
        self.require_agent_ready(machine)

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return machine.notify_parent()


class FailTurn(Transition):
    start: ActivePayload
    stop: TurnFailedPayload

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return machine.notify_parent()


class FinishTurn(Transition):
    start: TurnCompletedPayload | TurnFailedPayload
    stop: None = None

    def emit(self, machine: "TurnStateMachine") -> tuple[Emission, ...]:
        return ()


transition_adapter = TypeAdapter(
    StartTurn
    | RequestTools
    | ResolveTool
    | DelegateTool
    | AnswerTurn
    | FailTurn
    | FinishTurn
)
child_transition_adapter = TypeAdapter(ChildCompleted | ChildFailed)


class TurnStateMachine:
    """One turn's payload graph and context, with no persistence or execution."""

    def __init__(
        self,
        conversation_id: str,
        turn_id: str,
        *,
        parent: ParentToolReference | None = None,
        history: Sequence[ModelMessage] = (),
        usage: RunUsage | None = None,
    ) -> None:
        self.conversation_id = conversation_id
        self.turn_id = turn_id
        self.parent = parent
        self.history = (
            history
            if isinstance(history, MessageHistory)
            else MessageHistory(messages=tuple(history))
        )
        self.usage = deepcopy(usage)
        self.state: TurnPayload | None = None
        self.queued: TurnQueuedPayload | None = None
        self.request: ToolRequestedPayload | None = None
        self.calls: dict[str, ToolCallPart] = {}
        self.results: dict[str, ToolResultPayload] = {}
        self.delegations: dict[str, SubAgentRequestedPayload] = {}
        self.step_ids: set[str] = set()
        self.turn_messages: list[ModelMessage] = []
        self.terminal_event_id: str | None = None

    @property
    def completed(self) -> bool:
        return isinstance(self.state, TurnCompletedPayload)

    @property
    def failed(self) -> bool:
        return isinstance(self.state, TurnFailedPayload)

    @property
    def running(self) -> bool:
        """Nonterminal, including not-yet-started turns and waiting turns."""
        return not (self.completed or self.failed)

    @property
    def prompt(self) -> str:
        if self.queued is None:
            raise ValueError("Turn has no queued prompt")
        return self.queued.message

    @property
    def requests(self) -> DeferredToolRequests | None:
        if self.request is None:
            return None
        return requests_adapter.validate_python(self.request.requests)

    @property
    def tool_results(self) -> dict[str, object]:
        return {key: value.result for key, value in self.results.items()}

    def notify_parent(self) -> tuple[Emission, ...]:
        if self.parent is None:
            return ()
        return (
            ParentNotification(
                self.parent, self.conversation_id, self.turn_id
            ),
        )

    def dispatch_status(
        self, action: DispatchPayloadType
    ) -> Literal["ready", "wait", "stale"]:
        """Stale hints are acknowledged; premature resumes remain pending."""
        if (
            not self.running
            or action.conversation_id != self.conversation_id
            or action.turn_id != self.turn_id
        ):
            return "stale"
        if isinstance(action, ConversationPokePayload):
            return "ready" if self.request is None else "stale"
        if self.request is None or self.request.step_id != action.step_id:
            return "stale"
        if isinstance(action, AgentResumePayload):
            return "ready" if set(self.calls) == set(self.results) else "wait"
        if (
            action.tool_call_id not in self.calls
            or action.tool_call_id in self.results
        ):
            return "stale"
        request = self.delegations.get(action.tool_call_id)
        if isinstance(action, ToolCallPayload):
            return "stale" if request is not None else "ready"
        if (
            request is None
            or request.child_conversation_id != action.child_conversation_id
            or request.child_turn_id != action.child_turn_id
        ):
            return "stale"
        return "ready"

    def accepts_result(self, payload: ToolResultPayload) -> bool:
        return (
            self.running
            and self.request is not None
            and payload.turn_id == self.turn_id
            and payload.step_id == self.request.step_id
            and payload.tool_call_id not in self.results
        )

    def resume_actions(self, step_id: str) -> tuple[Emission, ...]:
        action = AgentResumePayload(
            conversation_id=self.conversation_id,
            turn_id=self.turn_id,
            step_id=step_id,
        )
        return (action,) if self.dispatch_status(action) == "ready" else ()

    def child_outcome(
        self,
        action: SubAgentResultPayload,
        event: ConversationEvent,
    ) -> ToolResultPayload | TurnFailedPayload | None:
        """Normalize an exact, correlated child terminal fact for persistence."""
        if self.dispatch_status(action) != "ready":
            return None
        if event.entry_id != action.child_terminal_event_id:
            raise ValueError("Child outcome has the wrong terminal event ID")
        edge = child_transition_adapter.validate_python(
            {
                "start": self.delegations[action.tool_call_id],
                "stop": event.payload,
            }
        )
        edge.validate_context(self)
        return edge.outcome

    def preview(self, payload: TurnPayload) -> tuple[Emission, ...]:
        """Validate and emit without making an unpersisted fact current."""
        if not self.running:
            raise ValueError(f"Turn is already terminal: {self.turn_id}")
        edge = transition_adapter.validate_python(
            {"start": self.state, "stop": payload}
        )
        edge.validate_context(self)
        return edge.emit(self)

    def transition(self, payload: TurnPayload) -> tuple[Emission, ...]:
        actions = self.preview(payload)
        # All validation precedes mutation, including message/request decoding.
        if isinstance(payload, ToolRequestedPayload):
            requests = requests_adapter.validate_python(payload.requests)
            messages = ModelMessagesTypeAdapter.validate_python(
                payload.step_messages
            )
            self.request = payload
            self.calls = {call.tool_call_id: call for call in requests.calls}
            self.results = {}
            self.delegations = {}
            self.step_ids.add(payload.step_id)
            self.turn_messages.extend(messages)
            self.usage = deepcopy(payload.usage)
        elif isinstance(payload, ToolResultPayload):
            self.results[payload.tool_call_id] = payload
        elif isinstance(payload, SubAgentRequestedPayload):
            self.delegations[payload.tool_call_id] = payload
        elif isinstance(payload, TurnQueuedPayload):
            self.queued = payload
            self.turn_id = payload.turn_id
        elif isinstance(payload, TurnCompletedPayload):
            self.turn_messages = ModelMessagesTypeAdapter.validate_python(
                payload.turn_messages
            )
            self.usage = deepcopy(payload.usage)
        self.state = payload
        return actions

    def replay(self, event: ConversationEvent) -> None:
        """Keep canonical results and the first terminal fact during replay."""
        payload = event.payload
        if isinstance(payload, ConversationCreatedPayload):
            raise ValueError("Conversation settings are not a turn payload")
        if not self.running:
            return
        if (
            isinstance(payload, ToolResultPayload)
            and not self.accepts_result(payload)
        ):
            return
        if isinstance(payload, SubAgentRequestedPayload) and (
            not self.running
            or self.request is None
            or payload.step_id != self.request.step_id
            or self.delegations.get(payload.tool_call_id) == payload
        ):
            return
        if payload == self.queued or payload == self.request:
            return
        self.transition(payload)
        if not self.running:
            self.terminal_event_id = event.entry_id


class Conversation(BaseModel):
    """Conversation configuration, turn machines, and the replay cursor."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    conversation_id: str
    settings: ConversationCreatedPayload
    turn_state: dict[str, TurnStateMachine] = Field(default_factory=dict)
    last_event_id: str = "0-0"
    _history: MessageHistory = PrivateAttr(default_factory=MessageHistory)
    _latest_completed: TurnStateMachine | None = PrivateAttr(default=None)

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
        machine = self.turn_state[payload.turn_id]
        was_running = machine.running
        machine.replay(event)
        if was_running and machine.completed:
            self._history = MessageHistory(
                previous=self._history, messages=tuple(machine.turn_messages)
            )
            self._latest_completed = machine
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
        raise ValueError(f"Conversation settings not found in {conversation_id}")
    conversation = Conversation(
        conversation_id=conversation_id,
        settings=first.payload,
        last_event_id=first.entry_id,
    )
    for event in iterator:
        conversation.apply(event)
    return conversation
