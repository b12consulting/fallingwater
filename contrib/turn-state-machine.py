"""Minimal stateful turn machine, without Redis or an actual agent.

Run with: .venv/bin/python contrib/turn-state-machine.py

Pydantic transition models hold actual payloads as start/stop.
TypeAdapter selects the matching model from a dictionary of those endpoints.
None marks the initial boundary or a terminal boundary. The machine retains
the latest payload; an edge with stop=None marks it as final without requiring
another input. The caller passes a decoded conversation event's payload.
Each transition's emit method describes its outgoing action. The caller
publishes messages; the machine performs no Redis I/O.
"""

from abc import ABC, abstractmethod
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from pydantic_ai.usage import RunUsage

from fallingwater.models import (
    Action,
    ConversationPayloadType,
    ConversationPokePayload,
    DispatchPayloadType,
    DispatchEvent,
    TurnCompletedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    encode_dispatch_event,
    decode_dispatch_event,
)


class Transition(BaseModel, ABC):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    start: TurnQueuedPayload | TurnCompletedPayload | None
    stop: TurnQueuedPayload | TurnCompletedPayload | None

    @model_validator(mode="after")
    def validate_turn_ids(self) -> Self:
        if (
            self.start is not None
            and self.stop is not None
            and self.start.turn_id != self.stop.turn_id
        ):
            raise ValueError("Transition endpoints must belong to the same turn")
        return self

    @abstractmethod
    def emit(self, conversation_id: str) -> DispatchPayloadType | None:
        """Describe the dispatch action enabled by this transition."""
        raise NotImplementedError


class StartTurn(Transition):
    start: None = None
    stop: TurnQueuedPayload

    def emit(self, conversation_id: str) -> ConversationPokePayload:
        return ConversationPokePayload(
            conversation_id=conversation_id, turn_id=self.stop.turn_id
        )


class AnswerTurn(Transition):
    start: TurnQueuedPayload
    stop: TurnCompletedPayload

    def emit(self, conversation_id: str) -> None:
        return None


class FinishTurn(Transition):
    start: TurnCompletedPayload
    stop: None = None

    def emit(self, conversation_id: str) -> None:
        return None


transition_adapter = TypeAdapter(StartTurn | AnswerTurn | FinishTurn)


class TurnStateMachine:
    def __init__(self, conversation_id: str, turn_id: str) -> None:
        self.conversation_id = conversation_id
        self.turn_id = turn_id
        self._state: ConversationPayloadType | None = None

    @property
    def state(self) -> ConversationPayloadType | None:
        return self._state

    @property
    def done(self) -> bool:
        return isinstance(self._state, TurnCompletedPayload)

    def transition(
        self, payload: ConversationPayloadType
    ) -> DispatchPayloadType | None:
        """Apply a valid input payload and return a dispatch payload or None."""
        if self.done:
            raise ValueError(
                f"Cannot apply {payload!r} in state {self._state!r}"
            )
        try:
            edge = transition_adapter.validate_python(
                {"start": self._state, "stop": payload}
            )
        except ValidationError as error:
            raise ValueError(
                f"Cannot apply {payload!r} in state {self._state!r}"
            ) from error
        message = edge.emit(self.conversation_id)
        self._state = edge.stop
        return message


def main() -> None:
    machine = TurnStateMachine(conversation_id="abc", turn_id="turn-1")

    def assert_rejected(payload: ConversationPayloadType) -> None:
        previous = machine.state
        try:
            machine.transition(payload)
        except ValueError as error:
            print("Rejected transition:", error)
        else:
            raise AssertionError("An invalid transition must fail")
        assert machine.state == previous

    queued = TurnQueuedPayload(turn_id="turn-1", message="What is 6 * 7?")
    completed = TurnCompletedPayload(
        turn_id="turn-1",
        message="42",
        turn_messages=[],
        usage=RunUsage(),
    )
    other_turn = TurnQueuedPayload(turn_id="turn-2", message="Another turn")

    for start, stop, expected in (
        (None, queued, StartTurn),
        (queued, completed, AnswerTurn),
        (completed, None, FinishTurn),
    ):
        selected = transition_adapter.validate_python(
            {"start": start, "stop": stop}
        )
        assert type(selected) is expected
        assert selected.start == start
        assert selected.stop == stop

    print("Initial state:", machine.state)
    assert machine.state is None
    assert not machine.done
    assert_rejected(completed)
    assert_rejected(other_turn)

    message = machine.transition(queued)
    print("Awaiting answer:", machine.state, "Message:", message)
    assert machine.state == queued
    assert not machine.done
    assert message == ConversationPokePayload(
        conversation_id="abc", turn_id="turn-1"
    )
    assert isinstance(message, ConversationPokePayload)
    fields = encode_dispatch_event(Action.CONVERSATION_POKE, message)
    dispatch_event = decode_dispatch_event("1-0", fields)
    assert dispatch_event == DispatchEvent(
        entry_id="1-0", type=Action.CONVERSATION_POKE, payload=message
    )
    assert_rejected(queued)
    assert_rejected(completed.model_copy(update={"turn_id": "turn-2"}))

    message = machine.transition(completed)
    print("Final state:", machine.state, "Message:", message)
    assert machine.state == completed
    assert machine.done
    assert message is None

    assert_rejected(completed)
    assert_rejected(queued)


if __name__ == "__main__":
    main()
