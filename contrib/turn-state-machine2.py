"""Payload-driven turn machines with tools and child delegation.

Run with: .venv/bin/python contrib/turn-state-machine2.py

Pydantic transitions hold start/stop payloads and implement emit(). A TypeAdapter
selects the edge; the machine retains its latest payload, request, and results.
Transitions return one DispatchPayloadType or None. Tools run sequentially so
each transition can emit a single action. The caller executes agents/tools,
persists outcomes, sets up children, and publishes dispatch hints.
Child completion emits a parent poke; the caller reads the child outcome and
passes its payload to the parent machine. No terminal Redis ID is needed here.

This is an in-memory sketch, not worker integration. History accumulation,
durable child intent (D74), replay, and concurrent execution are not modeled.
"""

from abc import ABC, abstractmethod
from typing import Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import ToolCallPart
from pydantic_ai.usage import RunUsage

from fallingwater.models import (
    AgentResumePayload,
    ConversationPayloadType,
    ConversationPokePayload,
    DispatchPayloadType,
    ParentToolReference,
    AgentRequestedPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)


requests_adapter = TypeAdapter(DeferredToolRequests)


TurnPayload: TypeAlias = (
    TurnQueuedPayload
    | ToolRequestedPayload
    | ToolResultPayload
    | AgentRequestedPayload
    | TurnCompletedPayload
    | TurnFailedPayload
)


def call_ids(request: ToolRequestedPayload) -> tuple[str, ...]:
    requests = requests_adapter.validate_python(request.requests)
    if requests.approvals:
        raise NotImplementedError("Tool approvals are not modeled")
    ids = tuple(call.tool_call_id for call in requests.calls)
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("Tool requests need nonempty, unique IDs")
    return ids


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

    @abstractmethod
    def emit(self, machine: "TurnStateMachine") -> DispatchPayloadType | None:
        raise NotImplementedError


class StartTurn(Transition):
    start: None = None
    stop: TurnQueuedPayload

    def emit(self, machine: "TurnStateMachine") -> ConversationPokePayload:
        return ConversationPokePayload(
            conversation_id=machine.conversation_id, turn_id=self.stop.turn_id
        )


class RequestTools(Transition):
    start: TurnQueuedPayload | ToolResultPayload
    stop: ToolRequestedPayload

    def emit(self, machine: "TurnStateMachine") -> ToolCallPayload:
        machine.require_agent_ready()
        if machine.request is not None and (
            self.stop.step_id == machine.request.step_id
        ):
            raise ValueError("Repeated tool request step")
        return ToolCallPayload(
            conversation_id=machine.conversation_id,
            turn_id=self.stop.turn_id,
            step_id=self.stop.step_id,
            tool_call_id=call_ids(self.stop)[0],
        )


class ResolveTool(Transition):
    start: ToolRequestedPayload | ToolResultPayload
    stop: ToolResultPayload

    def emit(self, machine: "TurnStateMachine") -> DispatchPayloadType:
        return machine.action_after_result(self.stop)


class DelegateTool(Transition):
    start: ToolRequestedPayload | ToolResultPayload
    stop: AgentRequestedPayload

    def emit(self, machine: "TurnStateMachine") -> ConversationPokePayload:
        machine.require_pending_call(
            self.stop.turn_id, self.stop.step_id, self.stop.tool_call_id
        )
        if self.stop.child_conversation_id == machine.conversation_id:
            raise ValueError("Child needs a separate conversation")
        if self.stop.child_turn_id == self.stop.turn_id:
            raise ValueError("Child needs a distinct turn ID")
        return ConversationPokePayload(
            conversation_id=self.stop.child_conversation_id,
            turn_id=self.stop.child_turn_id,
        )


class ChildOutcome(Transition):
    start: AgentRequestedPayload
    stop: TurnCompletedPayload | TurnFailedPayload

    @model_validator(mode="after")
    def validate_turn_ids(self) -> Self:
        if self.start.child_turn_id != self.stop.turn_id:
            raise ValueError("Outcome must belong to the delegated child turn")
        return self


class ChildCompleted(ChildOutcome):
    stop: TurnCompletedPayload

    @property
    def outcome(self) -> ToolResultPayload:
        # The parent's next node is its tool result, not the child's terminal.
        return ToolResultPayload(
            turn_id=self.start.turn_id,
            step_id=self.start.step_id,
            tool_call_id=self.start.tool_call_id,
            result=self.stop.message,
        )

    def emit(self, machine: "TurnStateMachine") -> DispatchPayloadType:
        return machine.action_after_result(self.outcome)


class ChildFailed(ChildOutcome):
    stop: TurnFailedPayload

    @property
    def outcome(self) -> TurnFailedPayload:
        return TurnFailedPayload(
            turn_id=self.start.turn_id,
            error=f"Subagent failed: {self.stop.error}",
        )

    def emit(self, machine: "TurnStateMachine") -> ConversationPokePayload | None:
        return machine.notify_parent()


class AnswerTurn(Transition):
    start: TurnQueuedPayload | ToolResultPayload
    stop: TurnCompletedPayload

    def emit(self, machine: "TurnStateMachine") -> ConversationPokePayload | None:
        machine.require_agent_ready()
        return machine.notify_parent()


class FailTurn(Transition):
    start: (
        TurnQueuedPayload
        | ToolRequestedPayload
        | ToolResultPayload
        | AgentRequestedPayload
    )
    stop: TurnFailedPayload

    def emit(self, machine: "TurnStateMachine") -> ConversationPokePayload | None:
        return machine.notify_parent()


class FinishTurn(Transition):
    start: TurnCompletedPayload | TurnFailedPayload
    stop: None = None

    def emit(self, machine: "TurnStateMachine") -> None:
        return None


transition_adapter = TypeAdapter(
    StartTurn
    | RequestTools
    | ResolveTool
    | DelegateTool
    | ChildCompleted
    | ChildFailed
    | AnswerTurn
    | FailTurn
    | FinishTurn
)


class TurnStateMachine:
    def __init__(
        self,
        conversation_id: str,
        turn_id: str,
        *,
        parent: ParentToolReference | None = None,
    ) -> None:
        self.conversation_id = conversation_id
        self.turn_id = turn_id
        self.parent = parent
        self._state: TurnPayload | None = None
        self.request: ToolRequestedPayload | None = None
        self.results: tuple[ToolResultPayload, ...] = ()

    @property
    def state(self) -> TurnPayload | None:
        return self._state

    @property
    def done(self) -> bool:
        return isinstance(
            self._state, (TurnCompletedPayload, TurnFailedPayload)
        )

    @property
    def pending_call(self) -> str | None:
        if self.request is None:
            return None
        resolved = {result.tool_call_id for result in self.results}
        return next(
            (
                call_id for call_id in call_ids(self.request)
                if call_id not in resolved
            ),
            None,
        )

    def require_agent_ready(self) -> None:
        if self.pending_call is not None:
            raise ValueError("Agent must wait for all tool results")

    def require_pending_call(
        self, turn_id: str, step_id: str, tool_call_id: str
    ) -> None:
        if (
            self.request is None
            or turn_id != self.request.turn_id
            or step_id != self.request.step_id
            or tool_call_id != self.pending_call
        ):
            raise ValueError("Outcome must match the pending tool call")

    def action_after_result(
        self, result: ToolResultPayload
    ) -> DispatchPayloadType:
        self.require_pending_call(
            result.turn_id, result.step_id, result.tool_call_id
        )
        if self.request is None:
            raise ValueError("Tool result needs an active request")
        resolved = {item.tool_call_id for item in (*self.results, result)}
        next_call = next(
            (item for item in call_ids(self.request) if item not in resolved),
            None,
        )
        if next_call is None:
            return AgentResumePayload(
                conversation_id=self.conversation_id,
                turn_id=result.turn_id,
                step_id=result.step_id,
            )
        return ToolCallPayload(
            conversation_id=self.conversation_id,
            turn_id=result.turn_id,
            step_id=result.step_id,
            tool_call_id=next_call,
        )

    def notify_parent(self) -> ConversationPokePayload | None:
        if self.parent is None:
            return None
        return ConversationPokePayload(
            conversation_id=self.parent.conversation_id,
            turn_id=self.parent.turn_id,
        )

    def transition(
        self, payload: ConversationPayloadType
    ) -> DispatchPayloadType | None:
        if self.done:
            raise ValueError(f"Turn is already terminal: {self._state!r}")
        try:
            edge = transition_adapter.validate_python(
                {"start": self._state, "stop": payload}
            )
        except ValidationError as error:
            raise ValueError(
                f"Cannot apply {payload!r} in state {self._state!r}"
            ) from error
        action = edge.emit(self)
        outcome = edge.outcome
        if isinstance(outcome, ToolRequestedPayload):
            self.request = outcome
            self.results = ()
        elif isinstance(outcome, ToolResultPayload):
            self.results = (*self.results, outcome)
        self._state = outcome
        return action


def tool_request(
    turn_id: str, step_id: str, calls: list[ToolCallPart]
) -> ToolRequestedPayload:
    return ToolRequestedPayload(
        turn_id=turn_id,
        step_id=step_id,
        requests=requests_adapter.dump_python(
            DeferredToolRequests(calls=calls), mode="json"
        ),
        step_messages=[],
        usage=RunUsage(),
    )


def main() -> None:
    def step(
        machine: TurnStateMachine, payload: ConversationPayloadType
    ) -> DispatchPayloadType | None:
        action = machine.transition(payload)
        print(
            f"{machine.turn_id}: {type(payload).__name__} -> "
            f"{type(machine.state).__name__}; action={action}"
        )
        return action

    def assert_rejected(
        machine: TurnStateMachine, payload: ConversationPayloadType
    ) -> None:
        previous = machine.state
        request = machine.request
        results = machine.results
        try:
            machine.transition(payload)
        except ValueError:
            print(f"{machine.turn_id}: rejected {type(payload).__name__}")
        else:
            raise AssertionError("Invalid transition must fail")
        assert machine.state == previous
        assert machine.request == request
        assert machine.results == results

    parent = TurnStateMachine("parent", "parent-turn")
    assert step(
        parent,
        TurnQueuedPayload(
            turn_id="parent-turn",
            message="Multiply 6 * 7 and ask a child for 2 + 3",
        ),
    ) == ConversationPokePayload(
        conversation_id="parent", turn_id="parent-turn"
    )
    assert step(
        parent,
        tool_request(
            "parent-turn",
            "parent-step",
            [
                ToolCallPart(
                    "multiply", {"left": 6, "right": 7}, tool_call_id="call-1"
                ),
                ToolCallPart(
                    "delegate", {"prompt": "Add 2 + 3"}, tool_call_id="call-2"
                ),
            ],
        ),
    ) == ToolCallPayload(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="call-1",
    )
    result = ToolResultPayload(
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="call-1",
        result=6 * 7,
    )
    assert step(parent, result) == ToolCallPayload(
        conversation_id="parent",
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="call-2",
    )
    assert_rejected(parent, result)

    delegation = AgentRequestedPayload(
        turn_id="parent-turn",
        step_id="parent-step",
        tool_call_id="call-2",
        child_conversation_id="child",
        child_turn_id="child-turn",
    )
    assert step(parent, delegation) == ConversationPokePayload(
        conversation_id="child", turn_id="child-turn"
    )
    assert_rejected(parent, delegation)

    # The caller materializes the child from the tool's delegation result.
    # The current parent request alone lacks the durable intent required by D74.
    child = TurnStateMachine(
        "child",
        "child-turn",
        parent=ParentToolReference(
            conversation_id="parent",
            turn_id="parent-turn",
            step_id="parent-step",
            tool_call_id="call-2",
        ),
    )
    assert step(
        child, TurnQueuedPayload(turn_id="child-turn", message="Add 2 + 3")
    ) == ConversationPokePayload(conversation_id="child", turn_id="child-turn")
    assert step(
        child,
        tool_request(
            "child-turn",
            "child-step",
            [
                ToolCallPart(
                    "add", {"left": 2, "right": 3}, tool_call_id="child-call"
                ),
            ],
        ),
    ) == ToolCallPayload(
        conversation_id="child",
        turn_id="child-turn",
        step_id="child-step",
        tool_call_id="child-call",
    )
    assert step(
        child,
        ToolResultPayload(
            turn_id="child-turn",
            step_id="child-step",
            tool_call_id="child-call",
            result=2 + 3,
        ),
    ) == AgentResumePayload(
        conversation_id="child", turn_id="child-turn", step_id="child-step"
    )
    outcome = TurnCompletedPayload(
        turn_id="child-turn", message="5", turn_messages=[], usage=RunUsage()
    )
    assert step(child, outcome) == ConversationPokePayload(
        conversation_id="parent", turn_id="parent-turn"
    )
    assert_rejected(child, outcome)

    # Receiving the parent poke makes the caller read and forward child facts.
    assert step(parent, outcome) == AgentResumePayload(
        conversation_id="parent", turn_id="parent-turn", step_id="parent-step"
    )
    assert isinstance(parent.state, ToolResultPayload)
    assert [result.result for result in parent.results] == [42, "5"]
    assert (
        step(
            parent,
            TurnCompletedPayload(
                turn_id="parent-turn",
                message="47",
                turn_messages=[],
                usage=RunUsage(),
            ),
        )
        is None
    )
    assert isinstance(parent.state, TurnCompletedPayload)
    assert isinstance(child.state, TurnCompletedPayload)
    assert_rejected(parent, outcome)


if __name__ == "__main__":
    main()
