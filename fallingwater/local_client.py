"""Conversation-bound client for the embedded local executor."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from .conversation import Conversation
from .local_executor import LocalExecutor
from .models import (
    ConversationCreatedPayload,
    ConversationEvent,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import check_agent


class LocalClient:
    """Submit sequential turns and read retained local events without printing.

    Each client owns its executor; use a context manager or ``close()`` to
    stop it. Use ``save()`` and ``load=True`` to retain
    settled history across processes. Queued timestamps reflect activation.
    """

    def __init__(
        self,
        *,
        conversation_id: str | None = None,
        agent_path: str | None = None,
        model: str | None = None,
        group: str = "user",
        deps: Mapping[str, Any] | None = None,
        load: bool = False,
        store: str = ".fw-store",
    ) -> None:
        if not group:
            raise ValueError("conversation reader group must be nonempty")
        self.executor = LocalExecutor()
        self._closed = False
        self.group = group
        with self.executor.condition:
            self.executor.check()
            if load:
                if conversation_id is None:
                    raise ValueError("loading requires a conversation ID")
                if deps:
                    raise ValueError(
                        "dependencies can only be set for a new conversation"
                    )
                self.conversation_id = self.executor.load(
                    conversation_id, store
                )
            else:
                if agent_path is None:
                    raise ValueError("agent is required for a new conversation")
                check_agent(agent_path, model)
                self.conversation_id = self.executor.create(
                    agent_path,
                    model=model,
                    deps=dict(deps or {}),
                    conversation_id=conversation_id,
                )
            self._session = self.executor.registry[self.conversation_id]
            self._session.acknowledged.setdefault(group, set())
            if load:
                self._session.acknowledged[group].update(self._session.events)

    def save(self, store: str = ".fw-store") -> int:
        """Save recorded history and descendants, including after closing."""
        return self.executor.save(self.conversation_id, store)

    @property
    def settings(self) -> ConversationCreatedPayload:
        with self.executor.condition:
            return self._session.conversation.settings

    @property
    def conversation(self) -> Conversation:
        """Return a replay snapshot, isolated from the running executor."""
        with self.executor.condition:
            return self._session.conversation.clone()

    def refresh(self) -> None:
        """Check availability; ``conversation`` already returns current state."""
        with self.executor.condition:
            self._check()

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError("Local client is closed")
        self.executor.check()

    def send(self, prompt: str) -> str:
        """Submit a new turn, starting background execution, and return its ID."""
        with self.executor.condition:
            self._check()
            turn_id = self.executor.send(self.conversation_id, prompt)
        self.executor.start()
        return turn_id

    def receive(self, turn_id: str) -> str:
        """Wait for a submitted turn, without consuming reader-group events."""
        with self.executor.condition:
            self._check()
            if (
                not any(
                    queued.turn_id == turn_id
                    for queued in self._session.pending
                )
                and turn_id not in self._session.conversation.turn_state
            ):
                raise ValueError(f"No prompt found for turn: {turn_id}")
            self.executor.condition.wait_for(
                lambda: self._terminal(turn_id) is not None
                or self._closed
                or self.executor._closed
                or self.executor._failure is not None
            )
            self._check()
            payload = self._terminal(turn_id)
            if isinstance(payload, TurnFailedPayload):
                raise RuntimeError(payload.error)
            if not isinstance(payload, TurnCompletedPayload):
                raise RuntimeError(f"No outcome found for turn: {turn_id}")
            return payload.message

    def _terminal(
        self, turn_id: str
    ) -> TurnCompletedPayload | TurnFailedPayload | None:
        turn = self._session.conversation.turn_state.get(turn_id)
        if turn is not None and isinstance(
            turn.state, (TurnCompletedPayload, TurnFailedPayload)
        ):
            return turn.state
        return None

    def ask(self, prompt: str) -> str:
        """Submit a new turn and wait for its result without printing."""
        return self.receive(self.send(prompt))

    def _available(self) -> list[ConversationEvent]:
        acknowledged = self._session.acknowledged[self.group]
        return [
            event
            for entry_id, event in self._session.events.items()
            if entry_id not in acknowledged
        ]

    def read(self, *, ffwd: bool = False) -> Iterator[ConversationEvent]:
        """Yield unacknowledged events; normally wait up to one second."""
        with self.executor.condition:
            self._check()
            if not ffwd:
                self.executor.condition.wait_for(
                    lambda: bool(self._available())
                    or self._closed
                    or self.executor._closed
                    or self.executor._failure is not None,
                    timeout=1,
                )
            self._check()
            events = self._available()
        yield from events

    def acknowledge(self, event: ConversationEvent) -> None:
        """Advance the selected group only after successful event handling."""
        with self.executor.condition:
            self._check()
            if event.entry_id not in self._session.events:
                raise ValueError(f"Unknown event: {event.entry_id}")
            self._session.acknowledged[self.group].add(event.entry_id)

    def queued_event_id(self, turn_id: str) -> str:
        """Find the recorded activation event, including acknowledged events."""
        with self.executor.condition:
            for event in self._session.events.values():
                if (
                    isinstance(event.payload, TurnQueuedPayload)
                    and event.payload.turn_id == turn_id
                ):
                    return event.entry_id
        raise ValueError(f"No prompt found for turn: {turn_id}")

    def rewind(self) -> None:
        """Replay retained events for this group without rerunning actions."""
        with self.executor.condition:
            self._check()
            self._session.acknowledged[self.group].clear()
            self.executor.condition.notify_all()

    def close(self) -> None:
        """Close this client and stop its executor."""
        with self.executor.condition:
            self._closed = True
            self.executor.condition.notify_all()
        self.executor.close()

    def __enter__(self) -> LocalClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
