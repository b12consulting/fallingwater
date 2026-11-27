"""Conversation-bound client for the embedded local executor."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from concurrent.futures import Future
from queue import Empty, Queue
from threading import Event as ThreadEvent
from threading import Lock, Thread, current_thread
from time import monotonic
from typing import Any
from uuid import uuid4

from . import archive
from .conversation import Conversation
from .local_executor import LocalExecutor
from .models import (
    ConversationCreatedPayload,
    ConversationEvent,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import check_agent, logger


class LocalClient:
    """Submit sequential turns and read retained local events without printing.

    Each client owns its executor and execution thread; use a context manager
    or ``close()`` to stop it. Use ``save()`` and ``load=True`` to retain
    settled history across processes. Queued timestamps reflect activation.
    Concurrent lifecycle and submission calls are unsupported. Reading events,
    acknowledgements, and snapshots may run on other threads.
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
        self._thread: Thread | None = None
        self._incoming: Queue[
            tuple[TurnQueuedPayload, Future[str]] | None
        ] = Queue()
        self._stop = ThreadEvent()
        self._lifecycle = Lock()
        self._failure: Exception | None = None
        self._results: dict[str, Future[str]] = {}
        self._acknowledged: set[str] = set()
        self.group = group
        if load:
            if conversation_id is None:
                raise ValueError("loading requires a conversation ID")
            if deps:
                raise ValueError(
                    "dependencies can only be set for a new conversation"
                )
            archive.load_local_conversation(
                self.executor, conversation_id, store
            )
            self.conversation_id = conversation_id
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
        if load:
            self._acknowledged.update(
                event.entry_id
                for event in self.executor.events(self.conversation_id)
            )

    def save(self, store: str = ".fw-store") -> int:
        """Save recorded history and descendants, including after closing."""
        return archive.save_local_conversation(
            self.executor, self.conversation_id, store
        )

    @property
    def settings(self) -> ConversationCreatedPayload:
        return self.executor.settings(self.conversation_id)

    @property
    def conversation(self) -> Conversation:
        """Return a replay snapshot, isolated from the running executor."""
        return self.executor.snapshot(self.conversation_id)

    def refresh(self) -> None:
        """Check availability; ``conversation`` already returns current state."""
        if self._stop.is_set():
            raise self._shutdown_error()

    def send(self, prompt: str) -> str:
        """Submit a new turn, starting background execution, and return its ID."""
        with self._lifecycle:
            self.refresh()
            if self._thread is None:
                thread = Thread(target=self._run, name="fw-local", daemon=True)
                thread.start()
                self._thread = thread
            queued = TurnQueuedPayload(turn_id=str(uuid4()), message=prompt)
            result: Future[str] = Future()
            self._results[queued.turn_id] = result
            self._incoming.put((queued, result))
        return queued.turn_id

    def _run(self) -> None:
        active: Future[str] | None = None
        failure: Exception | None = None
        try:
            while not self._stop.is_set():
                request = self._incoming.get()
                if request is None:
                    break
                queued, active = request
                if not active.set_running_or_notify_cancel():
                    active = None
                    continue
                if self._stop.is_set():
                    break
                self.executor.submit(self.conversation_id, queued)
                while not self._stop.is_set() and self.executor.step():
                    if active is None:
                        continue
                    # The next root prompt stays in the inbox until we drain.
                    events = self.executor.events(self.conversation_id)
                    payload = events[-1].payload
                    if isinstance(payload, TurnCompletedPayload):
                        active.set_result(payload.message)
                        active = None
                    elif isinstance(payload, TurnFailedPayload):
                        active.set_exception(RuntimeError(payload.error))
                        active = None
                if active is not None and not self._stop.is_set():
                    raise RuntimeError(f"No outcome found for {queued.turn_id}")
        except Exception as error:
            logger.exception("Local executor failed")
            failure = error
        finally:
            with self._lifecycle:
                self._failure = failure
                self._stop.set()
            self.executor.outgoing.put(None)
            if active is not None:
                active.set_exception(self._shutdown_error())
            while True:
                try:
                    request = self._incoming.get_nowait()
                except Empty:
                    break
                if request is not None:
                    _, result = request
                    if result.set_running_or_notify_cancel():
                        result.set_exception(self._shutdown_error())

    def _shutdown_error(self) -> RuntimeError:
        error = RuntimeError(
            "Local executor failed"
            if self._failure is not None
            else "Local executor is closed"
        )
        error.__cause__ = self._failure
        return error

    def receive(self, turn_id: str) -> str:
        """Wait for a submitted turn, without consuming reader-group events."""
        self.refresh()
        result = self._results.get(turn_id)
        if result is not None:
            answer = result.result()
            self.refresh()
            return answer
        turn = self.conversation.turn_state.get(turn_id)
        if turn is None:
            raise ValueError(f"No prompt found for turn: {turn_id}")
        if isinstance(turn.state, TurnFailedPayload):
            raise RuntimeError(turn.state.error)
        if not isinstance(turn.state, TurnCompletedPayload):
            raise RuntimeError(f"No outcome found for turn: {turn_id}")
        return turn.state.message

    def ask(self, prompt: str) -> str:
        """Submit a new turn and wait for its result without printing."""
        return self.receive(self.send(prompt))

    def read(self, *, ffwd: bool = False) -> Iterator[ConversationEvent]:
        """Yield unacknowledged events; normally wait up to one second."""
        self.refresh()
        # Already buffered notifications are covered by the fresh log snapshot.
        for _ in range(self.executor.outgoing.qsize()):
            try:
                self.executor.outgoing.get_nowait()
            except Empty:
                break
        events = self._available()
        deadline = monotonic() + 1
        while not ffwd and not events:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            try:
                self.executor.outgoing.get(timeout=remaining)
            except Empty:
                break
            self.refresh()
            events = self._available()
        self.refresh()
        yield from events

    def _available(self) -> list[ConversationEvent]:
        return [
            event
            for event in self.executor.events(self.conversation_id)
            if event.entry_id not in self._acknowledged
        ]

    def acknowledge(self, event: ConversationEvent) -> None:
        """Advance the selected group only after successful event handling."""
        self.refresh()
        if not any(
            saved.entry_id == event.entry_id
            for saved in self.executor.events(self.conversation_id)
        ):
            raise ValueError(f"Unknown event: {event.entry_id}")
        self._acknowledged.add(event.entry_id)

    def queued_event_id(self, turn_id: str) -> str:
        """Find the recorded activation event, including acknowledged events."""
        for event in self.executor.events(self.conversation_id):
            if (
                isinstance(event.payload, TurnQueuedPayload)
                and event.payload.turn_id == turn_id
            ):
                return event.entry_id
        raise ValueError(f"No prompt found for turn: {turn_id}")

    def rewind(self) -> None:
        """Replay retained events for this group without rerunning actions."""
        self.refresh()
        self._acknowledged.clear()
        self.executor.outgoing.put(None)

    def close(self) -> None:
        """Close this client and stop its executor."""
        with self._lifecycle:
            if not self._stop.is_set():
                self._stop.set()
                self._incoming.put(None)
                self.executor.outgoing.put(None)
        if self._thread is not None and self._thread is not current_thread():
            self._thread.join()

    def __enter__(self) -> LocalClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
