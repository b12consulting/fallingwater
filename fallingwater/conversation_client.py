"""Client workflows for Redis-backed conversations, without terminal I/O."""

from collections.abc import Iterator, Mapping
from uuid import uuid4

from redis import Redis

from .conversation import Conversation
from .models import (
    ConversationCreatedPayload,
    ConversationEvent,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .stream import DispatchStream
from .utils import check_agent, logger


class ConversationClient:
    """Open conversations, submit turns, and read their persisted events."""

    consumer = "terminal"

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        conversation_id: str | None = None,
        agent_path: str | None = None,
        model: str | None = None,
        group: str = "user",
        deps: Mapping[str, str] | None = None,
    ) -> None:
        if not group:
            raise ValueError("conversation reader group must be nonempty")
        self.group = group
        self.cursor = "0"
        self._queued_event_ids: dict[str, str] = {}
        self.conversation_id = conversation_id or str(uuid4())
        self.stream = DispatchStream(
            redis, namespace=namespace
        ).conversation_stream(self.conversation_id)

        records = self.stream.read_conversation_range(count=1)
        record = records[0] if records else None
        if record:
            if not isinstance(record.payload, ConversationCreatedPayload):
                raise ValueError(
                    f"{self.stream.name} is not a Fallingwater conversation"
                )
            if deps:
                raise ValueError(
                    "dependencies can only be set for a new conversation"
                )
            settings = record.payload
        else:
            if agent_path is None:
                raise ValueError("agent is required for a new conversation")
            settings = ConversationCreatedPayload(
                agent=agent_path, model=model or "", deps=dict(deps or {})
            )

        check_agent(settings.agent, settings.model)
        if not record:
            self.stream.append_payload(settings)
        self.settings = settings
        self.stream.create_group(self.group, id="0")

    def send(self, prompt: str) -> tuple[str, str]:
        """Queue a prompt as a new turn and return its ID and event cursor."""
        turn_id = str(uuid4())
        logger.debug("SEND (stream: %s): %.20s", self.stream.name, prompt)
        # Fresh-turn preparation needs settings, not a replayed conversation.
        conversation = Conversation(
            conversation_id=self.conversation_id, settings=self.settings
        )
        prepared = conversation.prepare(
            TurnQueuedPayload(turn_id=turn_id, message=prompt)
        )
        queued_id = self.stream.append_payload(prepared.payload)
        for action in prepared.dispatch(queued_id):
            self.stream.dispatch.append_payload(action)
        return turn_id, queued_id

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result independently of the reader group."""
        while True:
            for event in self.stream.read_conversation(cursor, block=1000):
                cursor = event.entry_id
                payload = event.payload
                if isinstance(payload, TurnCompletedPayload):
                    if payload.turn_id == turn_id:
                        return payload.message
                elif isinstance(payload, TurnFailedPayload):
                    if payload.turn_id == turn_id:
                        raise RuntimeError(payload.error)

    def ask(self, prompt: str) -> str:
        """Submit a new turn and wait for its result without printing."""
        return self.receive(*self.send(prompt))

    def read(self, *, ffwd: bool = False) -> Iterator[ConversationEvent]:
        """Yield group events; callers acknowledge them after handling them.

        Drain available events without blocking when ffwd is true. Otherwise,
        drain pending events, then wait up to one second for a new batch.
        """
        logger.debug("READ %s", self.stream.name)
        completed_pending = False
        while True:
            pending = self.cursor != ">"
            entries = self.stream.read_group(
                self.group,
                self.consumer,
                self.cursor,
                count=100,
                block=None if pending or ffwd else 1000,
            )
            if not entries:
                if not pending or (not ffwd and completed_pending):
                    return
                self.cursor = ">"
                continue

            for event in entries:
                if isinstance(event.payload, TurnQueuedPayload):
                    self._queued_event_ids[
                        event.payload.turn_id
                    ] = event.entry_id
                yield event
                if pending:
                    self.cursor = event.entry_id
                if isinstance(
                    event.payload, (TurnCompletedPayload, TurnFailedPayload)
                ):
                    completed_pending = True
            if not ffwd and not pending:
                return

    def acknowledge(self, event: ConversationEvent) -> None:
        """Acknowledge an event only after the caller has handled it."""
        self.stream.acknowledge(self.group, event.entry_id)

    def queued_event_id(self, turn_id: str) -> str:
        """Find a prompt's event ID, including already acknowledged prompts."""
        if turn_id not in self._queued_event_ids:
            for event in self.stream.iter_events():
                if isinstance(event.payload, TurnQueuedPayload):
                    self._queued_event_ids[
                        event.payload.turn_id
                    ] = event.entry_id
        if turn_id not in self._queued_event_ids:
            raise ValueError(f"No prompt found for turn: {turn_id}")
        return self._queued_event_ids[turn_id]

    def rewind(self) -> None:
        """Replay retained events from the beginning for this reader group."""
        self.stream.rewind_group(self.group)
        self.cursor = ">"
