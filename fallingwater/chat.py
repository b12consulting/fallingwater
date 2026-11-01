"""Terminal and library workflows for Redis-backed conversations."""

from collections.abc import Iterator, Mapping
from typing import cast
from uuid import uuid4

from redis import Redis

from .stream import DispatchStream
from .models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import logger, check_agent


class Chat:
    """Submit turns and observe their results through Redis."""

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
        deps: Mapping[str, str]
        | None = None,  # FIXME use deps class from py-ai
    ) -> None:
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self.cursor = "0"
        self.conversation_id = conversation_id or str(uuid4())
        self.stream = DispatchStream(
            redis, namespace=namespace
        ).conversation_stream(self.conversation_id)
        self.events = self.stream.name

        records = self.stream.read_conversation_range(count=1)
        record = records[0] if records else None
        if record:
            if record.type != Event.CONVERSATION_CREATED:
                raise ValueError(
                    f"{self.events} is not a Fallingwater conversation"
                )
            if deps:
                raise ValueError(
                    "dependencies can only be set for a new conversation"
                )
            created = cast(ConversationCreatedPayload, record.payload)
            self.agent_path = created.agent
            self.model = created.model
        else:
            if agent_path is None:
                raise ValueError("agent is required for a new conversation")
            self.agent_path = agent_path
            self.model = model or ""

        check_agent(self.agent_path, self.model)
        if not record:
            self.stream.append_event(
                Event.CONVERSATION_CREATED,
                ConversationCreatedPayload(
                    agent=self.agent_path,
                    model=self.model,
                    deps=dict(deps or {}),
                ),
            )
        self.stream.create_group(self.group, id="0")

    def send(self, prompt: str) -> tuple[str, str]:
        """Queue a prompt as a new turn and return its ID and event cursor."""
        turn_id = str(uuid4())
        logger.debug("SEND (stream: %s): %.20s", self.events, prompt)
        queued_id = self.stream.queue_turn(turn_id, prompt)
        return turn_id, queued_id

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            entries = self.stream.read_conversation(cursor, block=1000)
            for event in entries:
                cursor = event.entry_id
                if not isinstance(
                    event.payload,
                    (
                        TurnQueuedPayload,
                        TurnCompletedPayload,
                        TurnFailedPayload,
                    ),
                ):
                    continue
                if event.payload.turn_id != turn_id:
                    continue
                if event.type == Event.TURN_COMPLETED:
                    completed = cast(TurnCompletedPayload, event.payload)
                    return completed.message
                if event.type == Event.TURN_FAILED:
                    failed = cast(TurnFailedPayload, event.payload)
                    raise RuntimeError(failed.error)

    def ask(self, prompt: str) -> str:
        """Submit a new turn and wait for its result."""
        return self.receive(*self.send(prompt))

    def _handle_group_event(self, event: ConversationEvent) -> str | None:
        event_type = event.type
        payload = event.payload
        turn_id = getattr(payload, "turn_id", None)
        if event_type == Event.TURN_QUEUED and turn_id is not None:
            self.stream.acknowledge(self.group, event.entry_id)
            return None

        if event_type in (Event.TURN_COMPLETED, Event.TURN_FAILED):
            if event_type == Event.TURN_COMPLETED:
                completed = cast(TurnCompletedPayload, payload)
                print(completed.message)
            else:
                failed = cast(TurnFailedPayload, payload)
                print(f"Turn failed: {failed.error}")
            self.stream.acknowledge(self.group, event.entry_id)
            return turn_id

        self.stream.acknowledge(self.group, event.entry_id)
        return None

    def read(self, *, ffwd: bool = False) -> Iterator[str]:
        """Yield completed turn IDs, draining available events when ffwd is true."""
        logger.debug("READ %s", self.events)
        completed_pending = False
        block = 1000
        while True:
            # Consume Stream
            pending = self.cursor != ">"
            entries = self.stream.read_group(
                self.group,
                self.consumer,
                self.cursor,
                count=100,
                block=None if pending or ffwd else block,
            )
            # handle empty result
            if not entries:
                if not pending:
                    # Pre-existing entries are drained
                    return
                if not ffwd and completed_pending:
                    return
                self.cursor = ">"
                continue

            # process events
            for event in entries:
                turn_id = self._handle_group_event(event)
                if pending:
                    self.cursor = event.entry_id
                if turn_id is not None:
                    completed_pending = True
                    yield turn_id
            if not ffwd and not pending:
                return

    def resume_group(self) -> None:
        """Read available events before accepting another turn."""
        for _ in self.read(ffwd=True):
            pass

    def send_and_wait(self, prompt: str) -> None:
        turn_id, _ = self.send(prompt)
        while True:
            completed = False
            for completed_turn_id in self.read():
                if completed_turn_id == turn_id:
                    completed = True
            if completed:
                return

    def run_once(self, prompt: str) -> None:
        """Print the result of one prompt and return without opening a terminal."""
        self.resume_group()
        self.send_and_wait(prompt)

    def run(self) -> None:
        """Run a small terminal chat; Ctrl-C leaves the conversation intact."""
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        try:
            self.resume_group()
            while True:
                prompt = input("> ")
                self.send_and_wait(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\nConversation left open.")
