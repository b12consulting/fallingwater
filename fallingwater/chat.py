"""Terminal and library workflows for Redis-backed conversations."""

import sys
from collections.abc import Iterator, Mapping
from typing import cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from redis import Redis
from redis.exceptions import ResponseError

from .catalog import reify_agent
from .conversation import ConversationClient
from .streams import (
    ConversationCreatedPayload,
    ConversationEvent,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from .utils import logger


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
        deps: Mapping[str, str] | None = None,  # FIXME use deps class from py-ai
    ) -> None:
        self.client = ConversationClient(redis, namespace=namespace)
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self.conversation_id = conversation_id or str(uuid4())
        self.events = self.client.conversation_stream(self.conversation_id)
        self.dispatch_stream = self.client.dispatch_stream

        records = self.client.read_conversation_range(self.events, count=1)
        record = records[0][1] if records else None
        if record:
            if record.type != EventType.CONVERSATION_CREATED:
                raise ValueError(f"{self.events} is not a Fallingwater conversation")
            if deps:
                raise ValueError("dependencies can only be set for a new conversation")
            created = cast(ConversationCreatedPayload, record.payload)
            self.agent_path = created.agent
            self.model = created.model
        else:
            if agent_path is None:
                raise ValueError("agent is required for a new conversation")
            self.agent_path = agent_path
            self.model = model or ""

        self._check_agent()
        if not record:
            self.client.append_event(
                self.events,
                EventType.CONVERSATION_CREATED,
                ConversationCreatedPayload(
                    agent=self.agent_path,
                    model=self.model,
                    deps=dict(deps or {}),
                ),
            )

    def _check_agent(self) -> None:
        """Validate the effective agent and model before using the chat."""
        try:
            agent = reify_agent(self.agent_path)
        except Exception as error:
            logger.debug(
                "Could not instantiate agent %s", self.agent_path, exc_info=True
            )
            raise ValueError(
                f"cannot instantiate agent {self.agent_path!r}: {error}"
            ) from error
        if self.model or agent.model is not None:
            return
        if agent._root_capability.get_model() is not None:
            return
        raise ValueError(
            f"agent {self.agent_path!r} has no model; "
            "pass --model or configure one on the agent"
        )

    def _queued_cursor(self, turn_id: str, prompt: str) -> str | None:
        """Find an existing queued turn for an idempotent delegated request."""
        cursor = "-"
        while True:
            entries = self.client.read_conversation_range(
                self.events, min=cursor, count=100
            )
            if not entries:
                return None
            for entry_id, event in entries:
                if event.type != EventType.TURN_QUEUED:
                    continue
                queued = cast(TurnQueuedPayload, event.payload)
                if queued.turn_id != turn_id:
                    continue
                if queued.message != prompt:
                    raise ValueError(
                        f"turn ID {turn_id!r} is already queued with another prompt"
                    )
                return entry_id
            cursor = f"({entries[-1][0]}"

    def _queue(
        self, prompt: str, turn_id: str, *, reuse_turn_id: bool
    ) -> tuple[str, str]:
        """Queue a prompt using a generated or stable turn ID."""
        if reuse_turn_id:
            queued_cursor = self._queued_cursor(turn_id, prompt)
            if queued_cursor is not None:
                return turn_id, queued_cursor
        logger.debug("SEND (stream: %s): %.20s", self.events, prompt)
        queued_id = self.client.queue_turn(self.conversation_id, turn_id, prompt)
        return turn_id, queued_id

    def send(self, prompt: str, *, turn_id: str | None = None) -> tuple[str, str]:
        """Queue a prompt, optionally reusing an existing turn ID."""
        reuse_turn_id = turn_id is not None
        if reuse_turn_id and not turn_id:
            raise ValueError("turn ID must be nonempty")
        turn_id = turn_id or str(uuid4())
        return self._queue(prompt, turn_id, reuse_turn_id=reuse_turn_id)

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            entries = self.client.read_conversation(self.events, cursor, block=1000)
            for cursor, event in entries:
                if not isinstance(
                    event.payload,
                    (TurnQueuedPayload, TurnCompletedPayload, TurnFailedPayload),
                ):
                    continue
                if event.payload.turn_id != turn_id:
                    continue
                if event.type == EventType.TURN_COMPLETED:
                    completed = cast(TurnCompletedPayload, event.payload)
                    return completed.message
                if event.type == EventType.TURN_FAILED:
                    failed = cast(TurnFailedPayload, event.payload)
                    raise RuntimeError(failed.error)

    def ask(self, prompt: str, *, idempotency_key: str | None = None) -> str:
        """Submit or resume one turn, optionally using a stable idempotency key."""
        if idempotency_key is None:
            turn_id = str(uuid4())
            reuse_turn_id = False
        else:
            if not idempotency_key:
                raise ValueError("idempotency key must be nonempty")
            turn_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{self.conversation_id}:ask:{idempotency_key}",
                )
            )
            reuse_turn_id = True
        return self.receive(
            *self._queue(prompt, turn_id, reuse_turn_id=reuse_turn_id)
        )

    def _handle_group_event(
        self,
        entry_id: str,
        event: ConversationEvent,
        *,
        to_stderr: bool = False,
    ) -> str | None:
        event_type = event.type
        payload = event.payload
        turn_id = getattr(payload, "turn_id", None)
        if event_type == EventType.TURN_QUEUED and turn_id is not None:
            self.client.acknowledge(self.events, self.group, entry_id)
            return None

        if event_type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED):
            output = sys.stderr if to_stderr else sys.stdout
            if event_type == EventType.TURN_COMPLETED:
                completed = cast(TurnCompletedPayload, payload)
                print(completed.message, file=output, flush=True)
            else:
                failed = cast(TurnFailedPayload, payload)
                print(f"Turn failed: {failed.error}", file=output, flush=True)
            self.client.acknowledge(self.events, self.group, entry_id)
            return turn_id

        self.client.acknowledge(self.events, self.group, entry_id)
        return None

    def read(
        self,
        *,
        block: int | None = None,
        to_stderr: bool = False,
    ) -> Iterator[str]:
        """Read pending entries, then new events, yielding completed turn IDs."""
        logger.debug("READ %s", self.events)
        cursor = "0"
        completed_pending = False
        while True:
            entries = self.client.read_group(
                self.events,
                self.group,
                self.consumer,
                cursor,
                count=100,
            )
            if not entries:
                break
            for entry_id, event in entries:
                cursor = entry_id
                turn_id = self._handle_group_event(
                    entry_id, event, to_stderr=to_stderr
                )
                if turn_id is not None:
                    completed_pending = True
                    yield turn_id

        if block is not None and completed_pending:
            return

        while True:
            entries = self.client.read_group(
                self.events,
                self.group,
                self.consumer,
                ">",
                count=100,
                block=block,
            )
            if not entries:
                return
            for entry_id, event in entries:
                turn_id = self._handle_group_event(
                    entry_id, event, to_stderr=to_stderr
                )
                if turn_id is not None:
                    yield turn_id
            if block is not None:
                return

    def _resume_group(self, *, to_stderr: bool = False) -> None:
        """Read available events before accepting another turn."""
        try:
            self.client.create_group(self.events, self.group, id="0")
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise
            logger.debug(
                "Consumer group %s already exists on %s", self.group, self.events
            )
        for _ in self.read(to_stderr=to_stderr):
            pass

    def _send_and_wait(self, prompt: str) -> None:
        turn_id, _ = self.send(prompt)
        while True:
            completed = False
            for completed_turn_id in self.read(block=1000):
                if completed_turn_id == turn_id:
                    completed = True
            if completed:
                return

    def run_once(self, prompt: str) -> None:
        """Print the result of one prompt and return without opening a terminal."""
        self._resume_group(to_stderr=True)
        self._send_and_wait(prompt)

    def run(self) -> None:
        """Run a small terminal chat; Ctrl-C leaves the conversation intact."""
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        try:
            self._resume_group()
            while True:
                prompt = input("> ")
                self._send_and_wait(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\nConversation left open.")
