"""Synchronous Redis-backed conversation client."""

import sys
from collections.abc import Iterator, Mapping
from typing import cast
from uuid import uuid4

from redis import Redis
from redis.exceptions import ResponseError

from .catalog import reify_agent
from .streams import (
    ConversationCreatedPayload,
    ConversationEvent,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    decode_conversation_event,
    decode_text,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from .utils import logger


class Chat:
    """Submit one turn at a time and observe its result through Redis."""

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
        self.redis = redis
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self._pending_turns: dict[str, str] = {}
        self.conversation_id = conversation_id or str(uuid4())
        self.events = fmt_conversation_stream(namespace, self.conversation_id)
        self.dispatch_stream = fmt_dispatch_stream(namespace)

        records = redis.xrange(self.events, count=1)
        record = decode_conversation_event(records[0][1]) if records else None
        if record:
            if record.type != EventType.CONVERSATION_CREATED:
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

        self._check_agent()
        if not record:
            self.redis.xadd(
                self.events,
                encode_conversation_event(
                    EventType.CONVERSATION_CREATED,
                    ConversationCreatedPayload(
                        agent=self.agent_path,
                        model=self.model,
                        deps=dict(deps or {}),
                    ),
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

    def send(self, prompt: str) -> tuple[str, str]:
        """Record a queued turn and return its turn ID and event cursor."""
        turn_id = str(uuid4())
        logger.debug("SEND (stream: %s): %.20s", self.events, prompt)
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.events,
                encode_conversation_event(
                    EventType.TURN_QUEUED,
                    TurnQueuedPayload(turn_id=turn_id, message=prompt),
                ),
            )
            pipeline.xadd(
                self.dispatch_stream,
                {
                    "type": EventType.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": self.conversation_id,
                },
            )
            queued_id, _dispatch_id = pipeline.execute()
        return turn_id, decode_text(queued_id)

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            batches = self.redis.xread({self.events: cursor}, block=1000)
            for _stream_name, entries in batches:
                for raw_entry_id, fields in entries:
                    cursor = decode_text(raw_entry_id)
                    event = decode_conversation_event(fields)
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

    def ask(self, prompt: str) -> str:
        return self.receive(*self.send(prompt))

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
            self._pending_turns[turn_id] = entry_id
            return None

        if event_type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED):
            output = sys.stderr if to_stderr else sys.stdout
            if event_type == EventType.TURN_COMPLETED:
                completed = cast(TurnCompletedPayload, payload)
                print(completed.message, file=output, flush=True)
            else:
                failed = cast(TurnFailedPayload, payload)
                print(f"Turn failed: {failed.error}", file=output, flush=True)
            queued_id = self._pending_turns.pop(turn_id, None)
            if queued_id is None:
                self.redis.xack(self.events, self.group, entry_id)
            else:
                self.redis.xack(self.events, self.group, queued_id, entry_id)
            return turn_id

        self.redis.xack(self.events, self.group, entry_id)
        return None

    def drain_pending(self, *, to_stderr: bool = False) -> None:
        """Replay entries assigned to this stable consumer before reading new ones."""
        cursor = "0"
        logger.debug("DRAIN %s", self.events)
        while True:
            batches = self.redis.xreadgroup(
                self.group, self.consumer, {self.events: cursor}, count=100
            )
            entries = [entry for _, batch in batches for entry in batch]
            if not entries:
                return
            for raw_entry_id, fields in entries:
                entry_id = decode_text(raw_entry_id)
                cursor = entry_id
                self._handle_group_event(
                    entry_id,
                    decode_conversation_event(fields),
                    to_stderr=to_stderr,
                )

    def read_new(
        self,
        *,
        block: int | None = None,
        to_stderr: bool = False,
    ) -> Iterator[str]:
        """Read new events and yield IDs of turns that complete."""
        logger.debug("READ NEW %s", self.events)
        while True:
            batches = self.redis.xreadgroup(
                self.group,
                self.consumer,
                {self.events: ">"},
                count=100,
                block=block,
            )
            entries = [entry for _, batch in batches for entry in batch]
            if not entries:
                return
            for raw_entry_id, fields in entries:
                turn_id = self._handle_group_event(
                    decode_text(raw_entry_id),
                    decode_conversation_event(fields),
                    to_stderr=to_stderr,
                )
                if turn_id is not None:
                    yield turn_id
            if block is not None:
                return

    def _resume_group(self, *, to_stderr: bool = False) -> None:
        """Catch up with unread events before accepting another turn."""
        try:
            self.redis.xgroup_create(self.events, self.group, id="0")
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise
            logger.debug(
                "Consumer group %s already exists on %s", self.group, self.events
            )
        self.drain_pending(to_stderr=to_stderr)
        for _ in self.read_new(to_stderr=to_stderr):
            pass
        while self._pending_turns:
            for _ in self.read_new(block=1000, to_stderr=to_stderr):
                pass

    def _send_and_wait(self, prompt: str) -> None:
        turn_id, _ = self.send(prompt)
        while True:
            completed = False
            for completed_turn_id in self.read_new(block=1000):
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
