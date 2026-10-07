"""Synchronous Redis-backed conversation client."""

import json
import sys
from collections.abc import Mapping
from uuid import uuid4

from redis import Redis

from .catalog import reify_agent
from .streams import (
    ConversationStream,
    DispatchStream,
    EventType,
    ReadGroup,
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
        deps: Mapping[str, str] | None = None,
    ) -> None:
        self.redis = redis
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self._pending_turns: dict[str, str] = {}
        self.conversation_id = conversation_id or str(uuid4())
        self.conversation_stream = ConversationStream(
            redis, self.conversation_id, namespace
        )
        self.dispatch_stream = DispatchStream(redis, namespace)
        self.events = self.conversation_stream.name

        record = self.conversation_stream.one()
        if record:
            if record.get("type") != EventType.CONVERSATION_CREATED:
                raise ValueError(
                    f"{self.events} is not a Fallingwater conversation"
                )
            if deps:
                raise ValueError(
                    "dependencies can only be set for a new conversation"
                )
            existing_agent = record["agent"]
            existing_model = record.get("model", "")
            self.agent_path = existing_agent
            self.model = existing_model
        else:
            if agent_path is None:
                raise ValueError("agent is required for a new conversation")
            self.agent_path = agent_path
            self.model = model or ""

        self._check_agent()
        if not record:
            # FIXME we have a `if record .. else` just above
            self.conversation_stream.add(
                {
                    "type": EventType.CONVERSATION_CREATED,
                    "agent": self.agent_path,
                    "model": self.model,
                    "deps": json.dumps(dict(deps or {})),
                }
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
        # Pydantic AI also allows capabilities to select a model during a run.
        if agent._root_capability.get_model() is not None:
            return
        raise ValueError(
            f"agent {self.agent_path!r} has no model; "
            "pass --model or configure one on the agent"
        )

    def send(self, prompt: str) -> tuple[str, str]:
        """Record a queued turn and return its turn ID and event cursor."""
        turn_id = str(uuid4())
        with self.redis.pipeline(transaction=True) as pipeline:
            self.conversation_stream.add(
                {
                    "type": EventType.TURN_QUEUED,
                    "turn_id": turn_id,
                    "message": prompt,
                },
                pipeline=pipeline,
            )
            self.dispatch_stream.add(
                {
                    "type": EventType.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": self.conversation_id,
                },
                pipeline=pipeline,
            )
            queued_id, _dispatch_id = pipeline.execute()
        return turn_id, queued_id

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            entries = self.conversation_stream.read_after(cursor, block=1000)
            for event_id, fields in entries:
                cursor = event_id
                if fields.get("turn_id") != turn_id:
                    continue
                if fields.get("type") == EventType.TURN_COMPLETED:
                    return fields["message"]
                if fields.get("type") == EventType.TURN_FAILED:
                    raise RuntimeError(fields["error"])

    def ask(self, prompt: str) -> str:
        return self.receive(*self.send(prompt))

    def _handle_group_event(
        self,
        group: ReadGroup,
        entry_id: str,
        fields: dict[str, str],
        *,
        to_stderr: bool = False,
    ) -> str | None:
        event_type = fields.get("type")
        turn_id = fields.get("turn_id")
        if event_type == EventType.TURN_QUEUED and turn_id is not None:
            self._pending_turns[turn_id] = entry_id
            return None

        if event_type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED):
            output = sys.stderr if to_stderr else sys.stdout
            if event_type == EventType.TURN_COMPLETED:
                print(fields["message"], file=output, flush=True)
            else:
                print(
                    f"Turn failed: {fields['error']}", file=output, flush=True
                )
            queued_id = self._pending_turns.pop(turn_id, None)
            if queued_id is None:
                group.acknowledge(entry_id)
            else:
                group.acknowledge(queued_id, entry_id)
            return turn_id

        group.acknowledge(entry_id)
        return None

    def _drain_pending(
        self, group: ReadGroup, *, to_stderr: bool = False
    ) -> None:
        """Replay entries assigned to this stable consumer before reading new ones."""
        cursor = "0"
        while True:
            entries = group.read_pending(
                self.consumer,
                cursor,     # maybe we should let the group deal with the cursor itself
                count=100,  # TODO this should be a default param
            )
            if not entries:
                return
            for entry_id, fields in entries:
                cursor = entry_id
                self._handle_group_event(
                    group, entry_id, fields, to_stderr=to_stderr
                )

    def _read_new(
        self,
        group: ReadGroup,
        *,
        block: int | None = None,
        to_stderr: bool = False,
    ) -> set[str]:
        completed: set[str] = set()
        while True:
            entries = group.read_new(
                self.consumer,
                count=100,
                block=block,
            )
            if not entries:
                return completed
            for entry_id, fields in entries:
                turn_id = self._handle_group_event(
                    group, entry_id, fields, to_stderr=to_stderr
                )
                if turn_id is not None:
                    completed.add(turn_id)
            if block is not None:
                return completed

    def _resume_group(self, *, to_stderr: bool = False) -> ReadGroup:
        """Catch up with unread events before accepting another turn."""
        group = self.conversation_stream.group(self.group)
        self._drain_pending(group, to_stderr=to_stderr)
        self._read_new(group, to_stderr=to_stderr)
        while self._pending_turns:
            self._read_new(group, block=1000, to_stderr=to_stderr)
        return group

    def _send_and_wait(self, prompt: str, group: ReadGroup) -> None:
        turn_id, _ = self.send(prompt)
        while turn_id not in self._read_new(group, block=1000):
            pass

    def run_once(self, prompt: str) -> None:
        """Print the result of one prompt and return without opening a terminal."""
        group = self._resume_group(to_stderr=True)
        self._send_and_wait(prompt, group)

    def run(self) -> None:
        """Run a small terminal chat; Ctrl-C leaves the conversation intact."""
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        try:
            group = self._resume_group()
            while True:
                prompt = input("> ")
                if prompt:
                    self._send_and_wait(prompt, group)
        except (EOFError, KeyboardInterrupt):
            logger.debug(
                "Chat %s closed from the terminal", self.conversation_id
            )
            print()
            return
