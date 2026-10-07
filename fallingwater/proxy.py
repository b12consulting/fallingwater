"""Run complete Pydantic AI turns from the Redis dispatch stream."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from threading import Event
from typing import Any
from uuid import uuid4

from pydantic import TypeAdapter
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from redis import Redis
from redis.exceptions import RedisError

from .catalog import reify_agent
from .streams import (
    ConversationStream,
    DispatchStream,
    EventType,
    StreamNames,
)
from .utils import logger


@dataclass(frozen=True, slots=True)
class Turn:
    agent_path: str
    model: str
    prompt: str
    history: Sequence[ModelMessage]
    deps: dict[str, Any]


class Proxy:
    """
    Claim queued turns, run their agent, and publish conversation
    events.
    """

    group = "workers"

    def __init__(self, redis: Redis, *, namespace: str = "fw") -> None:
        self.redis = redis
        self.namespace = namespace
        self.names = StreamNames(namespace)
        self.consumer = str(uuid4())
        self.stop_event = Event()
        self.dispatch_stream = DispatchStream(redis, namespace)

    def stop(self) -> None:
        """Stop taking work after the active turn finishes."""
        self.stop_event.set()

    def load_turn(
        self, conversation_stream: ConversationStream, turn_id: str
    ) -> Turn:
        """Load the queued prompt, fixed agent settings, and prior history."""

        history: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        deps: dict[str, Any] = {}
        prompt: str | None = None
        for _, fields in conversation_stream.all():
            event_type = fields.get("type")
            # Model and agent
            if event_type == EventType.CONVERSATION_CREATED:
                agent_path = fields["agent"]
                model = fields.get("model", "")
                raw_deps = fields.get("deps", "{}")
                parsed_deps = json.loads(raw_deps)
                if not isinstance(parsed_deps, dict):
                    raise ValueError(
                        "Conversation dependencies must be a JSON object"
                    )
                deps = parsed_deps
            # Get prompt
            elif event_type == EventType.TURN_QUEUED:
                if fields.get("turn_id") == turn_id:
                    prompt = fields["message"]
                    break
            # Accumulate history
            elif event_type == EventType.TURN_COMPLETED:
                messages = ModelMessagesTypeAdapter.validate_json(
                    fields["history"]
                )
                history.extend(messages)

        if prompt is None:
            raise ValueError(f"No prompt found for turn: {turn_id}")
        if agent_path is None:
            raise ValueError(
                f"Conversation settings not found in {conversation_stream.name}"
            )

        return Turn(agent_path, model, prompt, history, deps)

    def process(self, fields: dict[str, str]) -> None:
        turn_id = fields["turn_id"]
        conversation_id = fields["conversation_id"]
        conversation_stream = ConversationStream(
            self.redis, conversation_id, self.namespace
        )

        # Load turn info
        try:
            turn = self.load_turn(conversation_stream, turn_id)
        except RedisError:
            # A failed read may be transient, so leave the dispatch
            # entry pending.
            raise
        except ValueError as error:
            logger.exception(
                "Could not load turn %s in conversation %s",
                turn_id,
                fields["conversation_id"],
            )
            conversation_stream.add(
                {
                    "type": EventType.TURN_FAILED,
                    "turn_id": turn_id,
                    "error": str(error),
                }
            )
            return

        # Reify agent and run it
        try:
            agent = reify_agent(turn.agent_path)
            deps = (
                TypeAdapter(agent.deps_type).validate_python(turn.deps)
                if turn.deps
                else None
            )
            result = agent.run_sync(
                turn.prompt,
                message_history=turn.history,
                deps=deps,
                conversation_id=fields["conversation_id"],
                run_id=turn_id,
                model=turn.model or None,
            )
        except Exception as error:
            logger.exception(
                "Turn %s failed in conversation %s",
                turn_id,
                conversation_id,
            )
            conversation_stream.add(
                {
                    "type": EventType.TURN_FAILED,
                    "turn_id": turn_id,
                    "error": str(error),
                }
            )
        else:
            conversation_stream.add(
                {
                    "type": EventType.TURN_COMPLETED,
                    "turn_id": turn_id,
                    "message": str(result.output),
                    "history": result.new_messages_json().decode(),
                }
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                turn_id,
                conversation_id,
            )

    def run(self) -> None:
        """Read new work until stopped, finishing any turn already claimed."""
        group = self.dispatch_stream.group(self.group, mkstream=True)
        while not self.stop_event.is_set():
            entries = group.read_new(
                self.consumer,
                count=1,
                block=500,
            )
            for entry_id, fields in entries:
                if fields.get("type") == EventType.CONVERSATION_POKE:
                    conversation_id = fields.get("conversation_id")
                    turn_id = fields.get("turn_id")
                    try:
                        if not conversation_id or not turn_id:
                            raise ValueError(
                                "poke requires conversation_id and turn_id"
                            )
                        self.names.conversation(conversation_id)
                    except ValueError as error:
                        logger.warning(
                            "Skipping malformed dispatch entry %s: %s",
                            entry_id,
                            error,
                        )
                    else:
                        logger.info(
                            "Received poke for turn %s in conversation %s",
                            turn_id,
                            conversation_id,
                        )
                        self.process(fields)
                group.acknowledge(entry_id)
