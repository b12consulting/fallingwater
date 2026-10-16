"""Run complete Pydantic AI turns from the Redis dispatch stream."""

from collections.abc import Sequence
from threading import Event
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from .catalog import reify_agent
from .streams import (
    ConversationStream,
    ConversationCreatedPayload,
    ConversationEvent,
    DispatchStream,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    validate_conversation_id,
)
from .utils import logger


class Conversation(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    agent_path: str
    model: str
    next_prompt: str
    current_turn_id: str
    history: Sequence[ModelMessage]
    deps: dict[str, Any]
    usage: RunUsage | None


class Proxy:
    """
    Claim queued turns, run their agent, and publish conversation
    events.
    """

    group = "workers"

    def __init__(self, redis: Redis, *, namespace: str = "fw") -> None:
        self.redis = redis
        self.namespace = namespace
        self.consumer = str(uuid4())
        self.stop_event = Event()
        self.dispatch_stream = DispatchStream(redis, namespace)

    def stop(self) -> None:
        """Stop taking work after the active turn finishes."""
        self.stop_event.set()

    def load_conversation(
        self, conversation_stream: ConversationStream, current_turn_id: str
    ) -> Conversation:
        """Load conversation settings, history, usage, and the queued prompt."""

        history: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        deps: dict[str, Any] = {}
        next_prompt: str | None = None
        usage: RunUsage | None = None
        for _, event in conversation_stream.all():
            event = cast(ConversationEvent, event)
            if event.type == EventType.CONVERSATION_CREATED:
                created = cast(ConversationCreatedPayload, event.payload)
                agent_path = created.agent
                model = created.model
                deps = created.deps
            elif event.type == EventType.TURN_QUEUED:
                queued = cast(TurnQueuedPayload, event.payload)
                if queued.turn_id == current_turn_id:
                    next_prompt = queued.message
                    break
            elif event.type == EventType.TURN_COMPLETED:
                completed = cast(TurnCompletedPayload, event.payload)
                messages = ModelMessagesTypeAdapter.validate_json(
                    completed.history
                )
                history.extend(messages)
                usage = completed.usage

        if next_prompt is None:
            raise ValueError(
                f"No prompt found for turn: {current_turn_id}"
            )
        if agent_path is None:
            raise ValueError(
                f"Conversation settings not found in {conversation_stream.name}"
            )

        return Conversation(
            agent_path=agent_path,
            model=model,
            next_prompt=next_prompt,
            current_turn_id=current_turn_id,
            history=history,
            deps=deps,
            usage=usage,
        )

    def process(self, fields: dict[str, str]) -> None:
        current_turn_id = fields["turn_id"]
        conversation_id = fields["conversation_id"]
        conversation_stream = ConversationStream(
            self.redis, conversation_id, self.namespace
        )

        # Load turn info
        try:
            conversation = self.load_conversation(
                conversation_stream, current_turn_id
            )
        except RedisError:
            # A failed read may be transient, so leave the dispatch
            # entry pending.
            raise
        except ValueError as error:
            logger.exception(
                "Could not load turn %s in conversation %s",
                current_turn_id,
                fields["conversation_id"],
            )
            conversation_stream.add(
                EventType.TURN_FAILED,
                TurnFailedPayload(
                    turn_id=current_turn_id,
                    error=str(error),
                ),
            )
            return

        # Reify agent and run it
        try:
            agent = reify_agent(conversation.agent_path)
            deps = (
                TypeAdapter(agent.deps_type).validate_python(conversation.deps)
                if conversation.deps
                else None
            )
            result = agent.run_sync(
                conversation.next_prompt,
                message_history=conversation.history,
                deps=deps,
                conversation_id=fields["conversation_id"],
                model=conversation.model or None,
                usage=conversation.usage,
            )
        except Exception as error:
            logger.exception(
                "Turn %s failed in conversation %s",
                current_turn_id,
                conversation_id,
            )
            conversation_stream.add(
                EventType.TURN_FAILED,
                TurnFailedPayload(
                    turn_id=current_turn_id,
                    error=str(error),
                ),
            )
        else:
            conversation_stream.add(
                EventType.TURN_COMPLETED,
                TurnCompletedPayload(
                    turn_id=current_turn_id,
                    message=str(result.output),
                    history=result.new_messages_json().decode(),
                    usage=result.usage,
                ),
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                current_turn_id,
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
                    current_turn_id = fields.get("turn_id")
                    try:
                        if not conversation_id or not current_turn_id:
                            raise ValueError(
                                "poke requires conversation_id and turn_id"
                            )
                        validate_conversation_id(conversation_id)
                    except ValueError as error:
                        logger.warning(
                            "Skipping malformed dispatch entry %s: %s",
                            entry_id,
                            error,
                        )
                    else:
                        logger.info(
                            "Received poke for turn %s in conversation %s",
                            current_turn_id,
                            conversation_id,
                        )
                        self.process(fields)
                group.acknowledge(entry_id)
