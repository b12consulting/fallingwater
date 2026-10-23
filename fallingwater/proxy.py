"""
Run complete Pydantic AI turns from the Redis dispatch stream.
"""

from collections.abc import Sequence
from threading import Event
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from .catalog import reify_agent
from .streams import (
    ConversationCreatedPayload,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    decode_conversation_entry,
    decode_dispatch_fields,
    decode_text,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
    validate_conversation_id,
)
from .runner import run_until_complete
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

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        claim_idle_ms: int = 300_000,
    ) -> None:
        if claim_idle_ms < 0:
            raise ValueError("claim_idle_ms must be nonnegative")
        self.redis = redis
        self.namespace = namespace
        self.claim_idle_ms = claim_idle_ms
        self.consumer = str(uuid4())
        self.stop_event = Event()
        self.dispatch_stream = fmt_dispatch_stream(namespace)
        self.claim_cursor = "0-0"

    def stop(self) -> None:
        """
        Stop taking work after the active turn finishes.
        """
        self.stop_event.set()

    def load_conversation(
        self, conversation_stream: str, current_turn_id: str
    ) -> Conversation:
        """
        Load conversation settings, history, usage, and the queued prompt.
        """

        history: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        deps: dict[str, Any] = {}
        next_prompt: str | None = None
        usage: RunUsage | None = None
        cursor = "-"
        while True:
            entries = self.redis.xrange(
                conversation_stream, min=cursor, count=100
            )
            if not entries:
                break
            found_prompt = False
            for raw_entry in entries:
                _, event = decode_conversation_entry(raw_entry)
                if event.type == EventType.CONVERSATION_CREATED:
                    created = cast(ConversationCreatedPayload, event.payload)
                    agent_path = created.agent
                    model = created.model
                    deps = created.deps
                elif event.type == EventType.TURN_QUEUED:
                    queued = cast(TurnQueuedPayload, event.payload)
                    if queued.turn_id == current_turn_id:
                        next_prompt = queued.message
                        found_prompt = True
                        break
                elif event.type == EventType.TURN_COMPLETED:
                    completed = cast(TurnCompletedPayload, event.payload)
                    messages = ModelMessagesTypeAdapter.validate_python(
                        completed.history
                    )
                    history.extend(messages)
                    usage = completed.usage

            if found_prompt:
                break
            cursor = f"({decode_text(entries[-1][0])}"

        if next_prompt is None:
            raise ValueError(
                f"No prompt found for turn: {current_turn_id}"
            )
        if agent_path is None:
            raise ValueError(
                f"Conversation settings not found in {conversation_stream}"
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
        conversation_stream = fmt_conversation_stream(
            self.namespace, conversation_id
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
            self.redis.xadd(
                conversation_stream,
                encode_conversation_event(
                    EventType.TURN_FAILED,
                    TurnFailedPayload(
                        turn_id=current_turn_id,
                        error=str(error),
                    ),
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
            result = run_until_complete(
                agent,
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
            self.redis.xadd(
                conversation_stream,
                encode_conversation_event(
                    EventType.TURN_FAILED,
                    TurnFailedPayload(
                        turn_id=current_turn_id,
                        error=str(error),
                    ),
                ),
            )
        else:
            self.redis.xadd(
                conversation_stream,
                encode_conversation_event(
                    EventType.TURN_COMPLETED,
                    TurnCompletedPayload(
                        turn_id=current_turn_id,
                        message=str(result.output),
                        history=ModelMessagesTypeAdapter.dump_python(
                            result.new_messages, mode="json"
                        ),
                        usage=result.usage,
                    ),
                ),
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                current_turn_id,
                conversation_id,
            )

    def run(self) -> None:
        """
        Read new work until stopped, finishing any turn already claimed.
        """
        try:
            self.redis.xgroup_create(
                self.dispatch_stream, self.group, id="0", mkstream=True
            )
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s",
                    self.dispatch_stream,
                )
                raise
            logger.debug("Consumer group %s already exists", self.group)

        while not self.stop_event.is_set():
            claim_cursor, claimed, _deleted = self.redis.xautoclaim(
                self.dispatch_stream,
                self.group,
                self.consumer,
                min_idle_time=self.claim_idle_ms,
                start_id=self.claim_cursor,
                count=1,
            )
            self.claim_cursor = (
                claim_cursor.decode()
                if isinstance(claim_cursor, bytes)
                else claim_cursor
            )
            if claimed:
                entries = claimed
            else:
                batches = self.redis.xreadgroup(
                    self.group,
                    self.consumer,
                    {self.dispatch_stream: ">"},
                    count=1,
                    block=500,
                )
                entries = [entry for _, batch in batches for entry in batch]
            for raw_entry_id, raw_fields in entries:
                entry_id = decode_text(raw_entry_id)
                fields = decode_dispatch_fields(raw_fields)
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
                self.redis.xack(self.dispatch_stream, self.group, entry_id)
