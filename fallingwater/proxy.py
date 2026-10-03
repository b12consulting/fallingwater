"""Run complete Pydantic AI turns from a Redis work stream."""

from collections.abc import Sequence
from threading import Event
from uuid import uuid4

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from redis import Redis
from redis.exceptions import ResponseError

from .catalog import reify_agent
from .streams import EventType, StreamNames
from .utils import logger


class Proxy:
    """Claim queued turns, run their agent, and publish conversation events."""

    group = "workers"

    def __init__(self, redis: Redis, *, namespace: str = "fw") -> None:
        self.redis = redis
        self.names = StreamNames(namespace)
        self.consumer = uuid4().hex
        self.stop_event = Event()
        try:
            redis.xgroup_create(self.names.work, self.group, id="0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s", self.names.work
                )
                raise
            logger.debug(
                "Consumer group %s already exists on %s", self.group, self.names.work
            )

    def stop(self) -> None:
        """Stop taking work after the active turn finishes."""
        self.stop_event.set()

    def _latest_history(self, events: str) -> Sequence[ModelMessage] | None:
        """Find the last completed turn's Pydantic AI message history."""
        upper = "+"
        while True:
            entries = self.redis.xrevrange(events, max=upper, count=100)
            for _, fields in entries:
                if fields.get("type") == EventType.TURN_COMPLETED:
                    return ModelMessagesTypeAdapter.validate_json(fields["history"])
            if len(entries) < 100:
                return None
            upper = f"({entries[-1][0]}"

    def _process(self, fields: dict[str, str]) -> None:
        turn_id = fields["turn_id"]
        events = self.names.conversation(fields["conversation_id"])
        history = self._latest_history(events)
        self.redis.xadd(events, {"type": EventType.TURN_STARTED, "turn_id": turn_id})

        try:
            agent = reify_agent(fields["agent"])
            result = agent.run_sync(
                fields["prompt"],
                message_history=history,
                conversation_id=fields["conversation_id"],
                run_id=turn_id,
                model=fields.get("model") or None,
            )
        except Exception as error:
            logger.exception(
                "Turn %s failed in conversation %s", turn_id, fields["conversation_id"]
            )
            self.redis.xadd(
                events,
                {
                    "type": EventType.TURN_FAILED,
                    "turn_id": turn_id,
                    "error": str(error),
                },
            )
        else:
            self.redis.xadd(
                events,
                {
                    "type": EventType.TURN_COMPLETED,
                    "turn_id": turn_id,
                    "output": str(result.output),
                    "history": result.all_messages_json().decode(),
                },
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                turn_id,
                fields["conversation_id"],
            )

    def run(self) -> None:
        """Read new work until stopped, finishing any turn already claimed."""
        while not self.stop_event.is_set():
            batches = self.redis.xreadgroup(
                self.group,
                self.consumer,
                {self.names.work: ">"},
                count=1,
                block=500,
            )
            for _, entries in batches:
                for entry_id, fields in entries:
                    if fields.get("type") == EventType.TURN_QUEUED:
                        logger.info(
                            "Received message for turn %s in conversation %s",
                            fields["turn_id"],
                            fields["conversation_id"],
                        )
                        self._process(fields)
                    self.redis.xack(self.names.work, self.group, entry_id)
