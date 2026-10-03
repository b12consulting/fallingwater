"""Run complete Pydantic AI turns from the Redis dispatch stream."""

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
        self.consumer = str(uuid4())
        self.stop_event = Event()
        try:
            redis.xgroup_create(self.names.dispatch, self.group, id="0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s", self.names.dispatch
                )
                raise
            logger.debug(
                "Consumer group %s already exists on %s",
                self.group,
                self.names.dispatch,
            )

    def stop(self) -> None:
        """Stop taking work after the active turn finishes."""
        self.stop_event.set()

    def _load_turn(
        self, events: str, turn_id: str
    ) -> tuple[str, str, str, Sequence[ModelMessage] | None]:
        """Load the queued prompt, fixed agent settings, and prior history."""
        lower = "-"
        history: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        prompt: str | None = None
        while True:
            entries = self.redis.xrange(events, min=lower, count=100)
            for _, fields in entries:
                event_type = fields.get("type")
                if event_type == EventType.CONVERSATION_CREATED:
                    agent_path = fields["agent"]
                    model = fields.get("model", "")
                elif (
                    event_type == EventType.TURN_QUEUED
                    and fields.get("turn_id") == turn_id
                ):
                    prompt = fields["prompt"]
                elif event_type == EventType.TURN_COMPLETED:
                    messages = ModelMessagesTypeAdapter.validate_json(fields["history"])
                    if fields.get("history_format", "snapshot") == "delta":
                        history.extend(messages)
                    else:
                        # Older completion events contain the full history.
                        history = messages
            if len(entries) < 100:
                if agent_path is None or prompt is None:
                    raise ValueError(f"Queued turn {turn_id} not found in {events}")
                return agent_path, model, prompt, history or None
            lower = f"({entries[-1][0]}"

    def _process(self, fields: dict[str, str]) -> None:
        turn_id = fields["turn_id"]
        events = self.names.conversation(fields["conversation_id"])
        agent_path, model, prompt, history = self._load_turn(events, turn_id)
        self.redis.xadd(events, {"type": EventType.TURN_STARTED, "turn_id": turn_id})

        try:
            agent = reify_agent(agent_path)
            result = agent.run_sync(
                prompt,
                message_history=history,
                conversation_id=fields["conversation_id"],
                run_id=turn_id,
                model=model or None,
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
                    "history": result.new_messages_json().decode(),
                    "history_format": "delta",
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
                {self.names.dispatch: ">"},
                count=1,
                block=500,
            )
            for _, entries in batches:
                for entry_id, fields in entries:
                    if fields.get("type") == EventType.CONVERSATION_POKE:
                        logger.info(
                            "Received poke for turn %s in conversation %s",
                            fields["turn_id"],
                            fields["conversation_id"],
                        )
                        self._process(fields)
                    self.redis.xack(self.names.dispatch, self.group, entry_id)
