"""Run complete Pydantic AI turns from the Redis dispatch stream."""

from collections.abc import Sequence
from threading import Event
from uuid import uuid4

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from .catalog import reify_agent
from .streams import EventType, StreamNames, ConversationStream
from .utils import logger


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

    def load_turn(
        self, conversation_stream: ConversationStream, turn_id: str
    ) -> tuple[str, str, str, Sequence[ModelMessage]]:
        """Load the queued prompt, fixed agent settings, and prior history."""

        history: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        prompt: str | None = None
        for _, fields in conversation_stream.all():
            event_type = fields.get("type")
            # Model and agent
            if event_type == EventType.CONVERSATION_CREATED:
                agent_path = fields["agent"]
                model = fields.get("model", "")
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
            raise ValueError(f"Conversation settings not found in {events}")

        return agent_path, model, prompt, history or None

    def process(self, fields: dict[str, str]) -> None:
        turn_id = fields["turn_id"]
        conversation_id = fields["conversation_id"]
        conversation_stream = ConversationStream(
            self.redis,
            conversation_id,
            self.namespace
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
            conversation_stream.add({
                "type": EventType.TURN_FAILED,
                "turn_id": turn_id,
                "error": str(error),
            })
            return

        # Reify agent and run it
        agent_path, model, prompt, history = turn
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
                "Turn %s failed in conversation %s",
                turn_id,
                conversation_id,
            )
            conversation_stream.add({
                "type": EventType.TURN_FAILED,
                "turn_id": turn_id,
                "error": str(error),
            })
        else:
            conversation_stream.add({
                "type": EventType.TURN_COMPLETED,
                "turn_id": turn_id,
                "message": str(result.output),
                "history": result.new_messages_json().decode(),
            })
            logger.info(
                "Sent response for turn %s in conversation %s",
                turn_id,
                conversation_id,
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
                    self.redis.xack(self.names.dispatch, self.group, entry_id)
