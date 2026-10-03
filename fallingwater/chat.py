"""Synchronous Redis-backed conversation client."""

from uuid import uuid4

from redis import Redis

from .catalog import reify_agent
from .streams import EventType, StreamNames
from .utils import logger


class Chat:
    """Submit one turn at a time and observe its result through Redis."""

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        conversation_id: str | None = None,
        agent_path: str | None = None,
        model: str | None = None,
    ) -> None:
        self.redis = redis
        self.names = StreamNames(namespace)
        self.conversation_id = conversation_id or uuid4().hex
        self.events = self.names.conversation(self.conversation_id)

        first = redis.xrange(self.events, count=1)
        if first:
            record = first[0][1]
            if record.get("type") != EventType.CONVERSATION_CREATED:
                raise ValueError(f"{self.events} is not a Fallingwater conversation")
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
        if not first:
            redis.xadd(
                self.events,
                {
                    "type": EventType.CONVERSATION_CREATED,
                    "agent": self.agent_path,
                    "model": self.model,
                },
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
        turn_id = uuid4().hex
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.events,
                {"type": EventType.TURN_QUEUED, "turn_id": turn_id, "prompt": prompt},
            )
            pipeline.xadd(
                self.names.dispatch,
                {
                    "type": EventType.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": self.conversation_id,
                },
            )
            queued_id, _dispatch_id = pipeline.execute()
        return turn_id, queued_id

    def receive(self, turn_id: str, cursor: str) -> str:
        """Wait for this turn's result without joining the worker group."""
        while True:
            batches = self.redis.xread({self.events: cursor}, block=1000)
            for _, entries in batches:
                for event_id, fields in entries:
                    cursor = event_id
                    if fields.get("turn_id") != turn_id:
                        continue
                    if fields.get("type") == EventType.TURN_COMPLETED:
                        return fields["output"]
                    if fields.get("type") == EventType.TURN_FAILED:
                        raise RuntimeError(fields["error"])

    def ask(self, prompt: str) -> str:
        return self.receive(*self.send(prompt))

    def run(self) -> None:
        """Run a small terminal chat; Ctrl-C leaves the conversation intact."""
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        while True:
            try:
                prompt = input("> ")
                if prompt:
                    print(self.ask(prompt))
            except (EOFError, KeyboardInterrupt):
                logger.debug("Chat %s closed from the terminal", self.conversation_id)
                print()
                return
            except RuntimeError as error:
                logger.debug("Chat %s turn failed: %s", self.conversation_id, error)
                print(f"Turn failed: {error}")
