"""Synchronous Redis-backed conversation client."""

from uuid import uuid4

from redis import Redis
from redis.exceptions import ResponseError

from .catalog import reify_agent
from .streams import EventType, StreamNames
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
    ) -> None:
        self.redis = redis
        self.names = StreamNames(namespace)
        if not group:
            raise ValueError("chat group must be nonempty")
        self.group = group
        self._pending_turns: dict[str, str] = {}
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

    def _ensure_group(self) -> None:
        try:
            self.redis.xgroup_create(self.events, self.group, id="0")
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create chat group %s on %s", self.group, self.events
                )
                raise
            logger.debug("Chat group %s already exists on %s", self.group, self.events)

    def _handle_group_event(self, entry_id: str, fields: dict[str, str]) -> str | None:
        event_type = fields.get("type")
        turn_id = fields.get("turn_id")
        if event_type == EventType.TURN_QUEUED and turn_id is not None:
            self._pending_turns[turn_id] = entry_id
            return None

        if event_type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED):
            if event_type == EventType.TURN_COMPLETED:
                print(fields["output"], flush=True)
            else:
                print(f"Turn failed: {fields['error']}", flush=True)
            queued_id = self._pending_turns.pop(turn_id, None)
            if queued_id is None:
                self.redis.xack(self.events, self.group, entry_id)
            else:
                self.redis.xack(self.events, self.group, queued_id, entry_id)
            return turn_id

        self.redis.xack(self.events, self.group, entry_id)
        return None

    def _drain_pending(self) -> None:
        """Replay entries assigned to this stable consumer before reading new ones."""
        cursor = "0"
        while True:
            batches = self.redis.xreadgroup(
                self.group, self.consumer, {self.events: cursor}, count=100
            )
            if not any(entries for _, entries in batches):
                return
            for _, entries in batches:
                for entry_id, fields in entries:
                    cursor = entry_id
                    self._handle_group_event(entry_id, fields)

    def _read_new(self, *, block: int | None = None) -> set[str]:
        completed: set[str] = set()
        while True:
            batches = self.redis.xreadgroup(
                self.group, self.consumer, {self.events: ">"}, count=100, block=block
            )
            if not any(entries for _, entries in batches):
                return completed
            for _, entries in batches:
                for entry_id, fields in entries:
                    turn_id = self._handle_group_event(entry_id, fields)
                    if turn_id is not None:
                        completed.add(turn_id)
            if block is not None:
                return completed

    def run(self) -> None:
        """Run a small terminal chat; Ctrl-C leaves the conversation intact."""
        self._ensure_group()
        print(f"Conversation: {self.conversation_id}")
        print(f"Model: {self.model}")
        print(f"Agent: {self.agent_path}")
        try:
            self._drain_pending()
            self._read_new()
            while True:
                while self._pending_turns:
                    self._read_new(block=1000)
                prompt = input("> ")
                if prompt:
                    turn_id, _ = self.send(prompt)
                    while turn_id not in self._read_new(block=1000):
                        pass
        except (EOFError, KeyboardInterrupt):
            logger.debug("Chat %s closed from the terminal", self.conversation_id)
            print()
            return
