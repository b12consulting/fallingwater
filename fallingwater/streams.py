"""Redis Stream names and event names shared by clients and workers."""

from dataclasses import dataclass
from enum import StrEnum


class EventType(StrEnum):
    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_POKE = "conversation.poke"
    TURN_QUEUED = "turn.queued"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"


@dataclass(frozen=True)
class StreamNames:
    namespace: str = "fw"

    def __post_init__(self) -> None:
        if not self.namespace or ":" in self.namespace:
            raise ValueError("namespace must be nonempty and contain no colon")

    @property
    def dispatch(self) -> str:
        return f"{self.namespace}:dispatch"

    def conversation(self, conversation_id: str) -> str:
        if not conversation_id or ":" in conversation_id:
            raise ValueError("conversation ID must be nonempty and contain no colon")
        return f"{self.namespace}:conversation:{conversation_id}:events"


class RedisStream:

    def __init__(self, redis, namespace: str):
        self.redis = redis
        self.namespace = namespace

    def all(self):
        lower = "-"
        while True:
            entries = self.redis.xrange(self.name, min=lower, count=100)
            yield from entries
            if not entries:
                break
            lower = f"({entries[-1][0]}"

    def one(self):
        first = self.redis.xrange(self.name, count=1)
        if not first:
            return None
        return first[0][1]

    def add(self, item: dict):
        self.redis.xadd(self.name, item)


class DispatchStream(RedisStream):
    def __init__(self, redis, namespace: str = "fw"):
        self.name = f"{namespace}:dispatch"
        super().__init__(redis, namespace)


class ConversationStream(RedisStream):
    def __init__(self, redis, conversation_id: str, namespace: str = "fw"):
        self.name = f"{namespace}:conversation:{conversation_id}:events"
        super().__init__(redis, namespace)
