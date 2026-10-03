"""Redis Stream names and event names shared by clients and workers."""

from dataclasses import dataclass
from enum import StrEnum


class EventType(StrEnum):
    CONVERSATION_CREATED = "conversation.created"
    TURN_QUEUED = "turn.queued"
    TURN_STARTED = "turn.started"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"


@dataclass(frozen=True)
class StreamNames:
    namespace: str = "fw"

    def __post_init__(self) -> None:
        if not self.namespace or ":" in self.namespace:
            raise ValueError("namespace must be nonempty and contain no colon")

    @property
    def work(self) -> str:
        return f"{self.namespace}:work"

    def conversation(self, conversation_id: str) -> str:
        if not conversation_id or ":" in conversation_id:
            raise ValueError("conversation ID must be nonempty and contain no colon")
        return f"{self.namespace}:conversation:{conversation_id}:events"
