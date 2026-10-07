"""Redis Stream names and event names shared by clients and workers."""

from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias

from redis.exceptions import ResponseError

from .utils import logger


StreamEntry: TypeAlias = tuple[str, dict[str, str]]


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
            raise ValueError(
                "conversation ID must be nonempty and contain no colon"
            )
        return f"{self.namespace}:conversation:{conversation_id}:events"


class RedisStream:
    def __init__(self, redis, name: str):
        self.redis = redis
        self.name = name

    def all(self) -> Iterator[StreamEntry]:
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

    def add(self, item: dict, *, pipeline=None) -> None:
        client = self.redis if pipeline is None else pipeline
        client.xadd(self.name, item)

    def read_after(self, cursor: str, *, block: int) -> list[StreamEntry]:
        batches = self.redis.xread({self.name: cursor}, block=block)
        return [entry for _, entries in batches for entry in entries]

    def group(self, name: str, *, mkstream: bool = False) -> "ReadGroup":
        options = {"id": "0"}
        if mkstream:
            options["mkstream"] = True
        try:
            self.redis.xgroup_create(self.name, name, **options)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s", self.name
                )
                raise
            logger.debug(
                "Consumer group %s already exists on %s", name, self.name
            )
        return ReadGroup(self, name)


class ReadGroup:
    def __init__(self, stream: RedisStream, name: str):
        self.stream = stream
        self.name = name

    def read_new(
        self, consumer: str, *, count: int, block: int | None
    ) -> list[StreamEntry]:
        return self._read(consumer, ">", count=count, block=block)

    def read_pending(
        self, consumer: str, cursor: str, *, count: int
    ) -> list[StreamEntry]:
        return self._read(consumer, cursor, count=count, block=None)

    def _read(
        self,
        consumer: str,
        cursor: str,
        *,
        count: int,
        block: int | None,
    ) -> list[StreamEntry]:
        batches = self.stream.redis.xreadgroup(
            self.name,
            consumer,
            {self.stream.name: cursor},
            count=count,
            block=block,
        )
        return [entry for _, entries in batches for entry in entries]

    def acknowledge(self, *entry_ids: str) -> None:
        if entry_ids:
            self.stream.redis.xack(self.stream.name, self.name, *entry_ids)


class DispatchStream(RedisStream):
    def __init__(self, redis, namespace: str = "fw"):
        name = StreamNames(namespace).dispatch
        super().__init__(redis, name)


class ConversationStream(RedisStream):
    def __init__(self, redis, conversation_id: str, namespace: str = "fw"):
        name = StreamNames(namespace).conversation(conversation_id)
        super().__init__(redis, name)
