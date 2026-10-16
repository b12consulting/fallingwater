"""Redis stream names, event payloads, and access helpers."""

from collections.abc import Iterator, Mapping
from enum import StrEnum
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai.usage import RunUsage
from redis.exceptions import ResponseError

from .utils import compress, decompress, logger


RawStreamEntry: TypeAlias = tuple[Any, Mapping[Any, Any]]
StreamEntry: TypeAlias = tuple[str, Any]


class EventType(StrEnum):
    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_POKE = "conversation.poke"
    TURN_QUEUED = "turn.queued"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"


class ConversationPayloadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConversationCreatedPayload(ConversationPayloadModel):
    agent: str
    model: str = ""
    deps: dict[str, Any] = Field(default_factory=dict)


class TurnQueuedPayload(ConversationPayloadModel):
    turn_id: str
    message: str


class TurnCompletedPayload(ConversationPayloadModel):
    turn_id: str
    message: str
    history: str
    usage: RunUsage


class TurnFailedPayload(ConversationPayloadModel):
    turn_id: str
    error: str


ConversationPayload: TypeAlias = (
    ConversationCreatedPayload
    | TurnQueuedPayload
    | TurnCompletedPayload
    | TurnFailedPayload
)

PAYLOAD_MODELS: dict[EventType, type[ConversationPayload]] = {
    EventType.CONVERSATION_CREATED: ConversationCreatedPayload,
    EventType.TURN_QUEUED: TurnQueuedPayload,
    EventType.TURN_COMPLETED: TurnCompletedPayload,
    EventType.TURN_FAILED: TurnFailedPayload,
}


class ConversationEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: EventType
    payload: ConversationPayload


def decode_text(value: str | bytes) -> str:
    """Normalize Redis text values when response decoding is disabled."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _validate_namespace(namespace: str) -> None:
    if not namespace or ":" in namespace:
        raise ValueError("namespace must be nonempty and contain no colon")


def validate_conversation_id(conversation_id: str) -> None:
    if not conversation_id or ":" in conversation_id:
        raise ValueError(
            "conversation ID must be nonempty and contain no colon"
        )


def fmt_dispatch_stream(namespace: str) -> str:
    """Format the dispatch stream name after validating its namespace."""
    _validate_namespace(namespace)
    return f"{namespace}:dispatch"


def fmt_conversation_stream(namespace: str, conversation_id: str) -> str:
    """Format a conversation stream name after validating its components."""
    _validate_namespace(namespace)
    validate_conversation_id(conversation_id)
    return f"{namespace}:conversation:{conversation_id}:events"


class RedisStream:
    def __init__(self, redis, name: str):
        self.redis = redis
        self.name = name

    @staticmethod
    def decode_event(fields: Mapping[Any, Any]) -> dict[str, str]:
        """Decode a text-only stream entry such as a dispatch event."""
        return {
            decode_text(key): decode_text(value)
            for key, value in fields.items()
        }

    def decode_entry(self, entry: RawStreamEntry) -> StreamEntry:
        entry_id, fields = entry
        return decode_text(entry_id), self.decode_event(fields)

    def decode_entries(
        self, entries: list[RawStreamEntry]
    ) -> Iterator[StreamEntry]:
        return (self.decode_entry(entry) for entry in entries)

    def all(self) -> Iterator[StreamEntry]:
        lower = "-"
        while True:
            entries = self.redis.xrange(self.name, min=lower, count=100)
            yield from self.decode_entries(entries)
            if not entries:
                break
            lower = f"({decode_text(entries[-1][0])}"

    def one(self) -> Any | None:
        first = self.redis.xrange(self.name, count=1)
        if not first:
            return None
        return self.decode_entry(first[0])[1]

    def add(self, item: Mapping[str, str | bytes], *, pipeline=None) -> None:
        client = self.redis if pipeline is None else pipeline
        client.xadd(self.name, dict(item))

    def read_after(self, cursor: str, *, block: int) -> Iterator[StreamEntry]:
        batches = self.redis.xread({self.name: cursor}, block=block)
        entries = [entry for _, batch in batches for entry in batch]
        return self.decode_entries(entries)

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
    ) -> Iterator[StreamEntry]:
        return self._read(consumer, ">", count=count, block=block)

    def read_pending(
        self, consumer: str, cursor: str, *, count: int
    ) -> Iterator[StreamEntry]:
        return self._read(consumer, cursor, count=count, block=None)

    def _read(
        self,
        consumer: str,
        cursor: str,
        *,
        count: int,
        block: int | None,
    ) -> Iterator[StreamEntry]:
        batches = self.stream.redis.xreadgroup(
            self.name,
            consumer,
            {self.stream.name: cursor},
            count=count,
            block=block,
        )
        entries = [entry for _, batch in batches for entry in batch]
        return self.stream.decode_entries(entries)

    def acknowledge(self, *entry_ids: str) -> None:
        if entry_ids:
            self.stream.redis.xack(self.stream.name, self.name, *entry_ids)


class DispatchStream(RedisStream):
    def __init__(self, redis, namespace: str = "fw"):
        super().__init__(redis, fmt_dispatch_stream(namespace))


class ConversationStream(RedisStream):
    def __init__(self, redis, conversation_id: str, namespace: str = "fw"):
        super().__init__(
            redis, fmt_conversation_stream(namespace, conversation_id)
        )

    @staticmethod
    def decode_event(fields: Mapping[Any, Any]) -> ConversationEvent:
        text_fields = {
            decode_text(key): value for key, value in fields.items()
        }
        try:
            event_type = EventType(decode_text(text_fields["type"]))
            payload_data = text_fields["payload"]
        except (KeyError, ValueError, TypeError) as error:
            raise ValueError(
                "invalid conversation stream event envelope"
            ) from error
        if text_fields.keys() != {"type", "payload"}:
            raise ValueError(
                "conversation stream events must contain only type and payload"
            )

        payload_model = PAYLOAD_MODELS.get(event_type)
        if payload_model is None:
            raise ValueError(
                f"unsupported conversation event type: {event_type}"
            )
        if not isinstance(payload_data, bytes):
            raise ValueError(
                "conversation event payload must be compressed bytes"
            )
        try:
            payload_json = decompress(payload_data)
            payload = payload_model.model_validate_json(payload_json)
        except Exception as error:
            raise ValueError(
                f"invalid compressed payload for {event_type}"
            ) from error
        return ConversationEvent(type=event_type, payload=payload)

    def decode_entry(self, entry: RawStreamEntry) -> StreamEntry:
        entry_id, fields = entry
        return decode_text(entry_id), self.decode_event(fields)

    def add(
        self,
        event_type: EventType,
        payload: ConversationPayload,
        *,
        pipeline=None,
    ) -> None:
        payload_model = PAYLOAD_MODELS.get(event_type)
        if payload_model is None or not isinstance(payload, payload_model):
            raise TypeError(
                f"invalid payload for conversation event {event_type}"
            )
        compressed_payload = compress(payload.model_dump_json().encode("utf-8"))
        super().add(
            {"type": event_type.value, "payload": compressed_payload},
            pipeline=pipeline,
        )

    def one(self) -> ConversationEvent | None:
        first = self.redis.xrange(self.name, count=1)
        if not first:
            return None
        return self.decode_event(first[0][1])
