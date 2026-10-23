"""Redis access and event encoding for Fallingwater conversations."""

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from redis import Redis

from .streams import (
    ConversationEvent,
    ConversationPayload,
    EventType,
    TurnQueuedPayload,
    decode_conversation_event,
    decode_dispatch_fields,
    decode_text,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)


ConversationEntry = tuple[str, ConversationEvent]
DispatchEntry = tuple[str, dict[str, str]]
GroupEntry = TypeVar("GroupEntry")


class ConversationClient:
    """Share Redis stream operations and codecs across chat and worker flows.

    Group and consumer names stay explicit on group operations because a
    conversation can have terminal readers while dispatch work uses its own
    consumer group.
    """

    def __init__(self, redis: Redis, *, namespace: str = "fw") -> None:
        self.redis = redis
        self.namespace = namespace
        self.dispatch_stream = fmt_dispatch_stream(namespace)

    def conversation_stream(self, conversation_id: str) -> str:
        """Return the event stream for a conversation ID."""
        return fmt_conversation_stream(self.namespace, conversation_id)

    def read_conversation_range(
        self, stream: str, *, min: str = "-", count: int = 100
    ) -> list[ConversationEntry]:
        """Read and decode a range of conversation events."""
        entries = self.redis.xrange(stream, min=min, count=count)
        return [
            (decode_text(entry_id), decode_conversation_event(fields))
            for entry_id, fields in entries
        ]

    def read_conversation(
        self,
        stream: str,
        cursor: str,
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[ConversationEntry]:
        """Read and decode new conversation events after a cursor."""
        options: dict[str, Any] = {"block": block}
        if count is not None:
            options["count"] = count
        batches = self.redis.xread({stream: cursor}, **options)
        return [
            (decode_text(entry_id), decode_conversation_event(fields))
            for _stream_name, entries in batches
            for entry_id, fields in entries
        ]

    def append_event(
        self, stream: str, event_type: EventType, payload: ConversationPayload
    ) -> str:
        """Encode and append one typed conversation event."""
        entry_id = self.redis.xadd(
            stream, encode_conversation_event(event_type, payload)
        )
        return decode_text(entry_id)

    def queue_turn(self, conversation_id: str, turn_id: str, prompt: str) -> str:
        """Atomically append a queued prompt and its dispatch poke."""
        conversation_stream = self.conversation_stream(conversation_id)
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                conversation_stream,
                encode_conversation_event(
                    EventType.TURN_QUEUED,
                    TurnQueuedPayload(turn_id=turn_id, message=prompt),
                ),
            )
            pipeline.xadd(
                self.dispatch_stream,
                {
                    "type": EventType.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": conversation_id,
                },
            )
            queued_id, _dispatch_id = pipeline.execute()
        return decode_text(queued_id)

    def create_group(
        self, stream: str, group: str, *, id: str = "0", mkstream: bool = False
    ) -> None:
        """Create a consumer group for either stream role."""
        if mkstream:
            self.redis.xgroup_create(stream, group, id=id, mkstream=True)
        else:
            self.redis.xgroup_create(stream, group, id=id)

    def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 100,
        block: int | None = None,
    ) -> list[ConversationEntry]:
        """Read and decode conversation events through a consumer group."""
        return self._read_group(
            stream,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_fields=decode_conversation_event,
        )

    def _read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int,
        block: int | None,
        decode_fields: Callable[[Mapping[Any, Any]], GroupEntry],
    ) -> list[tuple[str, GroupEntry]]:
        """Read and decode entries from a Redis consumer group."""
        batches = self.redis.xreadgroup(
            group,
            consumer,
            {stream: cursor},
            count=count,
            block=block,
        )
        return [
            (decode_text(entry_id), decode_fields(fields))
            for _stream_name, entries in batches
            for entry_id, fields in entries
        ]

    def read_dispatch_group(
        self,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 1,
        block: int | None = None,
    ) -> list[DispatchEntry]:
        """Read dispatch entries and normalize their IDs and text fields."""
        return self._read_group(
            self.dispatch_stream,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_fields=decode_dispatch_fields,
        )

    def claim_dispatch(
        self,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str,
        count: int = 1,
    ) -> tuple[str, list[DispatchEntry], list[str]]:
        """Claim pending dispatch entries and return Redis' next scan cursor."""
        next_cursor, entries, deleted_ids = self.redis.xautoclaim(
            self.dispatch_stream,
            group,
            consumer,
            min_idle_time=min_idle_time,
            start_id=start_id,
            count=count,
        )
        return (
            decode_text(next_cursor),
            [
                (decode_text(entry_id), decode_dispatch_fields(fields))
                for entry_id, fields in entries
            ],
            [decode_text(entry_id) for entry_id in deleted_ids],
        )

    def acknowledge(self, stream: str, group: str, *entry_ids: str) -> None:
        """Acknowledge one or more entries in the selected stream group."""
        self.redis.xack(stream, group, *entry_ids)
