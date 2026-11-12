"""Redis stream access for Fallingwater conversations."""

from collections.abc import Callable, Iterator, Mapping
from typing import Any, TypeVar

from redis import Redis
from redis.exceptions import ResponseError

from .models import (
    Action,
    ConversationEvent,
    ConversationPayload,
    ConversationPayloadType,
    DispatchEvent,
    DispatchPayloadType,
    Event,
    InvalidDispatchEvent,
    get_dispatch_action,
    get_cnv_event,
)
from .stream_utils import (
    decode_conversation_event,
    decode_dispatch_event,
    decode_text,
    encode_conversation_event,
    encode_dispatch_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from .utils import logger


GroupEntry = TypeVar("GroupEntry")

_APPEND_IF_CURRENT = """
local events = redis.call('XRANGE', KEYS[1], '(' .. ARGV[1], '+', 'COUNT', 100)
if #events > 0 then
    return {false, events}
end
local id = redis.call('XADD', KEYS[1], '*', 'type', ARGV[2], 'payload', ARGV[3])
return {id, {{id, {'type', ARGV[2], 'payload', ARGV[3]}}}}
"""


def _create_group(
    redis: Redis,
    stream: str,
    group: str,
    *,
    id: str,
    mkstream: bool | None = None,
) -> None:
    try:
        if mkstream is None:
            redis.xgroup_create(stream, group, id=id)
        else:
            redis.xgroup_create(stream, group, id=id, mkstream=mkstream)
    except ResponseError as error:
        if not str(error).startswith("BUSYGROUP"):
            logger.exception("Could not create consumer group for %s", stream)
            raise
        logger.debug("Consumer group %s already exists on %s", group, stream)


def _read_group(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    cursor: str,
    *,
    count: int,
    block: int | None,
    decode_entry: Callable[[str, Mapping[Any, Any]], GroupEntry],
) -> list[GroupEntry]:
    batches = redis.xreadgroup(
        group, consumer, {stream: cursor}, count=count, block=block
    )
    return [
        decode_entry(decode_text(entry_id), fields)
        for _stream_name, entries in batches
        for entry_id, fields in entries
    ]


class DispatchStream:
    """Access the shared Redis dispatch queue and create conversation streams."""

    def __init__(self, redis: Redis, *, namespace: str = "fw") -> None:
        self.redis = redis
        self.namespace = namespace
        self.name = fmt_dispatch_stream(namespace)

    def conversation_stream(self, conversation_id: str) -> "ConversationStream":
        return ConversationStream(self, conversation_id)

    def append_event(
        self, action_type: Action, payload: DispatchPayloadType
    ) -> str:
        """Append one dispatch action and return its Redis entry ID."""
        entry_id = self.redis.xadd(
            self.name, encode_dispatch_event(action_type, payload)
        )
        return decode_text(entry_id)

    def create_group(
        self, group: str, *, id: str = "0", mkstream: bool = False
    ) -> None:
        _create_group(self.redis, self.name, group, id=id, mkstream=mkstream)

    def append_payload(self, payload: DispatchPayloadType) -> str:
        """Append a supplied payload using its registered wire action type."""
        action = get_dispatch_action(type(payload))
        return self.append_event(action, payload)

    def read_group(
        self,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 1,
        block: int | None = None,
    ) -> list[DispatchEvent | InvalidDispatchEvent]:
        return _read_group(
            self.redis,
            self.name,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_entry=decode_dispatch_event,
        )

    def claim(
        self,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str,
        count: int = 1,
    ) -> tuple[str, list[DispatchEvent | InvalidDispatchEvent], list[str]]:
        next_cursor, entries, deleted_ids = self.redis.xautoclaim(
            self.name,
            group,
            consumer,
            min_idle_time=min_idle_time,
            start_id=start_id,
            count=count,
        )
        return (
            decode_text(next_cursor),
            [
                decode_dispatch_event(entry_id, fields)
                for entry_id, fields in entries
            ],
            [decode_text(entry_id) for entry_id in deleted_ids],
        )

    def acknowledge(self, group: str, *entry_ids: str) -> None:
        self.redis.xack(self.name, group, *entry_ids)


class ConversationStream:
    """Read and write one conversation's typed Redis events."""

    def __init__(self, dispatch: DispatchStream, conversation_id: str) -> None:
        self.dispatch = dispatch
        self.redis = dispatch.redis
        self.conversation_id = conversation_id
        self.name = fmt_conversation_stream(dispatch.namespace, conversation_id)

    def read_conversation_range(
        self,
        *,
        min: str = "-",
        max: str = "+",
        count: int | None = 100,
    ) -> list[ConversationEvent]:
        """Read and decode a range of conversation events."""
        entries = self.redis.xrange(self.name, min=min, max=max, count=count)
        return [
            decode_conversation_event(entry_id, fields)
            for entry_id, fields in entries
        ]

    def read_conversation_reverse_range(
        self, *, max: str = "+", min: str = "-", count: int = 100
    ) -> list[ConversationEvent]:
        """Read and decode events newest-first within Redis ID bounds."""
        entries = self.redis.xrevrange(self.name, max=max, min=min, count=count)
        return [
            decode_conversation_event(entry_id, fields)
            for entry_id, fields in entries
        ]

    def iter_events(
        self, *, after_id: str = "0-0", page_size: int = 100
    ) -> Iterator[ConversationEvent]:
        """Yield conversation events after an ID across Redis range pages."""
        if page_size < 1:
            raise ValueError("page_size must be positive")
        cursor = f"({after_id}"
        while entries := self.read_conversation_range(
            min=cursor, count=page_size
        ):
            yield from entries
            cursor = f"({entries[-1].entry_id}"

    def read_conversation(
        self,
        cursor: str,
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[ConversationEvent]:
        """Read and decode new conversation events after a cursor."""
        options: dict[str, Any] = {"block": block}
        if count is not None:
            options["count"] = count
        batches = self.redis.xread({self.name: cursor}, **options)
        return [
            decode_conversation_event(entry_id, fields)
            for _stream_name, entries in batches
            for entry_id, fields in entries
        ]

    def append_event(
        self, event_type: Event, payload: ConversationPayload
    ) -> str:
        """Encode and append one typed conversation event."""
        entry_id = self.redis.xadd(
            self.name, encode_conversation_event(event_type, payload)
        )
        return decode_text(entry_id)

    def append_payload(self, payload: ConversationPayloadType) -> str:
        """Append a supplied payload using its registered wire event type."""
        event = get_cnv_event(type(payload))
        return self.append_event(event, payload)

    def append_if_current(
        self,
        event_type: Event,
        payload: ConversationPayload,
        *,
        after_id: str,
    ) -> tuple[str | None, list[ConversationEvent]]:
        """Append at the replay cursor, or return up to 100 unseen events."""
        fields = encode_conversation_event(event_type, payload)
        entry_id, entries = self.redis.eval(
            _APPEND_IF_CURRENT,
            1,
            self.name,
            after_id,
            fields["type"],
            fields["payload"],
        )
        return (
            decode_text(entry_id) if entry_id is not None else None,
            [
                decode_conversation_event(
                    event_id,
                    dict(zip(values[::2], values[1::2], strict=True)),
                )
                for event_id, values in entries
            ],
        )

    def create_group(self, group: str, *, id: str = "0") -> None:
        _create_group(self.redis, self.name, group, id=id)

    def rewind_group(self, group: str) -> None:
        """Reset a reader group's delivery position to the stream beginning."""
        self.redis.xgroup_setid(self.name, group, id="0")

    def read_group(
        self,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 100,
        block: int | None = None,
    ) -> list[ConversationEvent]:
        """Read and decode conversation events through a consumer group."""
        return _read_group(
            self.redis,
            self.name,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_entry=decode_conversation_event,
        )

    def acknowledge(self, group: str, *entry_ids: str) -> None:
        self.redis.xack(self.name, group, *entry_ids)
