"""Redis access and event encoding for Fallingwater conversations."""

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from redis import Redis
from redis.exceptions import WatchError

from .streams import (
    ConversationEvent,
    ConversationPayload,
    ConversationCreatedPayload,
    DispatchType,
    EventType,
    ParentToolReference,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnQueuedPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
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

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        conversation_id: str | None = None,
    ) -> None:
        self.redis = redis
        self.namespace = namespace
        self.conversation_id = conversation_id
        self.dispatch_stream = fmt_dispatch_stream(namespace)

    def _resolve_conversation_id(self, conversation_id: str | None = None) -> str:
        resolved = (
            conversation_id if conversation_id is not None else self.conversation_id
        )
        if resolved is None:
            raise ValueError("conversation ID is required")
        return resolved

    def conversation_stream(self, conversation_id: str | None = None) -> str:
        """Return this client's event stream or one for an explicit ID."""
        return fmt_conversation_stream(
            self.namespace, self._resolve_conversation_id(conversation_id)
        )

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

    def queue_turn(
        self,
        turn_id: str,
        prompt: str,
        *,
        conversation_id: str | None = None,
    ) -> str:
        """Atomically append a queued prompt and its dispatch poke."""
        conversation_id = self._resolve_conversation_id(conversation_id)
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

    def queue_tool_requests(
        self,
        payload: ToolRequestedPayload,
        call_ids: list[str],
        *,
        conversation_id: str | None = None,
    ) -> None:
        """Persist one deferred request set and fan out one dispatch per call."""
        conversation_id = self._resolve_conversation_id(conversation_id)
        if len(call_ids) != len(set(call_ids)):
            raise ValueError("deferred tool call IDs must be unique")
        stream = self.conversation_stream(conversation_id)
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                stream,
                encode_conversation_event(EventType.TURN_TOOL_REQUESTED, payload),
            )
            for tool_call_id in call_ids:
                pipeline.xadd(
                    self.dispatch_stream,
                    {
                        "type": DispatchType.TOOL_CALL,
                        "conversation_id": conversation_id,
                        "turn_id": payload.turn_id,
                        "step_id": payload.step_id,
                        "tool_call_id": tool_call_id,
                    },
                )
            if not call_ids:
                pipeline.xadd(
                    self.dispatch_stream,
                    {
                        "type": DispatchType.AGENT_RESUME,
                        "conversation_id": conversation_id,
                        "turn_id": payload.turn_id,
                        "step_id": payload.step_id,
                    },
                )
            pipeline.execute()

    def queue_subagent(
        self,
        *,
        parent_request: SubAgentRequestedPayload,
        child_agent: str,
        child_model: str,
        child_deps: dict[str, Any],
        child_prompt: str,
        parent_conversation_id: str | None = None,
    ) -> bool:
        """Atomically record a child request and queue its first turn."""
        parent_conversation_id = self._resolve_conversation_id(
            parent_conversation_id
        )
        parent_stream = self.conversation_stream(parent_conversation_id)
        child_stream = self.conversation_stream(parent_request.conversation_id)
        reference = ParentToolReference(
            conversation_id=parent_conversation_id,
            turn_id=parent_request.turn_id,
            step_id=parent_request.step_id,
            tool_call_id=parent_request.tool_call_id,
        )
        child_turn_id = parent_request.child_turn_id
        while True:
            with self.redis.pipeline() as pipeline:
                try:
                    pipeline.watch(parent_stream)
                    parent_entries = pipeline.xrange(parent_stream)
                    if any(
                        event.type == EventType.TURN_SUBAGENT_REQUESTED
                        and event.payload.turn_id == parent_request.turn_id
                        and event.payload.step_id == parent_request.step_id
                        and event.payload.tool_call_id == parent_request.tool_call_id
                        for _entry_id, fields in parent_entries
                        for event in [decode_conversation_event(fields)]
                    ):
                        pipeline.unwatch()
                        return False
                    pipeline.multi()
                    pipeline.xadd(
                        parent_stream,
                        encode_conversation_event(
                            EventType.TURN_SUBAGENT_REQUESTED, parent_request
                        ),
                    )
                    pipeline.xadd(
                        child_stream,
                        encode_conversation_event(
                            EventType.CONVERSATION_CREATED,
                            ConversationCreatedPayload(
                                agent=child_agent,
                                model=child_model,
                                deps=child_deps,
                                parent=reference,
                            ),
                        ),
                    )
                    pipeline.xadd(
                        child_stream,
                        encode_conversation_event(
                            EventType.TURN_QUEUED,
                            TurnQueuedPayload(
                                turn_id=child_turn_id, message=child_prompt
                            ),
                        ),
                    )
                    pipeline.xadd(
                        self.dispatch_stream,
                        {
                            "type": DispatchType.CONVERSATION_POKE,
                            "conversation_id": parent_request.conversation_id,
                            "turn_id": child_turn_id,
                        },
                    )
                    pipeline.execute()
                    return True
                except WatchError:
                    continue

    def save_tool_result(
        self,
        payload: ToolResultPayload,
        expected_call_ids: list[str],
        *,
        conversation_id: str | None = None,
    ) -> bool:
        """Save a unique call result and queue one resume after the final result."""
        conversation_id = self._resolve_conversation_id(conversation_id)
        stream = self.conversation_stream(conversation_id)
        expected = set(expected_call_ids)
        if payload.tool_call_id not in expected:
            raise ValueError("tool result is not part of the deferred request set")

        while True:
            with self.redis.pipeline() as pipeline:
                try:
                    pipeline.watch(stream)
                    entries = pipeline.xrange(stream)
                    prior_results: set[str] = set()
                    request_found = False
                    terminal = False
                    for _entry_id, fields in entries:
                        event = decode_conversation_event(fields)
                        if getattr(event.payload, "turn_id", None) != payload.turn_id:
                            continue
                        if event.type == EventType.TURN_TOOL_REQUESTED:
                            requested = event.payload
                            if requested.step_id == payload.step_id:
                                request_found = True
                        elif event.type == EventType.TURN_TOOL_RESULT:
                            result = event.payload
                            if result.step_id == payload.step_id:
                                prior_results.add(result.tool_call_id)
                        elif event.type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED):
                            terminal = True

                    if terminal or payload.tool_call_id in prior_results:
                        pipeline.unwatch()
                        return False
                    if not request_found:
                        raise ValueError("tool result has no matching request step")

                    all_results = prior_results | {payload.tool_call_id}
                    pipeline.multi()
                    pipeline.xadd(
                        stream,
                        encode_conversation_event(EventType.TURN_TOOL_RESULT, payload),
                    )
                    if expected <= all_results:
                        pipeline.xadd(
                            self.dispatch_stream,
                            {
                                "type": DispatchType.AGENT_RESUME,
                                "conversation_id": conversation_id,
                                "turn_id": payload.turn_id,
                                "step_id": payload.step_id,
                            },
                        )
                    pipeline.execute()
                    return True
                except WatchError:
                    continue

    def finish_turn(
        self,
        event_type: EventType,
        payload: TurnCompletedPayload | TurnFailedPayload,
        *,
        parent: ParentToolReference | None,
        conversation_id: str | None = None,
    ) -> bool:
        """Persist one terminal event and notify its parent in the same transaction."""
        conversation_id = self._resolve_conversation_id(conversation_id)
        stream = self.conversation_stream(conversation_id)
        notification = None
        if parent is not None:
            notification = {
                "type": DispatchType.SUBAGENT_RESULT,
                "conversation_id": parent.conversation_id,
                "turn_id": parent.turn_id,
                "step_id": parent.step_id,
                "tool_call_id": parent.tool_call_id,
                "child_conversation_id": conversation_id,
                "child_turn_id": payload.turn_id,
            }

        while True:
            with self.redis.pipeline() as pipeline:
                try:
                    pipeline.watch(stream)
                    entries = pipeline.xrange(stream)
                    if any(
                        event.type in (EventType.TURN_COMPLETED, EventType.TURN_FAILED)
                        and getattr(event.payload, "turn_id", None) == payload.turn_id
                        for _entry_id, fields in entries
                        for event in [decode_conversation_event(fields)]
                    ):
                        pipeline.unwatch()
                        return False
                    pipeline.multi()
                    pipeline.xadd(
                        stream, encode_conversation_event(event_type, payload)
                    )
                    if notification is not None:
                        pipeline.xadd(self.dispatch_stream, notification)
                    pipeline.execute()
                    return True
                except WatchError:
                    continue

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
