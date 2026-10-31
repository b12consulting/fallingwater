"""Redis access and coordination for Fallingwater conversations."""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event as ThreadEvent, Thread
from typing import Any, TypeVar

from redis import Redis
from redis.exceptions import LockError, RedisError, WatchError

from .stream_utils import (
    ConversationStreamEvent,
    ConversationPayload,
    ConversationCreatedPayload,
    DispatchAction,
    Action,
    Event,
    ParentToolReference,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnQueuedPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    decode_conversation_event,
    decode_conversation_stream_event,
    decode_dispatch_action,
    decode_text,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
    InvalidDispatchAction,
)


GroupEntry = TypeVar("GroupEntry")


@dataclass(frozen=True)
class AgentRunLease:
    """Report whether a renewable agent-run lock was lost."""

    lost: ThreadEvent

    def ensure_held(self) -> None:
        if self.lost.is_set():
            raise RedisError("agent run lease was lost while running")


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

    def create_group(
        self, group: str, *, id: str = "0", mkstream: bool = False
    ) -> None:
        self.redis.xgroup_create(self.name, group, id=id, mkstream=mkstream)

    def read_group(
        self,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 1,
        block: int | None = None,
    ) -> list[DispatchAction | InvalidDispatchAction]:
        return _read_group(
            self.redis,
            self.name,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_entry=decode_dispatch_action,
        )

    def claim(
        self,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str,
        count: int = 1,
    ) -> tuple[str, list[DispatchAction | InvalidDispatchAction], list[str]]:
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
                decode_dispatch_action(entry_id, fields)
                for entry_id, fields in entries
            ],
            [decode_text(entry_id) for entry_id in deleted_ids],
        )

    def acknowledge(self, group: str, *entry_ids: str) -> None:
        self.redis.xack(self.name, group, *entry_ids)


class ConversationStream:
    """Access one conversation's events and atomically dispatch its actions."""

    def __init__(self, dispatch: DispatchStream, conversation_id: str) -> None:
        self.dispatch = dispatch
        self.redis = dispatch.redis
        self.conversation_id = conversation_id
        self.name = fmt_conversation_stream(dispatch.namespace, conversation_id)

    @contextmanager
    def lock_agent_run(
        self, turn_id: str, step_id: str, *, lease_seconds: float
    ) -> Iterator[AgentRunLease | None]:
        """Hold a renewable per-step lease for the duration of one agent run."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        lock_name = (
            f"{self.dispatch.namespace}:agent-run:"
            f"{self.conversation_id}:{turn_id}:{step_id}"
        )
        lock = self.redis.lock(
            lock_name,
            timeout=lease_seconds,
            blocking=False,
            thread_local=False,
        )
        if not lock.acquire(blocking=False):
            yield None
            return

        stop_heartbeat = ThreadEvent()
        lost = ThreadEvent()

        def renew() -> None:
            while not stop_heartbeat.wait(lease_seconds / 3):
                try:
                    if not lock.extend(lease_seconds, replace_ttl=True):
                        lost.set()
                        return
                except RedisError:
                    lost.set()
                    return

        heartbeat = Thread(target=renew, daemon=True)
        try:
            heartbeat.start()
            try:
                yield AgentRunLease(lost)
            finally:
                stop_heartbeat.set()
                heartbeat.join()
        finally:
            try:
                lock.release()
            except LockError:
                pass

    def read_conversation_range(
        self, *, min: str = "-", count: int = 100
    ) -> list[ConversationStreamEvent]:
        """Read and decode a range of conversation events."""
        entries = self.redis.xrange(self.name, min=min, count=count)
        return [
            decode_conversation_stream_event(entry_id, fields)
            for entry_id, fields in entries
        ]

    def iter_events(
        self, *, page_size: int = 100
    ) -> Iterator[ConversationStreamEvent]:
        """Yield all conversation events across successive Redis range pages."""
        if page_size < 1:
            raise ValueError("page_size must be positive")
        cursor = "-"
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
    ) -> list[ConversationStreamEvent]:
        """Read and decode new conversation events after a cursor."""
        options: dict[str, Any] = {"block": block}
        if count is not None:
            options["count"] = count
        batches = self.redis.xread({self.name: cursor}, **options)
        return [
            decode_conversation_stream_event(entry_id, fields)
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

    def queue_turn(
        self,
        turn_id: str,
        prompt: str,
    ) -> str:
        """Atomically append a queued prompt and its dispatch poke."""
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.name,
                encode_conversation_event(
                    Event.TURN_QUEUED,
                    TurnQueuedPayload(turn_id=turn_id, message=prompt),
                ),
            )
            pipeline.xadd(
                self.dispatch.name,
                {
                    "type": Action.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": self.conversation_id,
                },
            )
            queued_id, _dispatch_id = pipeline.execute()
        return decode_text(queued_id)

    def queue_tool_requests(
        self,
        payload: ToolRequestedPayload,
        call_ids: list[str],
    ) -> None:
        """Persist one deferred request set and fan out one dispatch per call."""
        if len(call_ids) != len(set(call_ids)):
            raise ValueError("deferred tool call IDs must be unique")
        with self.redis.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                self.name,
                encode_conversation_event(
                    Event.TURN_TOOL_REQUESTED, payload
                ),
            )
            for tool_call_id in call_ids:
                pipeline.xadd(
                    self.dispatch.name,
                    {
                        "type": Action.TOOL_CALL,
                        "conversation_id": self.conversation_id,
                        "turn_id": payload.turn_id,
                        "step_id": payload.step_id,
                        "tool_call_id": tool_call_id,
                    },
                )
            if not call_ids:
                pipeline.xadd(
                    self.dispatch.name,
                    {
                        "type": Action.AGENT_RESUME,
                        "conversation_id": self.conversation_id,
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
    ) -> bool:
        """Atomically record a child request and queue its first turn."""
        parent_stream = self.name
        child_stream = self.dispatch.conversation_stream(
            parent_request.conversation_id
        ).name
        reference = ParentToolReference(
            conversation_id=self.conversation_id,
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
                        event.type == Event.TURN_SUBAGENT_REQUESTED
                        and event.payload.turn_id == parent_request.turn_id
                        and event.payload.step_id == parent_request.step_id
                        and event.payload.tool_call_id
                        == parent_request.tool_call_id
                        for _entry_id, fields in parent_entries
                        for event in [decode_conversation_event(fields)]
                    ):
                        pipeline.unwatch()
                        return False
                    pipeline.multi()
                    pipeline.xadd(
                        parent_stream,
                        encode_conversation_event(
                            Event.TURN_SUBAGENT_REQUESTED, parent_request
                        ),
                    )
                    pipeline.xadd(
                        child_stream,
                        encode_conversation_event(
                            Event.CONVERSATION_CREATED,
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
                            Event.TURN_QUEUED,
                            TurnQueuedPayload(
                                turn_id=child_turn_id, message=child_prompt
                            ),
                        ),
                    )
                    pipeline.xadd(
                        self.dispatch.name,
                        {
                            "type": Action.CONVERSATION_POKE,
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
    ) -> bool:
        """Save a unique call result and queue one resume after the final result."""
        stream = self.name
        expected = set(expected_call_ids)
        if payload.tool_call_id not in expected:
            raise ValueError(
                "tool result is not part of the deferred request set"
            )

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
                        if (
                            getattr(event.payload, "turn_id", None)
                            != payload.turn_id
                        ):
                            continue
                        if event.type == Event.TURN_TOOL_REQUESTED:
                            requested = event.payload
                            if requested.step_id == payload.step_id:
                                request_found = True
                        elif event.type == Event.TURN_TOOL_RESULT:
                            result = event.payload
                            if result.step_id == payload.step_id:
                                prior_results.add(result.tool_call_id)
                        elif event.type in (
                            Event.TURN_COMPLETED,
                            Event.TURN_FAILED,
                        ):
                            terminal = True

                    if terminal or payload.tool_call_id in prior_results:
                        pipeline.unwatch()
                        return False
                    if not request_found:
                        raise ValueError(
                            "tool result has no matching request step"
                        )

                    all_results = prior_results | {payload.tool_call_id}
                    pipeline.multi()
                    pipeline.xadd(
                        stream,
                        encode_conversation_event(
                            Event.TURN_TOOL_RESULT, payload
                        ),
                    )
                    if expected <= all_results:
                        pipeline.xadd(
                            self.dispatch.name,
                            {
                                "type": Action.AGENT_RESUME,
                                "conversation_id": self.conversation_id,
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
        event_type: Event,
        payload: TurnCompletedPayload | TurnFailedPayload,
        *,
        parent: ParentToolReference | None,
    ) -> bool:
        """Persist one terminal event and notify its parent in the same transaction."""
        stream = self.name
        notification = None
        if parent is not None:
            notification = {
                "type": Action.SUBAGENT_RESULT,
                "conversation_id": parent.conversation_id,
                "turn_id": parent.turn_id,
                "step_id": parent.step_id,
                "tool_call_id": parent.tool_call_id,
                "child_conversation_id": self.conversation_id,
                "child_turn_id": payload.turn_id,
            }

        while True:
            with self.redis.pipeline() as pipeline:
                try:
                    pipeline.watch(stream)
                    entries = pipeline.xrange(stream)
                    if any(
                        event.type
                        in (Event.TURN_COMPLETED, Event.TURN_FAILED)
                        and getattr(event.payload, "turn_id", None)
                        == payload.turn_id
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
                        pipeline.xadd(self.dispatch.name, notification)
                    pipeline.execute()
                    return True
                except WatchError:
                    continue

    def create_group(self, group: str, *, id: str = "0") -> None:
        self.redis.xgroup_create(self.name, group, id=id)

    def read_group(
        self,
        group: str,
        consumer: str,
        cursor: str,
        *,
        count: int = 100,
        block: int | None = None,
    ) -> list[ConversationStreamEvent]:
        """Read and decode conversation events through a consumer group."""
        return _read_group(
            self.redis,
            self.name,
            group,
            consumer,
            cursor,
            count=count,
            block=block,
            decode_entry=decode_conversation_stream_event,
        )

    def acknowledge(self, group: str, *entry_ids: str) -> None:
        self.redis.xack(self.name, group, *entry_ids)
