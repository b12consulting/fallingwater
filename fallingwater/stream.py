"""Redis stream access for Fallingwater conversations."""

from collections.abc import Callable, Iterator, Mapping
from typing import Any, TypeVar

from redis import Redis
from redis.exceptions import ResponseError

from .models import (
    Action,
    ConversationEvent,
    ConversationPayload,
    ConversationCreatedPayload,
    DispatchEvent,
    DispatchPayloadType,
    DISPATCH_PAYLOAD_MODELS,
    Event,
    PAYLOAD_MODELS,
    ParentToolReference,
    SubAgentRequestedPayload,
    ToolResultPayload,
    TurnQueuedPayload,
    InvalidDispatchEvent,
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
from .turn_state import (
    Conversation,
    Emission,
    ParentNotification,
    TurnPayload,
    TurnStateMachine,
)


GroupEntry = TypeVar("GroupEntry")


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
        _create_group(
            self.redis, self.name, group, id=id, mkstream=mkstream
        )

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
    """Access one conversation's events and dispatch its actions."""

    def __init__(self, dispatch: DispatchStream, conversation_id: str) -> None:
        self.dispatch = dispatch
        self.redis = dispatch.redis
        self.conversation_id = conversation_id
        self.name = fmt_conversation_stream(dispatch.namespace, conversation_id)

    def read_conversation_range(
        self, *, min: str = "-", count: int = 100
    ) -> list[ConversationEvent]:
        """Read and decode a range of conversation events."""
        entries = self.redis.xrange(self.name, min=min, count=count)
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

    def queue_turn(
        self,
        turn_id: str,
        prompt: str,
    ) -> str:
        """Append a queued prompt, then dispatch its turn."""
        return self.checkpoint(
            TurnQueuedPayload(turn_id=turn_id, message=prompt),
            TurnStateMachine(self.conversation_id, turn_id),
        )

    def publish(self, actions: tuple[Emission, ...], entry_id: str) -> None:
        """Publish machine emissions, binding terminal IDs after persistence."""
        for emission in actions:
            payload = (
                emission.dispatch(entry_id)
                if isinstance(emission, ParentNotification)
                else emission
            )
            self.dispatch.append_event(
                next(
                    action
                    for action, model in DISPATCH_PAYLOAD_MODELS.items()
                    if isinstance(payload, model)
                ),
                payload,
            )

    def checkpoint(
        self, payload: TurnPayload, machine: TurnStateMachine
    ) -> str:
        """Persist a validated fact and emissions; refresh snapshots by replay."""
        actions = machine.preview(payload)
        event_type = next(
            event
            for event, model in PAYLOAD_MODELS.items()
            if isinstance(payload, model)
        )
        entry_id = self.append_event(event_type, payload)
        self.publish(actions, entry_id)
        return entry_id

    def queue_subagent(
        self,
        *,
        parent_request: SubAgentRequestedPayload,
        child_agent: str,
        child_model: str,
        child_deps: dict[str, Any],
        child_prompt: str,
        machine: TurnStateMachine,
    ) -> bool:
        """Record a child request and queue its first turn."""
        actions = machine.preview(parent_request)
        child_stream = self.dispatch.conversation_stream(
            parent_request.child_conversation_id
        )
        reference = ParentToolReference(
            conversation_id=self.conversation_id,
            turn_id=parent_request.turn_id,
            step_id=parent_request.step_id,
            tool_call_id=parent_request.tool_call_id,
        )
        # A queued child can reconstruct its parent request during recovery.
        child_stream.append_event(
            Event.CONVERSATION_CREATED,
            ConversationCreatedPayload(
                agent=child_agent,
                model=child_model,
                deps=child_deps,
                parent=reference,
            ),
        )
        child_stream.append_event(
            Event.TURN_QUEUED,
            TurnQueuedPayload(
                turn_id=parent_request.child_turn_id,
                message=child_prompt,
            ),
        )
        entry_id = self.append_event(
            Event.TURN_SUBAGENT_REQUESTED, parent_request
        )
        self.publish(actions, entry_id)
        return True

    def refresh(self, conversation: Conversation) -> None:
        """Fold the tail into an existing snapshot without publishing actions."""
        for event in self.iter_events(after_id=conversation.last_event_id):
            conversation.apply(event)

    def save_tool_result(
        self,
        payload: ToolResultPayload,
        conversation: Conversation,
    ) -> bool:
        """Reconcile concurrent facts before saving a result and emitting fan-in."""
        self.refresh(conversation)
        machine = conversation.turn_state[payload.turn_id]
        if not machine.accepts_result(payload):
            return False
        machine.preview(payload)
        entry_id = self.append_event(Event.TURN_TOOL_RESULT, payload)
        # Do not advance to our write's ID: a sibling may precede it in the tail.
        self.refresh(conversation)
        self.publish(machine.resume_actions(payload.step_id), entry_id)
        return True

    def create_group(self, group: str, *, id: str = "0") -> None:
        _create_group(self.redis, self.name, group, id=id)

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
