"""Redis stream access for Fallingwater conversations."""

from collections.abc import Callable, Iterator, Mapping
from typing import Any, TypeVar

from redis import Redis
from redis.exceptions import ResponseError

from .stream_models import (
    ConversationEvent,
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
    InvalidDispatchAction,
)
from .stream_utils import (
    decode_conversation_event,
    decode_dispatch_action,
    decode_text,
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from .utils import logger
from .turn_state import Conversation


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

    def append_event(self, action_type: Action, **fields: str) -> str:
        """Append one dispatch action and return its Redis entry ID."""
        entry_id = self.redis.xadd(
            self.name, {**fields, "type": action_type}
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
        queued_id = self.append_event(
            Event.TURN_QUEUED,
            TurnQueuedPayload(turn_id=turn_id, message=prompt),
        )
        self.dispatch.append_event(
            Action.CONVERSATION_POKE,
            turn_id=turn_id,
            conversation_id=self.conversation_id,
        )
        return queued_id

    def queue_tool_requests(
        self,
        payload: ToolRequestedPayload,
        call_ids: list[str],
    ) -> None:
        """Persist one deferred request set and fan out one dispatch per call."""
        if not call_ids:
            raise ValueError("deferred tool request set must contain a call")
        if len(call_ids) != len(set(call_ids)):
            raise ValueError("deferred tool call IDs must be unique")
        self.append_event(Event.TURN_TOOL_REQUESTED, payload)
        from pprint import pprint
        print("tool requested payload")
        pprint(payload.requests)
        for tool_call_id in call_ids:
            self.dispatch.append_event(
                Action.TOOL_CALL,
                conversation_id=self.conversation_id,
                turn_id=payload.turn_id,
                step_id=payload.step_id,
                tool_call_id=tool_call_id,
            )

    def queue_subagent(
        self,
        *,
        parent_request: SubAgentRequestedPayload,
        child_agent: str,
        child_model: str,
        child_deps: dict[str, Any],
        child_prompt: str,
    ) -> bool:
        """Record a child request and queue its first turn."""
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
        self.append_event(Event.TURN_SUBAGENT_REQUESTED, parent_request)
        self.dispatch.append_event(
            Action.CONVERSATION_POKE,
            conversation_id=parent_request.child_conversation_id,
            turn_id=parent_request.child_turn_id,
        )
        return True

    def save_tool_result(
        self,
        payload: ToolResultPayload,
        expected_call_ids: list[str],
        conversation: Conversation,
    ) -> bool:
        """Save a unique call result and queue one resume after the final result."""
        expected = set(expected_call_ids)
        if payload.tool_call_id not in expected:
            raise ValueError(
                "tool result is not part of the deferred request set"
            )

        if (
            conversation.current_turn_id != payload.turn_id
            or conversation.tool_step_id != payload.step_id
            or conversation.tool_requests is None
        ):
            raise ValueError("tool result has no matching request step")
        if conversation.completed or conversation.failed is not None:
            return False

        prior_results = set(conversation.tool_results)
        if payload.tool_call_id in prior_results:
            return False
        cursor = conversation.last_event_id
        for event in self.iter_events(after_id=cursor):
            cursor = event.entry_id
            if getattr(event.payload, "turn_id", None) != payload.turn_id:
                continue
            if event.type == Event.TURN_TOOL_RESULT:
                if event.payload.step_id == payload.step_id:
                    prior_results.add(event.payload.tool_call_id)
            elif event.type in (Event.TURN_COMPLETED, Event.TURN_FAILED):
                return False
            elif event.type == Event.TURN_TOOL_REQUESTED:
                return False

        if payload.tool_call_id in prior_results:
            return False

        self.append_event(Event.TURN_TOOL_RESULT, payload)
        prior_results.add(payload.tool_call_id)
        for event in self.iter_events(after_id=cursor):
            if (
                event.type == Event.TURN_TOOL_RESULT
                and event.payload.turn_id == payload.turn_id
                and event.payload.step_id == payload.step_id
            ):
                prior_results.add(event.payload.tool_call_id)
        # All requested results are available; resume the agent.
        if expected <= prior_results:
            self.dispatch.append_event(
                Action.AGENT_RESUME,
                conversation_id=self.conversation_id,
                turn_id=payload.turn_id,
                step_id=payload.step_id,
            )
        return True

    def finish_turn(
        self,
        event_type: Event,
        payload: TurnCompletedPayload | TurnFailedPayload,
        *,
        parent: ParentToolReference | None,
    ) -> bool:
        """Persist a terminal event and notify its parent."""
        terminal_id = self.append_event(event_type, payload)
        if parent is not None:
            self.dispatch.append_event(
                Action.SUBAGENT_RESULT,
                conversation_id=parent.conversation_id,
                turn_id=parent.turn_id,
                step_id=parent.step_id,
                tool_call_id=parent.tool_call_id,
                child_conversation_id=self.conversation_id,
                child_turn_id=payload.turn_id,
                child_terminal_event_id=terminal_id,
            )
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
