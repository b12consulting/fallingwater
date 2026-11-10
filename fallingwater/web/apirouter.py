"""
FastAPI router and small standalone app for conversation events.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import EventSourceResponse, HTMLResponse
from fastapi.sse import ServerSentEvent
from fastapi.templating import Jinja2Templates
from redis import Redis

from ..models import (
    ConversationEvent,
    Action,
    Event,
)
from ..stream import ConversationStream, DispatchStream
from ..stream_utils import (
    decode_conversation_event,
    decode_dispatch_fields,
    decode_text,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from ..utils import logger
from .dashboard import (
    DashboardContext,
    HistoryContext,
    dashboard_card,
    event_details,
    event_time,
    history_context,
)


router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
RECENT_CONVERSATION_LIMIT = 10
RECENT_EVENT_LIMIT = 10


@dataclass(frozen=True)
class DashboardCutoff:
    """Normalized dashboard cutoff and stream-specific read bounds."""

    entry_id: str = ""
    time: datetime | None = None
    event_before: str = "+"

    @property
    def dispatch_before(self) -> str:
        return f"({self.entry_id}" if self.entry_id else "+"

    @classmethod
    def parse(cls, entry_id: str) -> "DashboardCutoff":
        if not entry_id:
            return cls()
        milliseconds, sequence = map(int, entry_id.split("-"))
        if sequence > 2**64 - 1:
            raise ValueError("Redis sequence is out of range")
        time = event_time(entry_id)
        # Sequences are stream-local; event timelines use the cutoff's time.
        event_before = (
            f"({milliseconds}-0" if sequence == 0 else str(milliseconds)
        )
        return cls(f"{milliseconds}-{sequence}", time, event_before)


def get_redis(request: Request) -> Redis:
    """
    Get the Redis client configured by the hosting app.
    """
    return request.app.state.fw_redis


def get_namespace(request: Request) -> str:
    """
    Get the Redis key namespace configured by the hosting app.
    """
    return request.app.state.fw_namespace


def get_dashboard_before(
    before: Annotated[
        str, Query(pattern=r"^(?:[0-9]+-[0-9]+)?$", max_length=41)
    ] = "",
) -> DashboardCutoff:
    try:
        return DashboardCutoff.parse(before)
    except (ValueError, OverflowError, OSError) as error:
        logger.debug("Invalid dashboard cutoff %s", before, exc_info=True)
        raise HTTPException(
            status_code=422, detail="Use a valid Redis entry ID"
        ) from error


@router.get("/monitor", response_class=HTMLResponse)
def monitor(request: Request, full: bool = False) -> HTMLResponse:
    """
    Show recent conversation events in a small read-only page.
    """
    events_url = request.url_for("recent_events")
    if full:
        events_url = events_url.include_query_params(full="1")
    return templates.TemplateResponse(
        request=request,
        name="monitor.html",
        context={
            "events_url": str(events_url),
            "event_types": [event.value for event in Event],
        },
    )


def get_conversation_stream(
    conversation_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
) -> ConversationStream:
    try:
        stream = DispatchStream(redis, namespace=namespace).conversation_stream(
            conversation_id
        )
    except ValueError as error:
        logger.debug(
            "Invalid conversation ID %s", conversation_id, exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(error)) from error
    if not redis.exists(stream.name):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return stream


def _recent_streams(
    redis: Redis, namespace: str, *, before: str = "+"
) -> tuple[str, dict[str, str]]:
    """
    Find the latest dispatch ID and recently poked conversations.
    """
    dispatch_cursor, latest_pokes = _discover_conversations(
        redis, namespace, before=before
    )
    return dispatch_cursor, {
        fmt_conversation_stream(namespace, conversation_id): "0-0"
        for conversation_id in reversed(latest_pokes)
    }


def _discover_conversations(
    redis: Redis,
    namespace: str,
    *,
    before: str = "+",
) -> tuple[str, dict[str, str]]:
    dispatch_cursor = "0-0"
    latest_pokes: dict[str, str] = {}
    dispatch_name = fmt_dispatch_stream(namespace)
    while len(latest_pokes) < RECENT_CONVERSATION_LIMIT:
        entries = redis.xrevrange(dispatch_name, max=before, min="-", count=100)
        if not entries:
            break
        if dispatch_cursor == "0-0":
            dispatch_cursor = decode_text(entries[0][0])
        for entry_id, fields in entries:
            fields = decode_dispatch_fields(fields)
            if fields.get("type") != Action.CONVERSATION_POKE:
                continue
            conversation_id = fields.get("conversation_id")
            if conversation_id and conversation_id not in latest_pokes:
                latest_pokes[conversation_id] = decode_text(entry_id)
                if len(latest_pokes) == RECENT_CONVERSATION_LIMIT:
                    break
        before = f"({decode_text(entries[-1][0])}"

    return dispatch_cursor, latest_pokes


def _recent_tail(
    stream: ConversationStream, *, before: str = "+"
) -> tuple[str, list[ConversationEvent]]:
    """
    Return up to ten retained events in stream order and their latest ID.
    """
    decoded = stream.read_conversation_reverse_range(
        max=before, min="-", count=RECENT_EVENT_LIMIT
    )
    return (decoded[0].entry_id if decoded else "0-0"), list(reversed(decoded))


def _event_data(event: ConversationEvent, *, full: bool) -> dict:
    return {
        "type": event.type.value,
        "payload": event.payload.fmt(full=full),
    }


def _recent_sse(
    namespace: str,
    stream: str,
    event: ConversationEvent,
    *,
    full: bool,
) -> ServerSentEvent:
    conversation_id = _conversation_id(namespace, stream)
    return ServerSentEvent(
        id=f"{conversation_id}:{event.entry_id}",
        event=event.type.value,
        data={
            **_event_data(event, full=full),
            "conversation_id": conversation_id,
        },
    )


def _conversation_id(namespace: str, stream: str) -> str:
    return stream.removeprefix(f"{namespace}:conversation:").removesuffix(
        ":events"
    )


@router.get("/conversations/recent/events", response_class=EventSourceResponse)
def recent_events(
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
    full: bool = False,
) -> Iterator[ServerSentEvent]:
    """
    Follow streams for the ten most recently dispatched conversations.
    """
    dispatch = DispatchStream(redis, namespace=namespace)
    dispatch_name = dispatch.name
    dispatch_cursor, streams = _recent_streams(redis, namespace)

    for stream in streams:
        conversation = dispatch.conversation_stream(
            _conversation_id(namespace, stream)
        )
        streams[stream], entries = _recent_tail(conversation)
        for event in entries:
            yield _recent_sse(namespace, stream, event, full=full)

    while True:
        emitted_event = False
        new_streams: dict[str, None] = {}
        batches = redis.xread(
            {dispatch_name: dispatch_cursor, **streams}, count=100, block=1000
        )
        for raw_stream, entries in batches:
            stream = decode_text(raw_stream)
            if stream != dispatch_name:
                continue
            for entry_id, fields in entries:
                dispatch_cursor = decode_text(entry_id)
                fields = decode_dispatch_fields(fields)
                if fields.get("type") != Action.CONVERSATION_POKE:
                    continue
                conversation_id = fields.get("conversation_id")
                if not conversation_id:
                    continue
                conversation_name = fmt_conversation_stream(
                    namespace, conversation_id
                )
                if conversation_name in streams:
                    cursor = streams.pop(conversation_name)
                else:
                    cursor = "0-0"
                    if len(streams) == RECENT_CONVERSATION_LIMIT:
                        streams.pop(next(iter(streams)))
                    new_streams[conversation_name] = None
                streams[conversation_name] = cursor

        for raw_stream, entries in batches:
            stream = decode_text(raw_stream)
            if stream == dispatch_name or stream not in streams:
                continue
            for entry_id, fields in entries:
                event = decode_conversation_event(entry_id, fields)
                streams[stream] = event.entry_id
                emitted_event = True
                yield _recent_sse(namespace, stream, event, full=full)

        for stream in new_streams:
            if stream not in streams or streams[stream] != "0-0":
                continue
            conversation = dispatch.conversation_stream(
                _conversation_id(namespace, stream)
            )
            streams[stream], entries = _recent_tail(conversation)
            for event in entries:
                emitted_event = True
                yield _recent_sse(namespace, stream, event, full=full)

        if not emitted_event:
            yield ServerSentEvent(comment="keep-alive")


@router.get(
    "/conversations/{conversation_id}/events",
    response_class=EventSourceResponse,
)
def events(
    stream: Annotated[ConversationStream, Depends(get_conversation_stream)],
) -> Iterator[ServerSentEvent]:
    """
    Replay all conversation events, then follow new entries.
    """
    cursor = "0-0"
    while True:
        emitted_event = False
        for event in stream.read_conversation(cursor, count=100, block=1000):
            cursor = event.entry_id
            emitted_event = True
            yield ServerSentEvent(
                id=event.entry_id,
                event=event.type.value,
                data=_event_data(event, full=True),
            )
        if not emitted_event:
            yield ServerSentEvent(comment="keep-alive")


def _dashboard_context(
    request: Request,
    redis: Redis,
    namespace: str,
    cutoff: DashboardCutoff,
) -> DashboardContext:
    _, latest_pokes = _discover_conversations(
        redis, namespace, before=cutoff.dispatch_before
    )
    dispatch = DispatchStream(redis, namespace=namespace)
    conversations = []
    for conversation_id in latest_pokes:
        stream = dispatch.conversation_stream(conversation_id)
        _, events = _recent_tail(stream, before=cutoff.event_before)
        first = stream.read_conversation_range(
            min="-", max=cutoff.event_before, count=1
        )
        conversations.append(
            dashboard_card(
                request,
                conversation_id,
                events,
                first[0] if first else None,
            )
        )
    return {
        "conversations": conversations,
        "event_limit": RECENT_EVENT_LIMIT,
        "before": cutoff.entry_id,
        "cutoff_time": cutoff.time,
        "selected_date": cutoff.time.date().isoformat() if cutoff.time else "",
        "prev_id": next(reversed(latest_pokes.values()), ""),
    }


def _history_context(
    request: Request, stream: ConversationStream, conversation_id: str
) -> HistoryContext:
    events = stream.read_conversation_range(min="-", max="+", count=None)
    return history_context(request, conversation_id, events)


@router.get("/dashboard/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
    before: Annotated[DashboardCutoff, Depends(get_dashboard_before)],
) -> HTMLResponse:
    """Show recent conversations as cards with clickable event timelines."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=_dashboard_context(request, redis, namespace, before),
    )


@router.get("/dashboard-cards", response_class=HTMLResponse)
def dashboard_cards(
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
    before: Annotated[DashboardCutoff, Depends(get_dashboard_before)],
) -> HTMLResponse:
    """Refresh the same bounded recent set used by the monitor feed."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_cards.html",
        context=_dashboard_context(request, redis, namespace, before),
    )


@router.get("/dashboard-event", response_class=HTMLResponse)
def dashboard_event(
    request: Request,
    conversation_id: str,
    entry_id: Annotated[str, Query(pattern=r"^[0-9]+-[0-9]+$")],
    stream: Annotated[ConversationStream, Depends(get_conversation_stream)],
) -> HTMLResponse:
    """Render the exact selected event, including its complete payload."""
    entries = stream.read_conversation_range(
        min=entry_id, max=entry_id, count=1
    )
    if not entries:
        raise HTTPException(status_code=404, detail="Event not found")
    event = entries[0]
    return templates.TemplateResponse(
        request=request,
        name="dashboard_event.html",
        context=event_details(conversation_id, event),
    )


@router.get("/dashboard-history", response_class=HTMLResponse)
def dashboard_history(
    request: Request,
    conversation_id: str,
    stream: Annotated[ConversationStream, Depends(get_conversation_stream)],
) -> HTMLResponse:
    """Refresh all retained events without replacing the selected details."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_card.html",
        context=_history_context(request, stream, conversation_id),
    )


@router.get("/dashboard/{conversation_id:path}", response_class=HTMLResponse)
def conversation_dashboard(
    request: Request,
    conversation_id: str,
    stream: Annotated[ConversationStream, Depends(get_conversation_stream)],
) -> HTMLResponse:
    """Show full conversation history beside the selected event details."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_conversation.html",
        context=_history_context(request, stream, conversation_id),
    )


def create_app(
    *,
    redis: Redis,
    namespace: str = "fw",
) -> FastAPI:
    """
    Mount the router with its Redis client and namespace.
    """
    app = FastAPI()
    app.state.fw_redis = redis
    app.state.fw_namespace = namespace
    app.include_router(router, prefix="/fw-api")

    return app
