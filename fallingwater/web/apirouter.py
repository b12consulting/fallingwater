"""
FastAPI router and small standalone app for conversation events.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import EventSourceResponse, HTMLResponse
from fastapi.sse import ServerSentEvent
from fastapi.templating import Jinja2Templates
from redis import Redis

from ..models import (
    ConversationEvent,
    ConversationCreatedPayload,
    Action,
    Event,
)
from ..stream_utils import (
    decode_conversation_event,
    decode_dispatch_fields,
    decode_text,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from ..utils import logger
from ..stream import DispatchStream


router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
RECENT_CONVERSATION_LIMIT = 10
RECENT_EVENT_LIMIT = 10
EVENT_LABELS = {
    Event.CONVERSATION_CREATED: "Conversation created",
    Event.TURN_QUEUED: "User message",
    Event.TURN_TOOL_REQUESTED: "Tools requested",
    Event.TURN_TOOL_RESULT: "Tool result",
    Event.TURN_SUBAGENT_REQUESTED: "Subagent requested",
    Event.TURN_COMPLETED: "Assistant message",
    Event.TURN_FAILED: "Turn failed",
}
EVENT_ICONS = {
    Event.CONVERSATION_CREATED: "file-check",
    Event.TURN_QUEUED: "person",
    Event.TURN_COMPLETED: "robot",
    Event.TURN_SUBAGENT_REQUESTED: "arrow-bar-down",
    Event.TURN_TOOL_REQUESTED: "nut",
    Event.TURN_TOOL_RESULT: "nut-fill",
    Event.TURN_FAILED: "exclamation-square",
}


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
) -> str:
    try:
        stream = fmt_conversation_stream(namespace, conversation_id)
    except ValueError as error:
        logger.debug(
            "Invalid conversation ID %s", conversation_id, exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(error)) from error
    if not redis.exists(stream):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return stream


def _recent_streams(redis: Redis, namespace: str) -> tuple[str, dict[str, str]]:
    """
    Find the latest dispatch ID and recently poked conversations.
    """
    dispatch_cursor = "0-0"
    before = "+"
    recent_ids: dict[str, None] = {}
    dispatch_name = fmt_dispatch_stream(namespace)
    while len(recent_ids) < RECENT_CONVERSATION_LIMIT:
        entries = redis.xrevrange(dispatch_name, max=before, min="-", count=100)
        if not entries:
            break
        if dispatch_cursor == "0-0":
            dispatch_cursor = decode_text(entries[0][0])
        for _, fields in entries:
            fields = decode_dispatch_fields(fields)
            if fields.get("type") != Action.CONVERSATION_POKE:
                continue
            conversation_id = fields.get("conversation_id")
            if conversation_id and conversation_id not in recent_ids:
                recent_ids[conversation_id] = None
                if len(recent_ids) == RECENT_CONVERSATION_LIMIT:
                    break
        before = f"({decode_text(entries[-1][0])}"

    streams = {}
    for conversation_id in reversed(recent_ids):
        streams[fmt_conversation_stream(namespace, conversation_id)] = "0-0"
    return dispatch_cursor, streams


def _recent_tail(
    redis: Redis, stream: str
) -> tuple[str, list[ConversationEvent]]:
    """
    Return up to ten retained events in stream order and their latest ID.
    """
    entries = redis.xrevrange(
        stream, max="+", min="-", count=RECENT_EVENT_LIMIT
    )
    decoded = [
        decode_conversation_event(entry_id, fields)
        for entry_id, fields in entries
    ]
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
    conversation_id = stream.removeprefix(
        f"{namespace}:conversation:"
    ).removesuffix(":events")
    return ServerSentEvent(
        id=f"{conversation_id}:{event.entry_id}",
        event=event.type.value,
        data={
            **_event_data(event, full=full),
            "conversation_id": conversation_id,
        },
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
    dispatch_name = fmt_dispatch_stream(namespace)
    dispatch_cursor, streams = _recent_streams(redis, namespace)

    for stream in streams:
        streams[stream], entries = _recent_tail(redis, stream)
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
            streams[stream], entries = _recent_tail(redis, stream)
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
    stream: Annotated[str, Depends(get_conversation_stream)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> Iterator[ServerSentEvent]:
    """
    Replay all conversation events, then follow new entries.
    """
    cursor = "0-0"
    while True:
        emitted_event = False
        batches = redis.xread({stream: cursor}, count=100, block=1000)
        for _, entries in batches:
            for entry_id, fields in entries:
                event = decode_conversation_event(entry_id, fields)
                cursor = event.entry_id
                emitted_event = True
                yield ServerSentEvent(
                    id=event.entry_id,
                    event=event.type.value,
                    data=_event_data(event, full=True),
                )
        if not emitted_event:
            yield ServerSentEvent(comment="keep-alive")


def _event_time(entry_id: str) -> datetime:
    return datetime.fromtimestamp(
        int(entry_id.split("-", 1)[0]) / 1000, tz=timezone.utc
    )


def _dashboard_context(request: Request, redis: Redis, namespace: str) -> dict:
    _, streams = _recent_streams(redis, namespace)
    dispatch = DispatchStream(redis, namespace=namespace)
    conversations = []
    for stream in reversed(streams):
        conversation_id = stream.removeprefix(
            f"{namespace}:conversation:"
        ).removesuffix(":events")
        _, events = _recent_tail(redis, stream)
        first = dispatch.conversation_stream(
            conversation_id
        ).read_conversation_range(count=1)
        conversations.append(
            _dashboard_card(
                request, conversation_id, events, first[0] if first else None
            )
        )
    return {"conversations": conversations, "event_limit": RECENT_EVENT_LIMIT}


def _dashboard_card(
    request: Request,
    conversation_id: str,
    events: list[ConversationEvent],
    first: ConversationEvent | None,
) -> dict:
    return {
        "conversation_id": conversation_id,
        "url": request.url_for(
            "conversation_dashboard",
            conversation_id=quote(conversation_id, safe=""),
        ),
        "id_key": sha256(conversation_id.encode()).hexdigest(),
        "settings": (
            first.payload
            if first and isinstance(first.payload, ConversationCreatedPayload)
            else None
        ),
        "failed_count": sum(
            event.type == Event.TURN_FAILED for event in events
        ),
        "events": [
            {
                "entry_id": event.entry_id,
                "label": EVENT_LABELS[event.type],
                "icon": EVENT_ICONS.get(event.type),
                "failed": event.type == Event.TURN_FAILED,
                "time": _event_time(event.entry_id),
                "details_url": request.url_for(
                    "dashboard_event"
                ).include_query_params(
                    conversation_id=conversation_id,
                    entry_id=event.entry_id,
                ),
            }
            for event in events
        ],
    }


def _history_context(
    request: Request, redis: Redis, stream: str, conversation_id: str
) -> dict:
    events = [
        decode_conversation_event(entry_id, fields)
        for entry_id, fields in redis.xrange(stream, min="-", max="+")
    ]
    return {
        "conversation": _dashboard_card(
            request, conversation_id, events, events[0] if events else None
        ),
        "full_history": True,
        "cards_target": "conversation-history",
        "refresh_label": "Refresh history",
        "events_url": request.url_for(
            "events", conversation_id=quote(conversation_id, safe="")
        ),
    }


@router.get("/dashboard/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
) -> HTMLResponse:
    """Show recent conversations as cards with clickable event timelines."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context=_dashboard_context(request, redis, namespace),
    )


@router.get("/dashboard-cards", response_class=HTMLResponse)
def dashboard_cards(
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    namespace: Annotated[str, Depends(get_namespace)],
) -> HTMLResponse:
    """Refresh the same bounded recent set used by the monitor feed."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_cards.html",
        context=_dashboard_context(request, redis, namespace),
    )


@router.get("/dashboard-event", response_class=HTMLResponse)
def dashboard_event(
    request: Request,
    conversation_id: str,
    entry_id: Annotated[str, Query(pattern=r"^[0-9]+-[0-9]+$")],
    stream: Annotated[str, Depends(get_conversation_stream)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> HTMLResponse:
    """Render the exact selected event, including its complete payload."""
    entries = redis.xrange(stream, min=entry_id, max=entry_id, count=1)
    if not entries:
        raise HTTPException(status_code=404, detail="Event not found")
    event = decode_conversation_event(*entries[0])
    return templates.TemplateResponse(
        request=request,
        name="dashboard_event.html",
        context={
            "conversation_id": conversation_id,
            "event": event,
            "label": EVENT_LABELS[event.type],
            "time": _event_time(event.entry_id),
            "failed": event.type == Event.TURN_FAILED,
            "payload": event.payload.fmt(full=True),
        },
    )


@router.get("/dashboard-history", response_class=HTMLResponse)
def dashboard_history(
    request: Request,
    conversation_id: str,
    stream: Annotated[str, Depends(get_conversation_stream)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> HTMLResponse:
    """Refresh all retained events without replacing the selected details."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_card.html",
        context=_history_context(request, redis, stream, conversation_id),
    )


@router.get("/dashboard/{conversation_id:path}", response_class=HTMLResponse)
def conversation_dashboard(
    request: Request,
    conversation_id: str,
    stream: Annotated[str, Depends(get_conversation_stream)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> HTMLResponse:
    """Show full conversation history beside the selected event details."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard_conversation.html",
        context=_history_context(request, redis, stream, conversation_id),
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
