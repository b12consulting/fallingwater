"""
FastAPI router and small standalone app for conversation events.
"""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import EventSourceResponse, HTMLResponse
from fastapi.sse import ServerSentEvent
from fastapi.templating import Jinja2Templates
from redis import Redis

from ..streams import (
    ConversationEvent,
    ConversationStream,
    DispatchStream,
    EventType,
    decode_text,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from ..utils import logger


router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
RECENT_CONVERSATION_LIMIT = 10
RECENT_EVENT_LIMIT = 10


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
            "event_types": [
                event.value
                for event in EventType
                if event != EventType.CONVERSATION_POKE
            ],
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
        logger.debug("Invalid conversation ID %s", conversation_id, exc_info=True)
        raise HTTPException(status_code=400, detail=str(error)) from error
    if not redis.exists(stream):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return stream


def _recent_streams(
    redis: Redis, namespace: str
) -> tuple[str, dict[str, str]]:
    """
    Find the latest dispatch ID and recently poked conversations.
    """
    dispatch_cursor = "0-0"
    before = "+"
    recent_ids: dict[str, None] = {}
    dispatch_name = fmt_dispatch_stream(namespace)
    while len(recent_ids) < RECENT_CONVERSATION_LIMIT:
        entries = redis.xrevrange(
            dispatch_name, max=before, min="-", count=100
        )
        if not entries:
            break
        if dispatch_cursor == "0-0":
            dispatch_cursor = decode_text(entries[0][0])
        for _, fields in entries:
            fields = DispatchStream.decode_event(fields)
            if fields.get("type") != EventType.CONVERSATION_POKE:
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
) -> tuple[str, list[tuple[str, ConversationEvent]]]:
    """
    Return up to ten retained events in stream order and their latest ID.
    """
    entries = redis.xrevrange(
        stream, max="+", min="-", count=RECENT_EVENT_LIMIT
    )
    decoded = [ConversationStream.decode_entry(entry) for entry in entries]
    return (decoded[0][0] if decoded else "0-0"), list(reversed(decoded))


def _event_data(event: ConversationEvent, *, full: bool) -> dict:
    return {
        "type": event.type.value,
        "payload": event.payload.fmt(full=full),
    }


def _recent_sse(
    namespace: str,
    stream: str,
    entry_id: str,
    event: ConversationEvent,
    *,
    full: bool,
) -> ServerSentEvent:
    conversation_id = stream.removeprefix(
        f"{namespace}:conversation:"
    ).removesuffix(":events")
    return ServerSentEvent(
        id=f"{conversation_id}:{entry_id}",
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
        for entry_id, fields in entries:
            yield _recent_sse(
                namespace, stream, entry_id, fields, full=full
            )

    while True:
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
                fields = DispatchStream.decode_event(fields)
                if fields.get("type") != EventType.CONVERSATION_POKE:
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
            for raw_entry in entries:
                entry_id, event = ConversationStream.decode_entry(raw_entry)
                streams[stream] = entry_id
                yield _recent_sse(namespace, stream, entry_id, event, full=full)

        for stream in new_streams:
            if stream not in streams or streams[stream] != "0-0":
                continue
            streams[stream], entries = _recent_tail(redis, stream)
            for entry_id, event in entries:
                yield _recent_sse(namespace, stream, entry_id, event, full=full)


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
        batches = redis.xread({stream: cursor}, count=100, block=1000)
        for _, entries in batches:
            for raw_entry in entries:
                entry_id, event = ConversationStream.decode_entry(raw_entry)
                cursor = entry_id
                yield ServerSentEvent(
                    id=entry_id,
                    event=event.type.value,
                    data=_event_data(event, full=True),
                )


def create_app(
    *,
    redis_url: str = "redis://localhost:6379/0",
    namespace: str = "fw",
    redis: Redis | None = None,
) -> FastAPI:
    """
    Mount the router for the bundled web command.
    """
    owns_redis = redis is None
    client = (
        redis if redis is not None else Redis.from_url(redis_url)
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_redis:
                client.close()

    app = FastAPI(lifespan=lifespan)
    app.state.fw_redis = client
    app.state.fw_namespace = namespace
    app.include_router(router, prefix="/fw-api")
    return app
