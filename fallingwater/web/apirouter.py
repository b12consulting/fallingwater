"""FastAPI router and small standalone app for conversation events."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import EventSourceResponse, HTMLResponse
from fastapi.sse import ServerSentEvent
from fastapi.templating import Jinja2Templates
from redis.asyncio import Redis

from ..streams import EventType, StreamNames
from ..utils import logger


router = APIRouter()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
RECENT_CONVERSATION_LIMIT = 10
RECENT_EVENT_LIMIT = 10


def get_redis(request: Request) -> Redis:
    """Get the Redis client configured by the hosting app."""
    return request.app.state.fw_redis


def get_names(request: Request) -> StreamNames:
    """Get the Redis key namespace configured by the hosting app."""
    return StreamNames(request.app.state.fw_namespace)


@router.get("/monitor", response_class=HTMLResponse)
def monitor(request: Request) -> HTMLResponse:
    """Show recent conversation events in a small read-only page."""
    return templates.TemplateResponse(
        request=request,
        name="monitor.html",
        context={
            "events_url": str(request.url_for("recent_events")),
            "event_types": [
                event.value
                for event in EventType
                if event != EventType.CONVERSATION_POKE
            ],
        },
    )


async def conversation_stream(
    conversation_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    names: Annotated[StreamNames, Depends(get_names)],
) -> str:
    try:
        stream = names.conversation(conversation_id)
    except ValueError as error:
        logger.debug("Invalid conversation ID %s", conversation_id, exc_info=True)
        raise HTTPException(status_code=400, detail=str(error)) from error
    if not await redis.exists(stream):
        raise HTTPException(status_code=404, detail="Conversation not found")
    return stream


async def _recent_streams(
    redis: Redis, names: StreamNames
) -> tuple[str, dict[str, str]]:
    """Find the latest dispatch ID and ten distinct recently poked conversations."""
    dispatch_cursor = "0-0"
    before = "+"
    recent_ids: dict[str, None] = {}
    while len(recent_ids) < RECENT_CONVERSATION_LIMIT:
        entries = await redis.xrevrange(names.dispatch, max=before, min="-", count=100)
        if not entries:
            break
        if dispatch_cursor == "0-0":
            dispatch_cursor = entries[0][0]
        for _, fields in entries:
            if fields.get("type") != EventType.CONVERSATION_POKE:
                continue
            conversation_id = fields.get("conversation_id")
            if conversation_id and conversation_id not in recent_ids:
                recent_ids[conversation_id] = None
                if len(recent_ids) == RECENT_CONVERSATION_LIMIT:
                    break
        before = f"({entries[-1][0]}"

    streams = {
        names.conversation(conversation_id): "0-0"
        for conversation_id in reversed(recent_ids)
    }
    return dispatch_cursor, streams


async def _recent_tail(
    redis: Redis, stream: str
) -> tuple[str, list[tuple[str, dict[str, str]]]]:
    """Return up to ten retained events in stream order and their latest ID."""
    entries = await redis.xrevrange(stream, max="+", min="-", count=RECENT_EVENT_LIMIT)
    return (entries[0][0] if entries else "0-0"), list(reversed(entries))


def _recent_sse(
    names: StreamNames, stream: str, entry_id: str, fields: dict[str, str]
) -> ServerSentEvent:
    conversation_id = stream.removeprefix(
        f"{names.namespace}:conversation:"
    ).removesuffix(":events")
    return ServerSentEvent(
        id=f"{conversation_id}:{entry_id}",
        event=fields.get("type"),
        data={**fields, "conversation_id": conversation_id},
    )


@router.get("/conversations/recent/events", response_class=EventSourceResponse)
async def recent_events(
    redis: Annotated[Redis, Depends(get_redis)],
    names: Annotated[StreamNames, Depends(get_names)],
) -> AsyncIterator[ServerSentEvent]:
    """Follow streams for the ten most recently dispatched conversations."""
    dispatch_cursor, streams = await _recent_streams(redis, names)

    for stream in streams:
        streams[stream], entries = await _recent_tail(redis, stream)
        for entry_id, fields in entries:
            yield _recent_sse(names, stream, entry_id, fields)

    while True:
        new_streams: dict[str, None] = {}
        batches = await redis.xread(
            {names.dispatch: dispatch_cursor, **streams}, count=100, block=1000
        )
        for stream, entries in batches:
            if stream != names.dispatch:
                continue
            for entry_id, fields in entries:
                dispatch_cursor = entry_id
                if fields.get("type") != EventType.CONVERSATION_POKE:
                    continue
                conversation_id = fields.get("conversation_id")
                if not conversation_id:
                    continue
                conversation_stream = names.conversation(conversation_id)
                if conversation_stream in streams:
                    cursor = streams.pop(conversation_stream)
                else:
                    cursor = "0-0"
                    if len(streams) == RECENT_CONVERSATION_LIMIT:
                        streams.pop(next(iter(streams)))
                    new_streams[conversation_stream] = None
                streams[conversation_stream] = cursor

        for stream, entries in batches:
            if stream == names.dispatch or stream not in streams:
                continue
            for entry_id, fields in entries:
                streams[stream] = entry_id
                yield _recent_sse(names, stream, entry_id, fields)

        for stream in new_streams:
            if stream not in streams or streams[stream] != "0-0":
                continue
            streams[stream], entries = await _recent_tail(redis, stream)
            for entry_id, fields in entries:
                yield _recent_sse(names, stream, entry_id, fields)


@router.get(
    "/conversations/{conversation_id}/events",
    response_class=EventSourceResponse,
)
async def events(
    stream: Annotated[str, Depends(conversation_stream)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> AsyncIterator[ServerSentEvent]:
    """Replay all conversation events, then follow new entries."""
    cursor = "0-0"
    while True:
        batches = await redis.xread({stream: cursor}, count=100, block=1000)
        for _, entries in batches:
            for entry_id, fields in entries:
                cursor = entry_id
                yield ServerSentEvent(
                    id=entry_id, event=fields.get("type"), data=fields
                )


def create_app(
    *,
    redis_url: str = "redis://localhost:6379/0",
    namespace: str = "fw",
    redis: Redis | None = None,
) -> FastAPI:
    """Mount the router for the bundled web command."""
    owns_redis = redis is None
    client = (
        redis if redis is not None else Redis.from_url(redis_url, decode_responses=True)
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        if owns_redis:
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.fw_redis = client
    app.state.fw_namespace = namespace
    app.include_router(router, prefix="/fw-api")
    return app
