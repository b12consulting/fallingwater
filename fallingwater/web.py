"""FastAPI router and small standalone app for conversation events."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import EventSourceResponse
from fastapi.sse import ServerSentEvent
from redis.asyncio import Redis

from .streams import StreamNames
from .utils import logger


router = APIRouter()


def get_redis(request: Request) -> Redis:
    """Get the Redis client configured by the hosting app."""
    return request.app.state.fw_redis


def get_names(request: Request) -> StreamNames:
    """Get the Redis key namespace configured by the hosting app."""
    return StreamNames(request.app.state.fw_namespace)


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
