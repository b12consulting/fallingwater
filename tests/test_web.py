"""Checks for the optional FastAPI conversation event feed."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("fastapi.sse")

from fastapi.testclient import TestClient
from fastapi.sse import ServerSentEvent
from redis.asyncio import Redis

from fallingwater.streams import EventType
from fallingwater.web import create_app, events, router


def test_app_mounts_conversation_events_under_fw_api() -> None:
    redis = MagicMock(spec=Redis)

    app = create_app(redis=redis, namespace="fw-test")

    assert "/fw-api/conversations/{conversation_id}/events" in app.openapi()["paths"]
    assert app.state.fw_redis is redis
    assert app.state.fw_namespace == "fw-test"
    assert router.routes[0].endpoint is events


def test_sse_replays_events_then_reads_the_tail() -> None:
    async def check() -> None:
        redis = MagicMock(spec=Redis)
        redis.xread = AsyncMock(
            side_effect=[
                [
                    (
                        "fw-test:conversation:abc:events",
                        [
                            (
                                "1-0",
                                {
                                    "type": EventType.CONVERSATION_CREATED,
                                    "agent": "demo",
                                },
                            ),
                            ("2-0", {"type": EventType.TURN_QUEUED, "prompt": "hello"}),
                        ],
                    )
                ],
                [
                    (
                        "fw-test:conversation:abc:events",
                        [
                            (
                                "3-0",
                                {"type": EventType.TURN_STARTED, "turn_id": "turn-1"},
                            )
                        ],
                    )
                ],
            ]
        )
        stream = events("fw-test:conversation:abc:events", redis)

        first = await anext(stream)
        second = await anext(stream)
        third = await anext(stream)
        await stream.aclose()

        assert isinstance(first, ServerSentEvent)
        assert (first.id, first.event, first.data["agent"]) == (
            "1-0",
            EventType.CONVERSATION_CREATED,
            "demo",
        )
        assert (second.id, second.event) == ("2-0", EventType.TURN_QUEUED)
        assert (third.id, third.event) == ("3-0", EventType.TURN_STARTED)
        assert redis.xread.await_args_list[0].args[0] == {
            "fw-test:conversation:abc:events": "0-0"
        }
        assert redis.xread.await_args_list[1].args[0] == {
            "fw-test:conversation:abc:events": "2-0"
        }

    asyncio.run(check())


def test_unknown_conversation_returns_404_before_streaming() -> None:
    redis = MagicMock(spec=Redis)
    redis.exists = AsyncMock(return_value=0)
    app = create_app(redis=redis, namespace="fw-test")

    with TestClient(app) as client:
        response = client.get("/fw-api/conversations/missing/events")

    assert response.status_code == 404
    redis.exists.assert_awaited_once_with("fw-test:conversation:missing:events")
