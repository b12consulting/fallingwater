"""Checks for the optional FastAPI conversation event feed."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("fastapi.sse")
pytest.importorskip("jinja2")

from fastapi.testclient import TestClient
from fastapi.sse import ServerSentEvent
from redis.asyncio import Redis

from fallingwater.streams import EventType, StreamNames
from fallingwater.web.apirouter import (
    _recent_streams,
    create_app,
    events,
    monitor,
    recent_events,
    router,
)


def test_app_mounts_conversation_events_under_fw_api() -> None:
    redis = MagicMock(spec=Redis)

    app = create_app(redis=redis, namespace="fw-test")

    assert "/fw-api/monitor" in app.openapi()["paths"]
    assert "/fw-api/conversations/recent/events" in app.openapi()["paths"]
    assert "/fw-api/conversations/{conversation_id}/events" in app.openapi()["paths"]
    assert app.state.fw_redis is redis
    assert app.state.fw_namespace == "fw-test"
    assert router.routes[0].endpoint is monitor
    assert router.routes[1].endpoint is recent_events
    assert router.routes[2].endpoint is events


def test_monitor_page_subscribes_to_recent_events() -> None:
    app = create_app(redis=MagicMock(spec=Redis))

    with TestClient(app) as client:
        response = client.get("/fw-api/monitor")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert (
        'new EventSource("http://testserver/fw-api/conversations/recent/events")'
        in response.text
    )
    assert '"turn.completed"' in response.text


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


def test_recent_conversation_discovery_skips_duplicate_dispatch_entries() -> None:
    async def check() -> None:
        redis = MagicMock(spec=Redis)
        redis.xrevrange = AsyncMock(
            side_effect=[
                [
                    (
                        f"{number}-0",
                        {
                            "type": EventType.CONVERSATION_POKE,
                            "conversation_id": "same",
                        },
                    )
                    for number in range(200, 100, -1)
                ],
                [
                    (
                        f"{number}-0",
                        {
                            "type": EventType.CONVERSATION_POKE,
                            "conversation_id": f"c{index}",
                        },
                    )
                    for index, number in enumerate(range(100, 91, -1), start=1)
                ],
            ]
        )

        cursor, streams = await _recent_streams(redis, StreamNames("fw-test"))

        assert cursor == "200-0"
        assert list(streams) == [
            f"fw-test:conversation:{conversation_id}:events"
            for conversation_id in [
                "c9",
                "c8",
                "c7",
                "c6",
                "c5",
                "c4",
                "c3",
                "c2",
                "c1",
                "same",
            ]
        ]
        assert redis.xrevrange.await_args_list[1].kwargs["max"] == "(101-0"

    asyncio.run(check())


def test_recent_feed_replays_only_ten_events_then_follows_the_tail() -> None:
    async def check() -> None:
        redis = MagicMock(spec=Redis)
        names = StreamNames("fw-test")
        stream = names.conversation("abc")
        redis.xrevrange = AsyncMock(
            side_effect=[
                [
                    (
                        "10-0",
                        {
                            "type": EventType.CONVERSATION_POKE,
                            "conversation_id": "abc",
                        },
                    )
                ],
                [],
                [
                    (
                        f"{number}-0",
                        {"type": EventType.TURN_COMPLETED, "output": str(number)},
                    )
                    for number in range(12, 2, -1)
                ],
            ]
        )
        redis.xread = AsyncMock(
            return_value=[(stream, [("13-0", {"type": EventType.TURN_STARTED})])]
        )
        feed = recent_events(redis, names)

        received = [await anext(feed) for _ in range(11)]
        await feed.aclose()

        assert [event.id for event in received] == [
            f"abc:{number}-0" for number in range(3, 14)
        ]
        assert redis.xrevrange.await_args_list[2].args == (stream,)
        assert redis.xrevrange.await_args_list[2].kwargs["count"] == 10
        assert redis.xread.await_args.args[0][stream] == "12-0"

    asyncio.run(check())


def test_recent_feed_adds_new_conversations_and_evicts_oldest() -> None:
    async def check() -> None:
        redis = MagicMock(spec=Redis)
        names = StreamNames("fw-test")
        dispatch = names.dispatch

        async def xrevrange(
            stream: str, **_kwargs: object
        ) -> list[tuple[str, dict[str, str]]]:
            if stream == dispatch:
                return [
                    (
                        f"{number}-0",
                        {
                            "type": EventType.CONVERSATION_POKE,
                            "conversation_id": f"c{number}",
                        },
                    )
                    for number in range(10, 0, -1)
                ]
            if stream == names.conversation("c11"):
                return [
                    ("3-0", {"type": EventType.TURN_STARTED}),
                    ("2-0", {"type": EventType.TURN_QUEUED}),
                    ("1-0", {"type": EventType.CONVERSATION_CREATED}),
                ]
            return []

        redis.xrevrange = AsyncMock(side_effect=xrevrange)
        redis.xread = AsyncMock(
            side_effect=[
                [
                    (
                        names.conversation("c10"),
                        [("20-0", {"type": EventType.TURN_COMPLETED, "output": "old"})],
                    )
                ],
                [
                    (
                        dispatch,
                        [
                            (
                                "11-0",
                                {
                                    "type": EventType.CONVERSATION_POKE,
                                    "conversation_id": "c11",
                                },
                            )
                        ],
                    ),
                    (
                        names.conversation("c1"),
                        [
                            (
                                "21-0",
                                {"type": EventType.TURN_COMPLETED, "output": "evicted"},
                            )
                        ],
                    ),
                ],
                [
                    (
                        names.conversation("c11"),
                        [("4-0", {"type": EventType.TURN_COMPLETED})],
                    )
                ],
            ]
        )
        feed = recent_events(redis, names)

        received = [await anext(feed) for _ in range(5)]
        await feed.aclose()

        assert (
            received[0].id,
            received[0].event,
            received[0].data["conversation_id"],
        ) == (
            "c10:20-0",
            EventType.TURN_COMPLETED,
            "c10",
        )
        assert [event.id for event in received[1:]] == [
            "c11:1-0",
            "c11:2-0",
            "c11:3-0",
            "c11:4-0",
        ]
        assert redis.xread.await_args_list[0].args[0][dispatch] == "10-0"
        followed = redis.xread.await_args_list[2].args[0]
        assert names.conversation("c1") not in followed
        assert followed[names.conversation("c11")] == "3-0"

    asyncio.run(check())
