"""Checks for the optional FastAPI conversation event feed."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("fastapi.sse")
pytest.importorskip("jinja2")

from fastapi.testclient import TestClient
from fastapi.sse import ServerSentEvent
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.models import (
    ConversationCreatedPayload,
    Action,
    Event,
    TurnCompletedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    encode_conversation_event,
    fmt_conversation_stream,
    fmt_dispatch_stream,
)
from fallingwater.web.apirouter import (
    _recent_streams,
    create_app,
    events,
    monitor,
    recent_events,
    router,
)


def _event(event_type: Event, **values):
    if event_type == Event.CONVERSATION_CREATED:
        payload = ConversationCreatedPayload(
            agent=values.get("agent", "demo"), model=values.get("model", "test")
        )
    elif event_type == Event.TURN_QUEUED:
        payload = TurnQueuedPayload(
            turn_id=values.get("turn_id", "turn-1"),
            message=values.get("message", "hello"),
        )
    elif event_type == Event.TURN_COMPLETED:
        payload = TurnCompletedPayload(
            turn_id=values.get("turn_id", "turn-1"),
            message=values.get("message", "answer"),
            turn_messages=[],
            usage=RunUsage(),
        )
    else:
        raise ValueError(event_type)
    return encode_conversation_event(event_type, payload)


def _poke(conversation_id: str):
    return {
        "type": Action.CONVERSATION_POKE,
        "conversation_id": conversation_id,
    }


def test_app_mounts_conversation_events_under_fw_api() -> None:
    redis = MagicMock(spec=Redis)

    app = create_app(redis=redis, namespace="fw-test")

    assert "/fw-api/monitor" in app.openapi()["paths"]
    assert "/fw-api/conversations/recent/events" in app.openapi()["paths"]
    assert (
        "/fw-api/conversations/{conversation_id}/events"
        in app.openapi()["paths"]
    )
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
    redis = MagicMock(spec=Redis)
    stream = "fw-test:conversation:abc:events"
    redis.xread.side_effect = [
        [
            (
                stream,
                [
                    (
                        "1-0",
                        _event(Event.CONVERSATION_CREATED, agent="demo"),
                    ),
                    ("2-0", _event(Event.TURN_QUEUED, message="hello")),
                ],
            )
        ],
        [
            (
                stream,
                [("3-0", _event(Event.TURN_COMPLETED, message="answer"))],
            )
        ],
    ]
    stream_events = events(stream, redis)

    first = next(stream_events)
    second = next(stream_events)
    third = next(stream_events)
    stream_events.close()

    assert isinstance(first, ServerSentEvent)
    assert (first.id, first.event, first.data["payload"]["agent"]) == (
        "1-0",
        Event.CONVERSATION_CREATED,
        "demo",
    )
    assert (second.id, second.event) == ("2-0", Event.TURN_QUEUED)
    assert (third.id, third.event) == ("3-0", Event.TURN_COMPLETED)
    assert redis.xread.call_args_list[0].args[0] == {stream: "0-0"}
    assert redis.xread.call_args_list[1].args[0] == {stream: "2-0"}


def test_unknown_conversation_returns_404_before_streaming() -> None:
    redis = MagicMock(spec=Redis)
    redis.exists.return_value = 0
    app = create_app(redis=redis, namespace="fw-test")

    with TestClient(app) as client:
        response = client.get("/fw-api/conversations/missing/events")

    assert response.status_code == 404
    redis.exists.assert_called_once_with("fw-test:conversation:missing:events")


def test_recent_conversation_discovery_skips_duplicate_dispatch_entries() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrevrange.side_effect = [
        [
            (
                f"{number}-0",
                {
                    "type": Action.CONVERSATION_POKE,
                    "conversation_id": "same",
                },
            )
            for number in range(200, 100, -1)
        ],
        [
            (
                f"{number}-0",
                {
                    "type": Action.CONVERSATION_POKE,
                    "conversation_id": f"c{index}",
                },
            )
            for index, number in enumerate(range(100, 91, -1), start=1)
        ],
    ]

    cursor, streams = _recent_streams(redis, "fw-test")

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
    assert redis.xrevrange.call_args_list[1].kwargs["max"] == "(101-0"


def test_recent_feed_replays_only_ten_events_then_follows_the_tail() -> None:
    redis = MagicMock(spec=Redis)
    namespace = "fw-test"
    stream = fmt_conversation_stream(namespace, "abc")
    redis.xrevrange.side_effect = [
        [("10-0", _poke("abc"))],
        [],
        [
            (
                f"{number}-0",
                _event(Event.TURN_COMPLETED, message=str(number)),
            )
            for number in range(12, 2, -1)
        ],
    ]
    redis.xread.return_value = [
        (stream, [("13-0", _event(Event.TURN_QUEUED))])
    ]
    feed = recent_events(redis, namespace)

    received = [next(feed) for _ in range(11)]
    feed.close()

    assert [event.id for event in received] == [
        f"abc:{number}-0" for number in range(3, 14)
    ]
    assert redis.xrevrange.call_args_list[2].args == (stream,)
    assert redis.xrevrange.call_args_list[2].kwargs["count"] == 10
    assert redis.xread.call_args.args[0][stream] == "12-0"


def test_recent_feed_adds_new_conversations_and_evicts_oldest() -> None:
    redis = MagicMock(spec=Redis)
    namespace = "fw-test"
    dispatch = fmt_dispatch_stream(namespace)

    def stream_for(conversation_id: str) -> str:
        return fmt_conversation_stream(namespace, conversation_id)

    c11_stream = fmt_conversation_stream(namespace, "c11")

    def xrevrange(stream: str, **_kwargs: object):
        if stream == dispatch:
            return [
                (f"{number}-0", _poke(f"c{number}"))
                for number in range(10, 0, -1)
            ]
        if stream == c11_stream:
            return [
                ("3-0", _event(Event.TURN_COMPLETED)),
                ("2-0", _event(Event.TURN_QUEUED)),
                ("1-0", _event(Event.CONVERSATION_CREATED)),
            ]
        return []

    redis.xrevrange.side_effect = xrevrange
    redis.xread.side_effect = [
        [
            (
                stream_for("c10"),
                [("20-0", _event(Event.TURN_COMPLETED, message="old"))],
            )
        ],
        [
            (
                dispatch,
                [("11-0", _poke("c11"))],
            ),
            (
                stream_for("c1"),
                [("21-0", _event(Event.TURN_COMPLETED, message="evicted"))],
            ),
        ],
        [(stream_for("c11"), [("4-0", _event(Event.TURN_COMPLETED))])],
    ]
    feed = recent_events(redis, namespace)

    received = [next(feed) for _ in range(5)]
    feed.close()

    assert (
        received[0].id,
        received[0].event,
        received[0].data["conversation_id"],
    ) == ("c10:20-0", Event.TURN_COMPLETED, "c10")
    assert [event.id for event in received[1:]] == [
        "c11:1-0",
        "c11:2-0",
        "c11:3-0",
        "c11:4-0",
    ]
    assert redis.xread.call_args_list[0].args[0][dispatch] == "10-0"
    followed = redis.xread.call_args_list[2].args[0]
    assert stream_for("c1") not in followed
    assert followed[stream_for("c11")] == "3-0"
