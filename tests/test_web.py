"""Checks for the optional FastAPI conversation event feed."""

from html.parser import HTMLParser
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
    TurnFailedPayload,
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
    elif event_type == Event.TURN_FAILED:
        payload = TurnFailedPayload(
            turn_id=values.get("turn_id", "turn-1"),
            error=values.get("error", "tool failed"),
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


def _elements(html: str, tag: str) -> list[dict]:
    class Collector(HTMLParser):
        def __init__(self):
            super().__init__()
            self.elements = []

        def handle_starttag(self, name, attrs):
            if name == tag:
                self.elements.append(dict(attrs))

    parser = Collector()
    parser.feed(html)
    return parser.elements


def _timeline_row_sizes(html: str) -> list[int]:
    class Collector(HTMLParser):
        def __init__(self):
            super().__init__()
            self.sizes = []
            self.in_row = False

        def handle_starttag(self, name, attrs):
            attrs = dict(attrs)
            if name == "div" and "event-timeline" in attrs.get("class", ""):
                self.in_row = True
                self.sizes.append(0)
            elif name == "button" and self.in_row:
                self.sizes[-1] += 1

        def handle_endtag(self, name):
            if name == "div":
                self.in_row = False

    parser = Collector()
    parser.feed(html)
    return parser.sizes


def _dashboard_redis(conversation_ids: list[str]) -> MagicMock:
    redis = MagicMock(spec=Redis)
    dispatch = fmt_dispatch_stream("fw-test")
    redis.exists.return_value = 1

    def xrevrange(stream, **kwargs):
        if stream == dispatch:
            if kwargs["max"] != "+":
                return []
            return [
                (f"{index}-0", _poke(conversation_id))
                for index, conversation_id in enumerate(
                    reversed(conversation_ids), start=1
                )
            ]
        return [
            (
                f"17000000000{index:02}-0",
                _event(
                    Event.TURN_FAILED if index == 5 else Event.TURN_QUEUED,
                    message=f"Message {index}",
                ),
            )
            for index in range(12, 2, -1)
        ][: kwargs["count"]]

    redis.xrevrange.side_effect = xrevrange
    redis.xrange.return_value = [
        ("1700000000000-0", _event(Event.CONVERSATION_CREATED))
    ]
    return redis


def test_dashboard_empty_page_under_fw_api() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrevrange.return_value = []
    app = create_app(redis=redis)

    with TestClient(app) as client:
        response = client.get("/fw-api/dashboard/", follow_redirects=False)

    assert response.status_code == 200
    assert response.url.path == "/fw-api/dashboard/"
    assert "No recent conversations yet" in response.text
    assert "htmx.org@4.0.1" in response.text
    assert "bootstrap@5.3.8" in response.text
    assert 'hx-swap="innerMorph"' in response.text
    assert 'hx-trigger="every 2s, dashboardRefresh"' in response.text


@pytest.mark.parametrize(
    "path", ["/dashboard", "/dashboard/", "/dashboard/abc"]
)
def test_dashboard_root_shortcuts_are_not_available(path) -> None:
    app = create_app(redis=MagicMock(spec=Redis))

    with TestClient(app) as client:
        response = client.get(path, follow_redirects=False)

    assert response.status_code == 404
    assert "location" not in response.headers


def test_dashboard_limits_cards_and_events_and_refreshes_recent_set() -> None:
    conversation_ids = [f"c{index}" for index in range(1, 13)]
    redis = _dashboard_redis(conversation_ids)
    app = create_app(redis=redis, namespace="fw-test")

    with TestClient(app) as client:
        response = client.get("/fw-api/dashboard/")
        cards = _elements(response.text, "article")
        slots = [
            element
            for element in _elements(response.text, "button")
            if "data-event-id" in element
        ]
        assert [card["data-conversation-id"] for card in cards] == [
            f"c{index}" for index in range(12, 2, -1)
        ]
        assert len(slots) == 100
        assert [slot["data-event-id"] for slot in slots[:10]] == [
            f"17000000000{index:02}-0" for index in range(3, 13)
        ]
        assert "event-failed" in slots[2]["class"]
        assert all(
            "event-success" in slot["class"]
            for index, slot in enumerate(slots[:10])
            if index != 2
        )
        assert all(slot["hx-target"] == "#event-detail" for slot in slots)
        assert all("aria-label" in slot for slot in slots)
        first_time = "2023-11-14T22:13:20.003000+00:00"
        last_time = "2023-11-14T22:13:20.012000+00:00"
        assert slots[0]["title"].endswith(first_time)
        assert slots[0]["aria-label"].endswith(first_time)
        assert f">{first_time}</time>" in response.text
        assert f">{last_time}</time>" in response.text

        conversation_ids.append("c13")
        refreshed = client.get("/fw-api/dashboard-cards")

    assert refreshed.status_code == 200
    assert "<!doctype" not in refreshed.text
    assert [
        card["data-conversation-id"]
        for card in _elements(refreshed.text, "article")
    ] == [f"c{index}" for index in range(13, 3, -1)]


def test_dashboard_escapes_content_and_loads_exact_full_event() -> None:
    conversation_id = 'chat & "<script>'
    redis = _dashboard_redis([conversation_id])
    app = create_app(redis=redis, namespace="fw-test")
    entry_id = "1700000000003-0"
    message = "<script>alert('unsafe')</script>"
    redis.xrange.side_effect = [
        [("1700000000000-0", _event(Event.CONVERSATION_CREATED))],
        [(entry_id, _event(Event.TURN_COMPLETED, message=message))],
    ]

    with TestClient(app) as client:
        page = client.get("/fw-api/dashboard/")
        slots = [
            element
            for element in _elements(page.text, "button")
            if "data-event-id" in element
        ]
        details = client.get(slots[0]["hx-get"])

    assert conversation_id not in page.text
    assert "chat &amp; &#34;&lt;script&gt;" in page.text
    assert details.status_code == 200
    assert "Assistant message" in details.text
    assert "<script>alert" not in details.text
    assert "&lt;script&gt;" in details.text
    assert '"turn_messages"' in details.text
    assert '"usage"' in details.text
    assert ">2023-11-14T22:13:20.003000+00:00</time>" in details.text
    redis.xrange.assert_called_with(
        fmt_conversation_stream("fw-test", conversation_id),
        min=entry_id,
        max=entry_id,
        count=1,
    )


def test_dashboard_failed_event_is_red_and_shows_error() -> None:
    redis = MagicMock(spec=Redis)
    redis.exists.return_value = 1
    redis.xrange.return_value = [
        ("3-0", _event(Event.TURN_FAILED, error="Network timed out"))
    ]
    app = create_app(redis=redis)

    with TestClient(app) as client:
        response = client.get(
            "/fw-api/dashboard-event",
            params={"conversation_id": "abc", "entry_id": "3-0"},
        )

    assert response.status_code == 200
    assert "text-bg-danger" in response.text
    assert "Network timed out" in response.text


@pytest.mark.parametrize("event_count", [0, 20, 23])
def test_conversation_dashboard_full_history_rows_and_refresh(
    event_count,
) -> None:
    redis = MagicMock(spec=Redis)
    redis.exists.return_value = 1
    history = [
        (
            f"{1700000000000 + index}-0",
            _event(
                Event.CONVERSATION_CREATED
                if index == 0
                else Event.TURN_FAILED
                if index == 5
                else Event.TURN_QUEUED,
                message=f"History message {index}",
            ),
        )
        for index in range(event_count)
    ]
    redis.xrange.side_effect = lambda *_args, **_kwargs: list(history)
    app = create_app(redis=redis, namespace="fw-test")

    with TestClient(app) as client:
        page = client.get("/fw-api/dashboard/abc", follow_redirects=False)
        assert page.status_code == 200
        assert page.url.path == "/fw-api/dashboard/abc"
        assert "http://testserver/fw-api/conversations/abc/events" in [
            item["href"] for item in _elements(page.text, "a")
        ]
        assert "Full retained history, oldest to newest." not in page.text
        slots = [
            item
            for item in _elements(page.text, "button")
            if "data-event-id" in item
        ]
        assert [slot["data-event-id"] for slot in slots] == [
            entry_id for entry_id, _ in history
        ]
        assert _timeline_row_sizes(page.text) == (
            [10] * (event_count // 10)
            + ([event_count % 10] if event_count % 10 else [])
        )
        assert all(slot["hx-target"] == "#event-detail" for slot in slots)
        assert all("data-bs-toggle" not in slot for slot in slots)
        assert 'id="event-modal"' not in page.text
        assert (
            len(
                [
                    item
                    for item in _elements(page.text, "div")
                    if item.get("id") == "event-detail"
                ]
            )
            == 1
        )
        assert "col-12 col-md-6 col-xl-4" in page.text
        assert "col-12 col-md-6 col-xl-8" in page.text
        if event_count:
            assert "event-failed" in slots[5]["class"]
        else:
            assert "Waiting for conversation events" in page.text

        history.append(("1700000000099-0", _event(Event.TURN_COMPLETED)))
        refreshed = client.get(
            "/fw-api/dashboard-history", params={"conversation_id": "abc"}
        )

    assert refreshed.status_code == 200
    assert "<!doctype" not in refreshed.text
    assert 'id="event-detail"' not in refreshed.text
    assert (
        len(
            [
                item
                for item in _elements(refreshed.text, "button")
                if "data-event-id" in item
            ]
        )
        == event_count + 1
    )
    redis.xrange.assert_called_with(
        fmt_conversation_stream("fw-test", "abc"), min="-", max="+"
    )
    redis.xrevrange.assert_not_called()


@pytest.mark.parametrize(
    "conversation_id", ['chat & "<script>', "chat/path?x=#y", "cards", "event"]
)
def test_dashboard_conversation_links_preserve_and_escape_ids(
    conversation_id,
) -> None:
    redis = _dashboard_redis([conversation_id])
    app = create_app(redis=redis, namespace="fw-test")

    with TestClient(app) as client:
        page = client.get("/fw-api/dashboard/")
        link = next(
            item["href"]
            for item in _elements(page.text, "a")
            if "/dashboard/" in item.get("href", "")
        )
        history = client.get(link)

    assert history.status_code == 200
    assert "<script>" not in history.text.split("</head>", 1)[0]
    assert (
        _elements(history.text, "article")[0]["data-conversation-id"]
        == conversation_id
    )
    redis.xrange.assert_called_with(
        fmt_conversation_stream("fw-test", conversation_id), min="-", max="+"
    )


@pytest.mark.parametrize(
    ("conversation_id", "exists", "status"),
    [("missing", 0, 404), ("invalid:id", 1, 400)],
)
def test_conversation_dashboard_validates_conversation(
    conversation_id, exists, status
) -> None:
    redis = MagicMock(spec=Redis)
    redis.exists.return_value = exists
    app = create_app(redis=redis)

    with TestClient(app) as client:
        page = client.get(f"/fw-api/dashboard/{conversation_id}")
        fragment = client.get(
            "/fw-api/dashboard-history",
            params={"conversation_id": conversation_id},
        )

    assert page.status_code == status
    assert fragment.status_code == status
    redis.xrange.assert_not_called()


@pytest.mark.parametrize(
    ("exists", "entry_id", "status"),
    [(0, "3-0", 404), (1, "3-0", 404), (1, "invalid", 422)],
)
def test_dashboard_missing_or_invalid_event(exists, entry_id, status) -> None:
    redis = MagicMock(spec=Redis)
    redis.exists.return_value = exists
    redis.xrange.return_value = []
    app = create_app(redis=redis)

    with TestClient(app) as client:
        response = client.get(
            "/fw-api/dashboard-event",
            params={"conversation_id": "abc", "entry_id": entry_id},
        )

    assert response.status_code == status


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
    redis.xread.return_value = [(stream, [("13-0", _event(Event.TURN_QUEUED))])]
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
