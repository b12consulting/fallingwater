"""Checks for terminal display, input, and reader-thread lifecycle."""

from contextlib import nullcontext
from datetime import datetime
from io import StringIO
from itertools import chain, repeat
from os import terminal_size
from threading import Event as ThreadEvent
from unittest.mock import MagicMock

import pytest
from prompt_toolkit import ANSI, PromptSession
from prompt_toolkit.application import create_app_session
from prompt_toolkit.data_structures import Size
from prompt_toolkit.formatted_text import (
    fragment_list_to_text,
    to_formatted_text,
)
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import ColorDepth, DummyOutput
from prompt_toolkit.output.vt100 import Vt100_Output
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from fallingwater.chat import Chat
from fallingwater.conversation_client import ConversationClient
from fallingwater.models import (
    ConversationCreatedPayload,
    Event,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_event,
    encode_conversation_event,
)


@pytest.fixture(autouse=True)
def _app_session():
    with create_app_session():
        yield


def _created(agent: str = "pydantic_ai:Agent", model: str = "test"):
    return encode_conversation_event(
        Event.CONVERSATION_CREATED,
        ConversationCreatedPayload(agent=agent, model=model),
    )


def _queued(turn_id: str, message: str = "hello"):
    return encode_conversation_event(
        Event.TURN_QUEUED,
        TurnQueuedPayload(turn_id=turn_id, message=message),
    )


def _completed(turn_id: str, message: str):
    return encode_conversation_event(
        Event.TURN_COMPLETED,
        TurnCompletedPayload(
            turn_id=turn_id, message=message, turn_messages=[], usage=RunUsage()
        ),
    )


def _failed(turn_id: str, error: str):
    return encode_conversation_event(
        Event.TURN_FAILED, TurnFailedPayload(turn_id=turn_id, error=error)
    )


def _empty_chat(redis: Redis, **kwargs) -> Chat:
    redis.xrange.return_value = []
    redis.xadd.return_value = "1-0"
    return Chat(
        ConversationClient(
            redis,
            conversation_id="abc",
            agent_path="pydantic_ai:Agent",
            model="test",
            **kwargs,
        )
    )


def _terminal(monkeypatch, exit_error=EOFError) -> MagicMock:
    session = MagicMock()
    session.prompt.side_effect = exit_error
    monkeypatch.setattr(
        "fallingwater.chat.PromptSession", lambda **kwargs: session
    )
    monkeypatch.setattr("fallingwater.chat.patch_stdout", nullcontext)
    monkeypatch.setattr(
        "fallingwater.chat.print_formatted_text",
        lambda text: print(fragment_list_to_text(to_formatted_text(text))),
    )
    return session


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (datetime(2026, 11, 11, 0, 1, 2), "00:01:02"),
        (datetime(2026, 11, 10, 23, 59, 59), "2026/11/10 23:59:59"),
        (datetime(2026, 11, 12, 0, 0, 1), "2026/11/12 00:00:01"),
        (datetime(2025, 11, 11, 12, 0, 0), "2025/11/11 12:00:00"),
    ],
)
def test_timestamp_includes_date_only_outside_today(
    monkeypatch, timestamp: datetime, expected: str
) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return cls(2026, 11, 11, 12, 0, 0)

    monkeypatch.setattr("fallingwater.chat.datetime", FixedDatetime)
    entry_id = f"{int(timestamp.timestamp() * 1000)}-7"

    assert Chat.timestamp(entry_id) == expected


@pytest.mark.parametrize("exit_error", [EOFError, KeyboardInterrupt])
def test_cli_group_replays_unread_completed_turn(
    monkeypatch, capsys, exit_error
) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [
            (
                stream,
                [
                    ("1-0", _created()),
                    ("2-0", _queued("turn-1")),
                    ("3-0", _completed("turn-1", "answer")),
                ],
            )
        ],
        [(stream, [])],
    ]
    redis.xreadgroup.side_effect = chain(
        redis.xreadgroup.side_effect, repeat([])
    )
    _terminal(monkeypatch, exit_error)
    chat = _empty_chat(redis, group="reader-one")

    chat.run()

    assert "answer" in capsys.readouterr().out
    redis.xgroup_create.assert_called_once_with(stream, "reader-one", id="0")
    redis.xack.assert_any_call(stream, "reader-one", "2-0")
    redis.xack.assert_any_call(stream, "reader-one", "3-0")
    assert redis.xreadgroup.call_args_list[0].args == (
        "reader-one",
        "terminal",
        {stream: "0"},
    )
    assert redis.xreadgroup.call_args_list[1].args == (
        "reader-one",
        "terminal",
        {stream: ">"},
    )


@pytest.mark.parametrize("print_fails", [False, True])
def test_read_acknowledges_only_after_successful_rendering(
    monkeypatch, print_fails
) -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    stream = chat.client.stream.name
    redis.xreadgroup.side_effect = [
        [(stream, [("1-0", _created()), ("2-0", _queued("turn-1"))])],
        [],
        [],
    ]

    def print_message(*args, **kwargs):
        redis.xack.assert_called_once_with(stream, "user", "1-0")
        if print_fails:
            raise RuntimeError("display failed")

    monkeypatch.setattr(chat, "print_message", print_message)

    if print_fails:
        with pytest.raises(RuntimeError, match="display failed"):
            list(chat.read(ffwd=True))
        redis.xack.assert_called_once_with(stream, "user", "1-0")
    else:
        assert list(chat.read(ffwd=True)) == []
        assert redis.xack.call_count == 2
        redis.xack.assert_called_with(stream, "user", "2-0")


@pytest.mark.parametrize("columns", [40, 72])
def test_terminal_accepts_overlapping_prompts_and_labels_out_of_order_answers(
    monkeypatch, capsys, columns
) -> None:
    monkeypatch.setattr(
        "fallingwater.chat.get_terminal_size",
        lambda: terminal_size((columns, 24)),
    )
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    chat = _empty_chat(redis)
    first_ms = int(datetime(2026, 11, 11, 11, 27, 38).timestamp() * 1000)
    first_id = f"{first_ms}-0"
    second_id = f"{first_ms + 4000}-0"
    second_answer_id = f"{first_ms + 5000}-0"
    first_answer_id = f"{first_ms + 6000}-0"
    second_sent = ThreadEvent()
    replies_sent = False

    def send(prompt: str) -> tuple[str, str]:
        if prompt == "first request":
            return "turn-1", first_id
        assert prompt == "second request"
        second_sent.set()
        return "turn-2", second_id

    def read_group(*args, **kwargs):
        nonlocal replies_sent
        if kwargs["block"] is None or replies_sent:
            return []
        assert second_sent.wait(timeout=2), "Input waited for the first answer"
        replies_sent = True
        return [
            (
                stream,
                [
                    (first_id, _queued("turn-1", "first request")),
                    (second_id, _queued("turn-2", "second request")),
                    (second_answer_id, _completed("turn-2", "second answer")),
                    (first_answer_id, _completed("turn-1", "first answer")),
                ],
            )
        ]

    redis.xreadgroup.side_effect = read_group
    with create_pipe_input() as pipe, create_app_session(input=pipe):
        session: PromptSession[str] = PromptSession(
            input=pipe, output=DummyOutput(), erase_when_done=True
        )
        monkeypatch.setattr(
            "fallingwater.chat.PromptSession", lambda **kwargs: session
        )
        monkeypatch.setattr(chat.client, "send", MagicMock(side_effect=send))

        def acknowledge(_stream, _group, entry_id):
            if entry_id == first_answer_id:
                pipe.send_text("\x04")

        redis.xack.side_effect = acknowledge
        pipe.send_text("first request\nsecond request\n")
        chat.run()

    output = capsys.readouterr().out
    lines = output.splitlines()
    for timestamp, message in (
        (Chat.timestamp(first_id), "› first request"),
        (Chat.timestamp(second_id), "› second request"),
        (Chat.timestamp(second_id), "second answer"),
        (Chat.timestamp(first_id), "first answer"),
    ):
        index = next(
            index - 1
            for index, line in enumerate(lines)
            if line.rstrip() == message
        )
        assert lines[index].strip() == timestamp
        assert len(lines[index]) == columns
        assert lines[index + 1].rstrip() == message
    assert output.index("second answer") < output.index("first answer")


def test_queued_message_background_preserves_message_text(
    monkeypatch,
) -> None:
    width = 24
    monkeypatch.setattr(
        "fallingwater.chat.get_terminal_size",
        lambda: terminal_size((width, 24)),
    )
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    milliseconds = int(datetime(2026, 11, 11, 11, 27, 38).timestamp() * 1000)
    queued_id = f"{milliseconds}-0"
    stdout = StringIO()
    output = Vt100_Output(
        stdout,
        lambda: Size(rows=24, columns=width),
        term="xterm-256color",
        default_color_depth=ColorDepth.DEPTH_24_BIT,
    )
    with create_app_session(output=output):
        chat.handle_group_event(
            decode_conversation_event(
                queued_id,
                _queued("turn-1", "\u754c" * 12 + "\n\nlast\tline"),
            )
        )
        chat.handle_group_event(
            decode_conversation_event(
                f"{milliseconds + 1000}-0",
                _completed("turn-1", "answer"),
            )
        )

    fragments = to_formatted_text(ANSI(stdout.getvalue()))
    highlighted = "".join(
        text for style, text, *_ in fragments if "bg:#e5e5e5" in style
    )
    lines = highlighted.replace("\r", "").splitlines()
    assert lines == [
        "› " + "\u754c" * 12,
        "",
        "last\tline",
    ]
    assert "answer" in fragment_list_to_text(fragments)
    assert "answer" not in highlighted


def test_cli_group_replays_pending_failure(monkeypatch, capsys) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    milliseconds = int(datetime(2026, 11, 11, 11, 27, 38).timestamp() * 1000)
    queued_id = f"{milliseconds}-0"
    failed_id = f"{milliseconds + 6000}-0"
    redis.xreadgroup.side_effect = chain(
        [
            [
                (
                    stream,
                    [
                        (failed_id, _failed("turn-1", "boom")),
                    ],
                )
            ],
            [(stream, [])],
            [(stream, [])],
        ],
        repeat([]),
    )
    _terminal(monkeypatch)
    chat = _empty_chat(redis)
    redis.xrange.side_effect = [
        [(queued_id, _queued("turn-1"))],
        [],
    ]

    chat.run()

    assert [line.strip() for line in capsys.readouterr().out.splitlines()][
        3:5
    ] == [Chat.timestamp(queued_id), "Turn failed: boom"]
    redis.xack.assert_any_call(stream, "user", failed_id)


@pytest.mark.parametrize("fail_before_prompt", [False, True])
def test_terminal_reader_failure_interrupts_input(
    monkeypatch, fail_before_prompt
) -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    prompt_started = ThreadEvent()

    def read_group(*args, **kwargs):
        if kwargs["block"] is not None:
            if not fail_before_prompt:
                assert prompt_started.wait(timeout=2)
            raise RedisError("reader failed")
        return []

    redis.xreadgroup.side_effect = read_group
    with create_pipe_input() as pipe, create_app_session(input=pipe):
        session: PromptSession[str] = PromptSession(
            input=pipe, output=DummyOutput(), erase_when_done=True
        )
        session.app.after_render += lambda app: prompt_started.set()
        monkeypatch.setattr(
            "fallingwater.chat.PromptSession", lambda **kwargs: session
        )

        with pytest.raises(RedisError, match="reader failed"):
            chat.run()

    assert not session.app.is_running


def test_terminal_send_failure_stops_reader(monkeypatch) -> None:
    redis = MagicMock(spec=Redis)
    redis.xreadgroup.return_value = []
    chat = _empty_chat(redis)
    session = _terminal(monkeypatch)
    session.prompt.side_effect = ["hello", EOFError]
    monkeypatch.setattr(
        chat.client, "send", MagicMock(side_effect=RedisError("send failed"))
    )

    with pytest.raises(RedisError, match="send failed"):
        chat.run()


def test_terminal_does_not_wait_for_existing_or_submitted_turns(
    monkeypatch, capsys
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xreadgroup.side_effect = chain(
        [[("fw:conversation:abc:events", [("2-0", _queued("old"))])]],
        repeat([]),
    )
    chat = _empty_chat(redis)
    session = _terminal(monkeypatch)
    session.prompt.side_effect = ["hello", EOFError]
    send = MagicMock(return_value=("turn-1", "2-0"))
    monkeypatch.setattr(chat.client, "send", send)

    chat.run()

    send.assert_called_once_with("hello")
    assert "Bye." in capsys.readouterr().out


def test_read_keeps_new_event_cursor_across_calls(capsys) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [("2-0", _queued("old"))])],
        [(stream, [])],
        [(stream, [])],
        [(stream, [("3-0", _completed("new", "answer"))])],
    ]
    chat = _empty_chat(redis)
    redis.xrange.side_effect = [
        [("2-1", _queued("new"))],
        [],
    ]

    assert chat.client.cursor == "0"
    assert list(chat.read(ffwd=True)) == []
    assert chat.client.cursor == ">"
    assert list(chat.read()) == ["new"]
    timestamp = Chat.timestamp("2-1")
    assert [line.strip() for line in capsys.readouterr().out.splitlines()] == [
        timestamp,
        "› hello",
        timestamp,
        "answer",
    ]
    assert [call.args[2] for call in redis.xreadgroup.call_args_list] == [
        {stream: "0"},
        {stream: "2-0"},
        {stream: ">"},
        {stream: ">"},
    ]
    assert [
        call.kwargs["block"] for call in redis.xreadgroup.call_args_list
    ] == [
        None,
        None,
        None,
        1000,
    ]


@pytest.mark.parametrize("pending_consumer", ["terminal", "other-reader"])
def test_rewind_replays_once_in_order_without_changing_other_groups_or_work(
    isolated_redis, capsys, pending_consumer
) -> None:
    redis = isolated_redis
    client = ConversationClient(
        redis,
        namespace="fw-test",
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
        group="reader-one",
    )
    chat = Chat(client)
    stream = client.stream
    first_turn, _ = client.send("first prompt")
    redis.xadd(stream.name, _completed(first_turn, "first answer"))
    second_turn, _ = client.send("second prompt")
    redis.xadd(stream.name, _completed(second_turn, "second answer"))
    acknowledged = stream.read_group(
        client.group, client.consumer, ">", count=3
    )
    stream.acknowledge(
        client.group, *(event.entry_id for event in acknowledged)
    )
    pending = stream.read_group(client.group, pending_consumer, ">")
    assert len(pending) == 2
    stream.create_group("other-group", id="$")
    dispatch = stream.dispatch
    dispatch.create_group("workers", mkstream=True)
    dispatch.read_group("workers", "worker-one", ">", count=1)
    events_before = redis.xrange(stream.name)
    dispatch_before = redis.xrange(dispatch.name)
    workers_before = redis.xinfo_groups(dispatch.name)
    other_before = next(
        group
        for group in redis.xinfo_groups(stream.name)
        if group["name"] == b"other-group"
    )

    for _ in range(2):
        client.rewind()
        selected = next(
            group
            for group in redis.xinfo_groups(stream.name)
            if group["name"] == b"reader-one"
        )
        assert selected["last-delivered-id"] == b"0-0"

        chat.resume_group()

        lines = capsys.readouterr().out.splitlines()
        assert len(lines) == 8
        assert lines[1::2] == [
            "› first prompt",
            "first answer",
            "› second prompt",
            "second answer",
        ]
        assert redis.xpending(stream.name, client.group)["pending"] == 0
        assert redis.xrange(stream.name) == events_before
        assert redis.xrange(dispatch.name) == dispatch_before
        assert redis.xinfo_groups(dispatch.name) == workers_before
        assert (
            next(
                group
                for group in redis.xinfo_groups(stream.name)
                if group["name"] == b"other-group"
            )
            == other_before
        )


def test_run_once_prints_its_response_and_acknowledges_it(
    monkeypatch, capsys
) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [(stream, [])],
        [(stream, [])],
        [
            (
                stream,
                [
                    ("2-0", _queued("turn-1", "Hello")),
                    ("3-0", _completed("turn-1", "answer")),
                ],
            )
        ],
    ]
    chat = _empty_chat(redis)
    send = MagicMock(return_value=("turn-1", "2-0"))
    monkeypatch.setattr(chat.client, "send", send)

    chat.run_once("Hello")

    send.assert_called_once_with("Hello")
    timestamp = Chat.timestamp("2-0")
    assert [line.strip() for line in capsys.readouterr().out.splitlines()] == [
        timestamp,
        "› Hello",
        timestamp,
        "answer",
    ]
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")


def test_run_once_prints_prior_answers_before_new_answer(
    monkeypatch, capsys
) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [
            (
                stream,
                [
                    ("2-0", _queued("old")),
                    ("3-0", _completed("old", "earlier answer")),
                ],
            )
        ],
        [(stream, [])],
        [(stream, [])],
        [
            (
                stream,
                [
                    ("4-0", _queued("new", "Hello again")),
                    ("5-0", _completed("new", "new answer")),
                ],
            )
        ],
    ]
    chat = _empty_chat(redis)
    monkeypatch.setattr(
        chat.client, "send", MagicMock(return_value=("new", "4-0"))
    )

    chat.run_once("Hello again")

    output = capsys.readouterr()
    timestamp = Chat.timestamp("2-0")
    assert [line.strip() for line in output.out.splitlines()] == [
        timestamp,
        "› hello",
        timestamp,
        "earlier answer",
        timestamp,
        "› Hello again",
        timestamp,
        "new answer",
    ]
    assert output.err == ""
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0")
    redis.xack.assert_any_call(stream, "user", "5-0")
