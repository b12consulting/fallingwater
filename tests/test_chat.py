"""Checks for submitting a turn and observing compressed conversation events."""

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
from fallingwater.models import (
    ConversationCreatedPayload,
    Action,
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
    return Chat(
        redis,
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
        **kwargs,
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


def test_send_pokes_dispatch_with_only_conversation_and_turn_ids() -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    redis.xadd.return_value = "2-0"

    turn_id, cursor = chat.send("hello")

    assert cursor == "2-0"
    queued_stream, queued_fields = redis.xadd.call_args_list[-2].args
    queued = decode_conversation_event(cursor, queued_fields)
    assert queued_stream == "fw:conversation:abc:events"
    assert queued.type == Event.TURN_QUEUED
    assert queued.payload.turn_id == turn_id
    assert queued.payload.message == "hello"
    dispatch_stream, dispatch_fields = redis.xadd.call_args_list[-1].args
    assert dispatch_stream == "fw:dispatch"
    assert dispatch_fields == {
        "type": Action.CONVERSATION_POKE,
        "turn_id": turn_id,
        "conversation_id": "abc",
    }
    redis.pipeline.assert_not_called()


def test_send_queues_repeated_prompts_as_distinct_turns() -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    redis.xadd.side_effect = ["2-0", "1-0", "3-0", "2-0"]

    first_turn, first_cursor = chat.send("hello")
    second_turn, second_cursor = chat.send("hello")

    assert first_turn != second_turn
    assert (first_cursor, second_cursor) == ("2-0", "3-0")
    queued = [
        decode_conversation_event(cursor, call.args[1]).payload
        for cursor, call in zip(
            (first_cursor, second_cursor), redis.xadd.call_args_list[-4::2]
        )
    ]
    assert queued == [
        TurnQueuedPayload(turn_id=first_turn, message="hello"),
        TurnQueuedPayload(turn_id=second_turn, message="hello"),
    ]
    assert chat.conversation.turn_state == {}


def test_send_does_not_dispatch_when_persistence_fails() -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    redis.xadd.reset_mock()
    redis.xadd.side_effect = RuntimeError("write failed")

    with pytest.raises(RuntimeError, match="write failed"):
        chat.send("hello")

    assert redis.xadd.call_count == 1
    assert redis.xadd.call_args.args[0] == "fw:conversation:abc:events"
    assert chat.conversation.turn_state == {}


def test_named_new_conversation_uses_supplied_agent_and_model() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    chat = Chat(
        redis,
        conversation_id="test",
        agent_path="pydantic_ai:Agent",
        model="test",
    )

    assert chat.conversation_id == "test"
    assert (chat.agent_path, chat.model) == ("pydantic_ai:Agent", "test")
    _stream, fields = redis.xadd.call_args.args
    created = decode_conversation_event("1-0", fields)
    assert created.type == Event.CONVERSATION_CREATED
    assert created.payload.agent == "pydantic_ai:Agent"
    assert created.payload.model == "test"
    redis.xgroup_create.assert_called_once_with(
        "fw:conversation:test:events", "user", id="0"
    )


def test_receive_reads_completed_message() -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xread.return_value = [
        (
            stream,
            [
                ("3-0", _queued("other")),
                ("4-0", _completed("turn-1", "answer")),
            ],
        )
    ]
    chat = _empty_chat(redis)

    assert chat.receive("turn-1", "2-0") == "answer"
    redis.xread.assert_called_once_with({stream: "2-0"}, block=1000)


def test_invalid_agent_does_not_create_conversation() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []

    with pytest.raises(ValueError, match="cannot instantiate agent"):
        Chat(
            redis,
            conversation_id="test",
            agent_path="fallingwater.demo:missing_agent",
            model="test",
        )

    redis.xadd.assert_not_called()


def test_new_conversation_requires_model_when_agent_has_none() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []

    with pytest.raises(ValueError, match="pass --model"):
        Chat(
            redis,
            conversation_id="test",
            agent_path="fallingwater.demo:haiku_master",
        )

    redis.xadd.assert_not_called()


def test_resume_uses_saved_agent_and_model_and_validates_them() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [("1-0", _created())]

    chat = Chat(
        redis,
        conversation_id="test",
        agent_path="fallingwater.demo:missing_agent",
        model="another-model",
    )

    assert (chat.agent_path, chat.model) == ("pydantic_ai:Agent", "test")
    redis.xadd.assert_not_called()


def test_resume_rejects_unavailable_saved_agent() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        (
            "1-0",
            _created(agent="fallingwater.demo:missing_agent"),
        )
    ]

    with pytest.raises(ValueError, match="cannot instantiate agent"):
        Chat(
            redis,
            conversation_id="test",
            agent_path="pydantic_ai:Agent",
            model="test",
        )


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
        monkeypatch.setattr(chat, "send", MagicMock(side_effect=send))

        def acknowledge(_stream, _group, entry_id):
            if entry_id == first_answer_id:
                pipe.send_text("\x04")

        redis.xack.side_effect = acknowledge
        pipe.send_text("first request\nsecond request\n")
        chat.run()

    output = capsys.readouterr().out
    lines = output.splitlines()
    for timestamp, message in (
        ("[11:27:38]", "You: first request"),
        ("[11:27:42]", "You: second request"),
        ("[reply to 11:27:42]", "Agent: second answer"),
        ("[reply to 11:27:38]", "Agent: first answer"),
    ):
        index = next(
            index
            for index, line in enumerate(lines)
            if line.strip() == timestamp
        )
        assert len(lines[index]) == columns
        assert lines[index + 1].rstrip() == message
    assert output.index("Agent: second answer") < output.index(
        "Agent: first answer"
    )


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
        " " * 14 + "[11:27:38]",
        "You: " + "\u754c" * 12,
        "",
        "last\tline",
    ]
    assert "Agent: answer" in fragment_list_to_text(fragments)
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

    assert "[reply to 11:27:38]\nTurn failed: boom" in capsys.readouterr().out
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
        chat, "send", MagicMock(side_effect=RedisError("send failed"))
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
    monkeypatch.setattr(chat, "send", send)

    chat.run()

    send.assert_called_once_with("hello")
    assert "Conversation left open." in capsys.readouterr().out


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

    assert chat.cursor == "0"
    assert list(chat.read(ffwd=True)) == []
    assert chat.cursor == ">"
    assert list(chat.read()) == ["new"]
    timestamp = Chat.timestamp("2-1")
    assert [line.strip() for line in capsys.readouterr().out.splitlines()] == [
        f"[{timestamp}]",
        "You: hello",
        f"[reply to {timestamp}]",
        "Agent: answer",
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
    chat = Chat(
        redis,
        namespace="fw-test",
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
        group="reader-one",
    )
    first_turn, _ = chat.send("first prompt")
    redis.xadd(chat.events, _completed(first_turn, "first answer"))
    second_turn, _ = chat.send("second prompt")
    redis.xadd(chat.events, _completed(second_turn, "second answer"))
    acknowledged = chat.stream.read_group(
        chat.group, chat.consumer, ">", count=3
    )
    chat.stream.acknowledge(
        chat.group, *(event.entry_id for event in acknowledged)
    )
    pending = chat.stream.read_group(chat.group, pending_consumer, ">")
    assert len(pending) == 2
    chat.stream.create_group("other-group", id="$")
    dispatch = chat.stream.dispatch
    dispatch.create_group("workers", mkstream=True)
    dispatch.read_group("workers", "worker-one", ">", count=1)
    events_before = redis.xrange(chat.events)
    dispatch_before = redis.xrange(dispatch.name)
    workers_before = redis.xinfo_groups(dispatch.name)
    other_before = next(
        group
        for group in redis.xinfo_groups(chat.events)
        if group["name"] == b"other-group"
    )

    for _ in range(2):
        chat.rewind()
        selected = next(
            group
            for group in redis.xinfo_groups(chat.events)
            if group["name"] == b"reader-one"
        )
        assert selected["last-delivered-id"] == b"0-0"

        chat.resume_group()

        lines = capsys.readouterr().out.splitlines()
        assert len(lines) == 8
        assert [
            line for line in lines if line.startswith(("You:", "Agent:"))
        ] == [
            "You: first prompt",
            "Agent: first answer",
            "You: second prompt",
            "Agent: second answer",
        ]
        assert redis.xpending(chat.events, chat.group)["pending"] == 0
        assert redis.xrange(chat.events) == events_before
        assert redis.xrange(dispatch.name) == dispatch_before
        assert redis.xinfo_groups(dispatch.name) == workers_before
        assert (
            next(
                group
                for group in redis.xinfo_groups(chat.events)
                if group["name"] == b"other-group"
            )
            == other_before
        )


def test_rewind_failure_does_not_change_local_reader_cursor() -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    chat.cursor = "123-0"
    redis.xgroup_setid.side_effect = RedisError("rewind failed")

    with pytest.raises(RedisError, match="rewind failed"):
        chat.rewind()

    assert chat.cursor == "123-0"


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
    monkeypatch.setattr(chat, "send", send)

    chat.run_once("Hello")

    send.assert_called_once_with("Hello")
    timestamp = Chat.timestamp("2-0")
    assert [line.strip() for line in capsys.readouterr().out.splitlines()] == [
        f"[{timestamp}]",
        "You: Hello",
        f"[reply to {timestamp}]",
        "Agent: answer",
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
    monkeypatch.setattr(chat, "send", MagicMock(return_value=("new", "4-0")))

    chat.run_once("Hello again")

    output = capsys.readouterr()
    timestamp = Chat.timestamp("2-0")
    assert [line.strip() for line in output.out.splitlines()] == [
        f"[{timestamp}]",
        "You: hello",
        f"[reply to {timestamp}]",
        "Agent: earlier answer",
        f"[{timestamp}]",
        "You: Hello again",
        f"[reply to {timestamp}]",
        "Agent: new answer",
    ]
    assert output.err == ""
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0")
    redis.xack.assert_any_call(stream, "user", "5-0")
