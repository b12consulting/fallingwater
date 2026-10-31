"""Checks for submitting a turn and observing compressed conversation events."""

from unittest.mock import MagicMock

import pytest
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.chat import Chat
from fallingwater.stream_models import (
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
            turn_id=turn_id, message=message, history=[], usage=RunUsage()
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


def _end_input(_prompt: str) -> str:
    raise EOFError


def test_send_pokes_dispatch_with_only_conversation_and_turn_ids() -> None:
    redis = MagicMock(spec=Redis)
    chat = _empty_chat(redis)
    redis.xadd.return_value = "2-0"

    turn_id, cursor = chat.send("hello")

    assert cursor == "2-0"
    queued_stream, queued_fields = redis.xadd.call_args_list[-2].args
    queued = decode_conversation_event(queued_fields)
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
        decode_conversation_event(call.args[1]).payload
        for call in redis.xadd.call_args_list[-4::2]
    ]
    assert queued == [
        TurnQueuedPayload(turn_id=first_turn, message="hello"),
        TurnQueuedPayload(turn_id=second_turn, message="hello"),
    ]


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
    created = decode_conversation_event(fields)
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


def test_cli_group_replays_unread_completed_turn(monkeypatch, capsys) -> None:
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
    monkeypatch.setattr("builtins.input", _end_input)
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


def test_cli_group_does_not_wait_for_pending_turn_without_an_answer(
    monkeypatch, capsys
) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [("2-0", _queued("turn-1"))])],
        [(stream, [])],
        [(stream, [])],
        [(stream, [])],
        [(stream, [("3-0", _completed("turn-1", "late answer"))])],
        [(stream, [])],
        [
            (
                stream,
                [
                    ("4-0", _queued("turn-2", "new prompt")),
                    ("5-0", _completed("turn-2", "new answer")),
                ],
            )
        ],
    ]
    prompts = iter(["new prompt"])

    def next_prompt(_prompt: str) -> str:
        try:
            return next(prompts)
        except StopIteration:
            raise EOFError from None

    monkeypatch.setattr("builtins.input", next_prompt)
    chat = _empty_chat(redis)
    send = MagicMock(return_value=("turn-2", "4-0"))
    monkeypatch.setattr(chat, "send", send)

    chat.run()

    output = capsys.readouterr().out
    assert "late answer" in output
    assert "new answer" in output
    send.assert_called_once_with("new prompt")
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0")
    redis.xack.assert_any_call(stream, "user", "5-0")


def test_cli_group_replays_pending_failure(monkeypatch, capsys) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [
            (
                stream,
                [
                    ("2-0", _queued("turn-1")),
                    ("3-0", _failed("turn-1", "boom")),
                ],
            )
        ],
        [(stream, [])],
        [(stream, [])],
    ]
    monkeypatch.setattr("builtins.input", _end_input)
    chat = _empty_chat(redis)

    chat.run()

    assert "Turn failed: boom" in capsys.readouterr().out
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")


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

    assert chat.cursor == "0"
    assert list(chat.read(ffwd=True)) == []
    assert chat.cursor == ">"
    assert list(chat.read()) == ["new"]
    assert capsys.readouterr().out == "answer\n"
    assert [call.args[2] for call in redis.xreadgroup.call_args_list] == [
        {stream: "0"},
        {stream: "2-0"},
        {stream: ">"},
        {stream: ">"},
    ]
    assert [call.kwargs["block"] for call in redis.xreadgroup.call_args_list] == [
        None,
        None,
        None,
        1000,
    ]


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
                    ("2-0", _queued("turn-1")),
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
    assert capsys.readouterr().out == "answer\n"
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
    assert output.out == "earlier answer\nnew answer\n"
    assert output.err == ""
    redis.xack.assert_any_call(stream, "user", "2-0")
    redis.xack.assert_any_call(stream, "user", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0")
    redis.xack.assert_any_call(stream, "user", "5-0")
