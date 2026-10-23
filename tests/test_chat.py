"""Checks for submitting a turn and observing compressed conversation events."""

from unittest.mock import MagicMock

import pytest
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.chat import Chat
from fallingwater.streams import (
    ConversationCreatedPayload,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    decode_conversation_event,
    encode_conversation_event,
)


def _created(agent: str = "pydantic_ai:Agent", model: str = "test"):
    return encode_conversation_event(
        EventType.CONVERSATION_CREATED,
        ConversationCreatedPayload(agent=agent, model=model),
    )


def _queued(turn_id: str, message: str = "hello"):
    return encode_conversation_event(
        EventType.TURN_QUEUED,
        TurnQueuedPayload(turn_id=turn_id, message=message),
    )


def _completed(turn_id: str, message: str):
    return encode_conversation_event(
        EventType.TURN_COMPLETED,
        TurnCompletedPayload(
            turn_id=turn_id, message=message, history=[], usage=RunUsage()
        ),
    )


def _failed(turn_id: str, error: str):
    return encode_conversation_event(
        EventType.TURN_FAILED, TurnFailedPayload(turn_id=turn_id, error=error)
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
    pipeline = redis.pipeline.return_value.__enter__.return_value
    pipeline.execute.return_value = ("2-0", "1-0")

    turn_id, cursor = chat.send("hello")

    assert cursor == "2-0"
    queued_stream, queued_fields = pipeline.xadd.call_args_list[0].args
    queued = decode_conversation_event(queued_fields)
    assert queued_stream == "fw:conversation:abc:events"
    assert queued.type == EventType.TURN_QUEUED
    assert queued.payload.turn_id == turn_id
    assert queued.payload.message == "hello"
    dispatch_stream, dispatch_fields = pipeline.xadd.call_args_list[1].args
    assert dispatch_stream == "fw:dispatch"
    assert dispatch_fields == {
        "type": EventType.CONVERSATION_POKE,
        "turn_id": turn_id,
        "conversation_id": "abc",
    }
    redis.pipeline.assert_called_once_with(transaction=True)


def test_named_new_conversation_uses_supplied_agent_and_model() -> None:
    redis = MagicMock(spec=Redis)
    chat = Chat(
        redis, conversation_id="test", agent_path="pydantic_ai:Agent", model="test"
    )

    assert chat.conversation_id == "test"
    assert (chat.agent_path, chat.model) == ("pydantic_ai:Agent", "test")
    _stream, fields = redis.xadd.call_args.args
    created = decode_conversation_event(fields)
    assert created.type == EventType.CONVERSATION_CREATED
    assert created.payload.agent == "pydantic_ai:Agent"
    assert created.payload.model == "test"


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
        Chat(redis, conversation_id="test", agent_path="fallingwater.demo:haiku_master")

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
    redis.xrange.return_value = [(
        "1-0",
        _created(agent="fallingwater.demo:missing_agent"),
    )]

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
    redis.xack.assert_any_call(stream, "reader-one", "2-0", "3-0")
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


def test_cli_group_recovers_pending_turn_and_waits_for_its_answer(
    monkeypatch, capsys
) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [("2-0", _queued("turn-1"))])],
        [(stream, [])],
        [(stream, [])],
        [(stream, [("3-0", _completed("turn-1", "late answer"))])],
    ]
    monkeypatch.setattr("builtins.input", _end_input)
    chat = _empty_chat(redis)

    chat.run()

    assert "late answer" in capsys.readouterr().out
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")
    assert redis.xreadgroup.call_args_list[-1].kwargs["block"] == 1000


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
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")


def test_run_once_prints_its_response_and_acknowledges_it(monkeypatch, capsys) -> None:
    redis = MagicMock(spec=Redis)
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
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
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")


def test_run_once_keeps_prior_answers_out_of_stdout(monkeypatch, capsys) -> None:
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
    assert output.out == "new answer\n"
    assert output.err == "earlier answer\n"
    redis.xack.assert_any_call(stream, "user", "2-0", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0", "5-0")
