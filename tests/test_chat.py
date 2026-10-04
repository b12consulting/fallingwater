"""Checks for submitting a turn to the conversation and dispatch streams."""

from unittest.mock import MagicMock, call

import pytest
from redis import Redis

from fallingwater.chat import Chat
from fallingwater.streams import EventType


def _end_input(_prompt: str) -> str:
    raise EOFError


def test_send_pokes_dispatch_with_only_conversation_and_turn_ids() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    pipeline = redis.pipeline.return_value.__enter__.return_value
    pipeline.execute.return_value = ("2-0", "1-0")
    chat = Chat(
        redis,
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
    )

    turn_id, cursor = chat.send("hello")

    assert cursor == "2-0"
    pipeline.xadd.assert_has_calls(
        [
            call(
                "fw:conversation:abc:events",
                {"type": EventType.TURN_QUEUED, "turn_id": turn_id, "prompt": "hello"},
            ),
            call(
                "fw:dispatch",
                {
                    "type": EventType.CONVERSATION_POKE,
                    "turn_id": turn_id,
                    "conversation_id": "abc",
                },
            ),
        ]
    )
    redis.pipeline.assert_called_once_with(transaction=True)


def test_named_new_conversation_uses_supplied_agent_and_model() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []

    chat = Chat(
        redis, conversation_id="test", agent_path="pydantic_ai:Agent", model="test"
    )

    assert chat.conversation_id == "test"
    assert (chat.agent_path, chat.model) == ("pydantic_ai:Agent", "test")
    redis.xadd.assert_called_once_with(
        "fw:conversation:test:events",
        {
            "type": EventType.CONVERSATION_CREATED,
            "agent": "pydantic_ai:Agent",
            "model": "test",
        },
    )


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
    redis.xrange.return_value = [
        (
            "1-0",
            {
                "type": EventType.CONVERSATION_CREATED,
                "agent": "pydantic_ai:Agent",
                "model": "test",
            },
        )
    ]

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
            {
                "type": EventType.CONVERSATION_CREATED,
                "agent": "fallingwater.demo:missing_agent",
                "model": "test",
            },
        )
    ]

    with pytest.raises(ValueError, match="cannot instantiate agent"):
        Chat(
            redis, conversation_id="test", agent_path="pydantic_ai:Agent", model="test"
        )


def test_cli_group_replays_unread_completed_turn(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [
            (
                stream,
                [
                    ("1-0", {"type": EventType.CONVERSATION_CREATED}),
                    ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "turn-1"}),
                    (
                        "3-0",
                        {
                            "type": EventType.TURN_COMPLETED,
                            "turn_id": "turn-1",
                            "output": "answer",
                        },
                    ),
                ],
            )
        ],
        [(stream, [])],
    ]
    monkeypatch.setattr("builtins.input", _end_input)
    chat = Chat(
        redis,
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
        group="reader-one",
    )

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
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "turn-1"})])],
        [(stream, [])],
        [(stream, [])],
        [
            (
                stream,
                [
                    (
                        "3-0",
                        {
                            "type": EventType.TURN_COMPLETED,
                            "turn_id": "turn-1",
                            "output": "late answer",
                        },
                    )
                ],
            )
        ],
    ]
    monkeypatch.setattr("builtins.input", _end_input)
    chat = Chat(
        redis, conversation_id="abc", agent_path="pydantic_ai:Agent", model="test"
    )

    chat.run()

    assert "late answer" in capsys.readouterr().out
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")
    assert redis.xreadgroup.call_args_list[-1].kwargs["block"] == 1000


def test_cli_group_replays_pending_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [
            (
                stream,
                [
                    ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "turn-1"}),
                    (
                        "3-0",
                        {
                            "type": EventType.TURN_FAILED,
                            "turn_id": "turn-1",
                            "error": "boom",
                        },
                    ),
                ],
            )
        ],
        [(stream, [])],
        [(stream, [])],
    ]
    monkeypatch.setattr("builtins.input", _end_input)
    chat = Chat(
        redis, conversation_id="abc", agent_path="pydantic_ai:Agent", model="test"
    )

    chat.run()

    assert "Turn failed: boom" in capsys.readouterr().out
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")


def test_run_once_prints_its_response_and_acknowledges_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [(stream, [])],
        [
            (
                stream,
                [
                    ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "turn-1"}),
                    (
                        "3-0",
                        {
                            "type": EventType.TURN_COMPLETED,
                            "turn_id": "turn-1",
                            "output": "answer",
                        },
                    ),
                ],
            )
        ],
    ]
    chat = Chat(
        redis, conversation_id="abc", agent_path="pydantic_ai:Agent", model="test"
    )
    send = MagicMock(return_value=("turn-1", "2-0"))
    monkeypatch.setattr(chat, "send", send)

    chat.run_once("Hello")

    send.assert_called_once_with("Hello")
    assert capsys.readouterr().out == "answer\n"
    redis.xack.assert_called_once_with(stream, "user", "2-0", "3-0")


def test_run_once_keeps_prior_answers_out_of_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []
    stream = "fw:conversation:abc:events"
    redis.xreadgroup.side_effect = [
        [(stream, [])],
        [
            (
                stream,
                [
                    ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "old"}),
                    (
                        "3-0",
                        {
                            "type": EventType.TURN_COMPLETED,
                            "turn_id": "old",
                            "output": "earlier answer",
                        },
                    ),
                ],
            )
        ],
        [(stream, [])],
        [
            (
                stream,
                [
                    ("4-0", {"type": EventType.TURN_QUEUED, "turn_id": "new"}),
                    (
                        "5-0",
                        {
                            "type": EventType.TURN_COMPLETED,
                            "turn_id": "new",
                            "output": "new answer",
                        },
                    ),
                ],
            )
        ],
    ]
    chat = Chat(
        redis, conversation_id="abc", agent_path="pydantic_ai:Agent", model="test"
    )
    monkeypatch.setattr(chat, "send", MagicMock(return_value=("new", "4-0")))

    chat.run_once("Hello again")

    output = capsys.readouterr()
    assert output.out == "new answer\n"
    assert output.err == "earlier answer\n"
    redis.xack.assert_any_call(stream, "user", "2-0", "3-0")
    redis.xack.assert_any_call(stream, "user", "4-0", "5-0")
