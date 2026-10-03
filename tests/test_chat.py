"""Checks for submitting a turn to the conversation and dispatch streams."""

from unittest.mock import MagicMock, call

import pytest
from redis import Redis

from fallingwater.chat import Chat
from fallingwater.streams import EventType


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
