"""Checks for loading and processing turns from conversation events."""

from unittest.mock import MagicMock

import pytest
from pydantic_ai.messages import ModelMessagesTypeAdapter, ModelRequest, UserPromptPart
from pydantic_ai.models.test import TestModel
from redis import Redis

from fallingwater.demo import flaky_agent
from fallingwater.proxy import Proxy
from fallingwater.streams import EventType


def _messages_json(prompt: str) -> str:
    messages = [ModelRequest(parts=[UserPromptPart(content=prompt)])]
    return ModelMessagesTypeAdapter.dump_json(messages).decode()


def test_completed_turn_stores_only_new_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        (
            "1-0",
            {
                "type": EventType.CONVERSATION_CREATED,
                "agent": "example:agent",
                "model": "test",
            },
        ),
        (
            "2-0",
            {"type": EventType.TURN_QUEUED, "turn_id": "turn-1", "message": "hello"},
        ),
    ]
    result = MagicMock(output="reply")
    result.new_messages_json.return_value = _messages_json("hello").encode()
    agent = MagicMock()
    agent.run_sync.return_value = result
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)

    Proxy(redis).process(
        {
            "turn_id": "turn-1",
            "conversation_id": "abc",
        }
    )

    agent.run_sync.assert_called_once()
    assert agent.run_sync.call_args.args == ("hello",)
    assert agent.run_sync.call_args.kwargs["model"] == "test"
    assert redis.xadd.call_count == 1
    completed = redis.xadd.call_args_list[-1].args[1]
    assert completed["type"] == EventType.TURN_COMPLETED
    messages = ModelMessagesTypeAdapter.validate_json(completed["history"])
    assert messages[0].parts[0].content == "hello"
    assert completed["message"] == "reply"
    assert "history_format" not in completed
    result.all_messages_json.assert_not_called()


def test_flaky_demo_failure_is_recorded_as_turn_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        (
            "1-0",
            {
                "type": EventType.CONVERSATION_CREATED,
                "agent": "fallingwater.demo:flaky_agent",
                "model": "",
            },
        ),
        (
            "2-0",
            {"type": EventType.TURN_QUEUED, "turn_id": "turn-1", "message": "hello"},
        ),
    ]
    monkeypatch.setattr("fallingwater.demo.random", lambda: 0.0)

    with flaky_agent.override(model=TestModel()):
        Proxy(redis).process(
            {
                "turn_id": "turn-1",
                "conversation_id": "abc",
            }
        )

    assert redis.xadd.call_count == 1
    failed = redis.xadd.call_args_list[-1].args[1]
    assert failed == {
        "type": EventType.TURN_FAILED,
        "turn_id": "turn-1",
        "error": "Flaky demo agent failed this turn",
    }
