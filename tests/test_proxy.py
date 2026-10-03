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


def test_history_replays_deltas_after_legacy_snapshot() -> None:
    redis = MagicMock(spec=Redis)
    first_page = [
        ("1-0", {"type": EventType.CONVERSATION_CREATED, "agent": "example:agent"}),
        ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "turn-1", "prompt": "go"}),
    ]
    first_page.extend(
        (f"{index}-0", {"type": EventType.TURN_STARTED}) for index in range(3, 100)
    )
    first_page.append(
        (
            "100-0",
            {"type": EventType.TURN_COMPLETED, "history": _messages_json("first")},
        )
    )
    redis.xrange.side_effect = [
        first_page,
        [
            (
                "101-0",
                {
                    "type": EventType.TURN_COMPLETED,
                    "history": _messages_json("second"),
                    "history_format": "delta",
                },
            )
        ],
    ]

    agent_path, model, prompt, history = Proxy(redis)._load_turn(
        "fw:conversation:abc:events", "turn-1"
    )

    assert (agent_path, model, prompt) == ("example:agent", "", "go")
    assert history is not None
    assert [message.parts[0].content for message in history] == ["first", "second"]
    assert redis.xrange.call_args_list[1].kwargs["min"] == "(100-0"


def test_load_turn_selects_prompt_by_turn_id() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        ("1-0", {"type": EventType.CONVERSATION_CREATED, "agent": "example:agent"}),
        ("2-0", {"type": EventType.TURN_QUEUED, "turn_id": "first", "prompt": "one"}),
        ("3-0", {"type": EventType.TURN_QUEUED, "turn_id": "second", "prompt": "two"}),
    ]

    _, _, prompt, _ = Proxy(redis)._load_turn("fw:conversation:abc:events", "second")

    assert prompt == "two"


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
            {"type": EventType.TURN_QUEUED, "turn_id": "turn-1", "prompt": "hello"},
        ),
    ]
    result = MagicMock(output="reply")
    result.new_messages_json.return_value = _messages_json("hello").encode()
    agent = MagicMock()
    agent.run_sync.return_value = result
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)

    Proxy(redis)._process(
        {
            "turn_id": "turn-1",
            "conversation_id": "abc",
        }
    )

    agent.run_sync.assert_called_once()
    assert agent.run_sync.call_args.args == ("hello",)
    assert agent.run_sync.call_args.kwargs["model"] == "test"
    completed = redis.xadd.call_args_list[-1].args[1]
    assert completed["type"] == EventType.TURN_COMPLETED
    messages = ModelMessagesTypeAdapter.validate_json(completed["history"])
    assert messages[0].parts[0].content == "hello"
    assert completed["history_format"] == "delta"
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
            {"type": EventType.TURN_QUEUED, "turn_id": "turn-1", "prompt": "hello"},
        ),
    ]
    monkeypatch.setattr("fallingwater.demo.random", lambda: 0.0)

    with flaky_agent.override(model=TestModel()):
        Proxy(redis)._process(
            {
                "turn_id": "turn-1",
                "conversation_id": "abc",
            }
        )

    failed = redis.xadd.call_args_list[-1].args[1]
    assert failed == {
        "type": EventType.TURN_FAILED,
        "turn_id": "turn-1",
        "error": "Flaky demo agent failed this turn",
    }
