"""Demo tools return nonblocking agent requests."""

from runpy import run_path
from unittest.mock import MagicMock

import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from fallingwater.demo import editor, editor_writers, jeopardy
from fallingwater.agent_proxy import AgentRequest, _find_attached_tool
from fallingwater.catalog import reify_agent
from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationEvent,
    TurnCompletedPayload,
    TurnQueuedPayload,
    get_cnv_event,
)


def _context() -> RunContext[None]:
    return RunContext(
        deps=None,
        model=TestModel(),
        usage=RunUsage(),
        conversation_id="host-conversation",
        tool_call_id="parent-tool-call",
    )


def test_contestant_tool_returns_a_child_request(monkeypatch) -> None:
    monkeypatch.delenv("FW_MODEL", raising=False)

    result = jeopardy.ask_contestant(_context(), "sport clue")

    assert result == AgentRequest(
        agent_path="fallingwater.demo:jeopardy_contestant",
        prompt="sport clue",
        model="test:test",
    )


def test_contestant_tool_uses_configured_child_model(monkeypatch) -> None:
    monkeypatch.setenv("FW_MODEL", "ollama:gemma4")

    result = jeopardy.ask_contestant(_context(), "sport clue")

    assert result.model == "ollama:gemma4"


@pytest.mark.parametrize("configured_redis", [False, True])
@pytest.mark.parametrize("completed_turn_id", ["host-turn", "other"])
def test_jeopardy_main_prints_event_types_until_first_completed_turn(
    monkeypatch, capsys, configured_redis, completed_turn_id
) -> None:
    redis_url = (
        "redis://example:6379/1"
        if configured_redis
        else "redis://localhost:6379/0"
    )
    namespace = "fw-test" if configured_redis else "fw"
    for name, value in (
        ("FW_REDIS_URL", redis_url),
        ("FW_NAMESPACE", namespace),
    ):
        if configured_redis:
            monkeypatch.setenv(name, value)
        else:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FW_MODEL", "test")
    connection = MagicMock()
    from_url = MagicMock(return_value=connection)
    client = MagicMock()
    client.send.return_value = "host-turn"
    payloads = [
        ConversationCreatedPayload(
            agent="fallingwater.demo:jeopardy_host", model="test"
        ),
        TurnQueuedPayload(
            turn_id="host-turn", message="Start a Jeopardy game about science."
        ),
        TurnQueuedPayload(turn_id="other", message="Other prompt"),
        TurnCompletedPayload(
            turn_id=completed_turn_id,
            message="Clues and contestant responses",
            turn_messages=[],
            usage=RunUsage(),
        ),
        TurnQueuedPayload(turn_id="later", message="Another prompt"),
    ]
    events = [
        ConversationEvent(
            entry_id=f"{index}-0",
            type=get_cnv_event(type(payload)),
            payload=payload,
        )
        for index, payload in enumerate(payloads, 1)
    ]
    client.read.side_effect = [events[:2], [], events[2:]]
    acknowledged = []

    def acknowledge(event):
        assert capsys.readouterr().out == f"Event Type: {event.type}\n"
        acknowledged.append(event)

    client.acknowledge.side_effect = acknowledge
    client_class = MagicMock(return_value=client)
    monkeypatch.setattr("redis.Redis.from_url", from_url)
    monkeypatch.setattr("fallingwater.redis_client.RedisClient", client_class)

    run_path(jeopardy.__file__, run_name="__main__")

    from_url.assert_called_once_with(redis_url)
    client_class.assert_called_once_with(
        connection.__enter__.return_value,
        namespace=namespace,
        agent_path="fallingwater.demo:jeopardy_host",
        model="test",
    )
    client.send.assert_called_once_with("Start a Jeopardy game about science.")
    client.ask.assert_not_called()
    assert acknowledged == events[:-1]
    assert capsys.readouterr().out == "Clues and contestant responses\n"
    assert client.read.call_count == 3
    connection.__exit__.assert_called_once()


def test_editor_writer_tool_creates_and_continues_named_sessions() -> None:
    assert (
        reify_agent("fallingwater.demo:editor")
        is editor
        is editor_writers.editor
    )
    assert (
        reify_agent("fallingwater.demo.editor_writers:editor")
        is editor_writers.editor
    )
    assert (
        reify_agent("fallingwater.demo.editor_writers:writer")
        is editor_writers.writer
    )
    created = editor_writers.ask_writer("draft an invitation", name="alice")
    assert created == AgentRequest(
        agent_path="fallingwater.demo.editor_writers:writer",
        prompt="draft an invitation",
        name="alice",
        persistent=True,
    )
    assert editor_writers.ask_writer(
        "make it warmer", name="alice"
    ) == AgentRequest(
        agent_path="fallingwater.demo.editor_writers:writer",
        name="alice",
        prompt="make it warmer",
        persistent=True,
    )
    assert editor_writers.ask_writer("draft").persistent
    tool = _find_attached_tool(editor, "ask_writer")
    assert "persistent" not in tool.function_schema.json_schema["properties"]
