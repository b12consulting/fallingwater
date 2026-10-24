"""The host contestant tool returns nonblocking child-agent requests."""

from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from fallingwater.demo import jeopardy
from fallingwater.runner import SubAgentRequest


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

    assert result == SubAgentRequest(
        agent_path="fallingwater.demo:jeopardy_contestant",
        prompt="sport clue",
        model="test:test",
    )


def test_contestant_tool_uses_configured_child_model(monkeypatch) -> None:
    monkeypatch.setenv("FW_MODEL", "ollama:gemma4")

    result = jeopardy.ask_contestant(_context(), "sport clue")

    assert result.model == "ollama:gemma4"
