"""
Agent import paths are the integration point for client applications.
"""

import sys
from datetime import datetime, timezone
from types import ModuleType
from unittest.mock import Mock

import pytest
from pydantic_ai import Agent, AgentSpec
from pydantic_ai.models.test import TestModel

from fallingwater.catalog import reify_agent
from fallingwater.demo import flaky_agent, haiku_master, time_agent


def test_load_existing_agent_from_both_path_forms() -> None:
    assert reify_agent("fallingwater.demo:haiku_master") is haiku_master
    assert reify_agent("fallingwater.demo.haiku_master") is haiku_master


def test_build_agent_from_application_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("fallingwater_test_agents")
    module.build = lambda: Agent.from_spec(
        AgentSpec.from_dict({"name": "test"})
    )
    monkeypatch.setitem(sys.modules, module.__name__, module)

    first = reify_agent("fallingwater_test_agents:build")
    second = reify_agent("fallingwater_test_agents:build")
    assert isinstance(first, Agent)
    assert first is not second


def test_reject_non_agent_import(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("fallingwater_test_invalid_agent")
    module.value = "hello"
    monkeypatch.setitem(sys.modules, module.__name__, module)

    with pytest.raises(TypeError, match="did not resolve"):
        reify_agent("fallingwater_test_invalid_agent:value")


def test_flaky_demo_agent_can_fail_or_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert reify_agent("fallingwater.demo:flaky_agent") is flaky_agent

    monkeypatch.setattr("fallingwater.demo.random", lambda: 0.0)
    with pytest.raises(RuntimeError, match="Flaky demo agent failed this turn"):
        flaky_agent.run_sync("hello", model=TestModel())

    monkeypatch.setattr("fallingwater.demo.random", lambda: 1.0)
    result = flaky_agent.run_sync("hello", model=TestModel())
    assert result.output


def test_time_demo_agent_calls_current_time_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert reify_agent("fallingwater.demo:time_agent") is time_agent
    current = datetime(2026, 11, 7, 22, 42, 39, tzinfo=timezone.utc)
    clock = Mock(wraps=datetime)
    clock.now.return_value = current
    monkeypatch.setattr("fallingwater.demo.time.datetime", clock)

    result = time_agent.run_sync("What time is it?", model=TestModel())

    clock.now.assert_called_once_with()
    assert current.astimezone().isoformat(timespec="seconds") in result.output
