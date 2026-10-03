"""Agent import paths are the integration point for client applications."""

import sys
from types import ModuleType

import pytest
from pydantic_ai import Agent, AgentSpec
from pydantic_ai.models.test import TestModel

from fallingwater.catalog import reify_agent
from fallingwater.demo import flaky_agent, haiku_master


def test_load_existing_agent_from_both_path_forms() -> None:
    assert reify_agent("fallingwater.demo:haiku_master") is haiku_master
    assert reify_agent("fallingwater.demo.haiku_master") is haiku_master


def test_build_agent_from_application_function(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("fallingwater_test_agents")
    module.build = lambda: Agent.from_spec(AgentSpec.from_dict({"name": "test"}))
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
