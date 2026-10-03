"""Agent import paths are the integration point for client applications."""

import sys
from types import ModuleType

import pytest
from pydantic_ai import Agent, AgentSpec

from fallingwater.catalog import reify_agent
from fallingwater.demo import haiku_master


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
