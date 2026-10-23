"""
Resolve application-defined Pydantic AI agents by Python import path.
"""

from collections.abc import Mapping
from functools import lru_cache
from importlib import import_module
from typing import Any, Callable

from pydantic_ai import Agent, AgentSpec


@lru_cache(maxsize=128)
def _resolve_import(path: str) -> object:
    """
    Cache import-path resolution without caching factory results.
    """
    if ":" in path:
        module_name, attribute_path = path.split(":", 1)
    else:
        module_name, separator, attribute_path = path.rpartition(".")
        if not separator:
            raise ValueError("agent path must be module:object or module.object")
    if not module_name or not attribute_path:
        raise ValueError("agent path must be module:object or module.object")

    value: object = import_module(module_name)
    for attribute in attribute_path.split("."):
        value = getattr(value, attribute)
    return value


def reify_tool_executors(
    path: str,
) -> Mapping[str, Mapping[str, Callable[..., Any]]]:
    """Load the optional caller-side deferred-tool registry beside an agent."""
    module_name = (
        path.split(":", 1)[0]
        if ":" in path
        else path.rpartition(".")[0]
    )
    if not module_name:
        raise ValueError("agent path must be module:object or module.object")
    module = import_module(module_name)
    executors = getattr(module, "tool_executors_by_agent", {})
    if not isinstance(executors, Mapping):
        raise TypeError("tool_executors_by_agent must be a mapping")
    for agent_name, agent_executors in executors.items():
        if not isinstance(agent_name, str) or not isinstance(
            agent_executors, Mapping
        ):
            raise TypeError(
                "tool_executors_by_agent must map agent names to tool mappings"
            )
        for tool_name, executor in agent_executors.items():
            if not isinstance(tool_name, str) or not callable(executor):
                raise TypeError(
                    "tool executor mappings must map tool names to callables"
                )
    return executors


def reify_agent(path: str) -> Agent:
    """
    Load ``module:object`` or ``module.object`` as an Agent.

    The object may be an Agent, an AgentSpec, or a zero-argument function that
    returns either. The function is called for each turn.
    """
    value = _resolve_import(path)
    if callable(value) and not isinstance(value, Agent):
        value = value()
    if isinstance(value, AgentSpec):
        value = Agent.from_spec(value)
    if not isinstance(value, Agent):
        raise TypeError(f"{path!r} did not resolve to a Pydantic AI Agent")
    return value
