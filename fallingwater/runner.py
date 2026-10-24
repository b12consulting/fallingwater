"""Run one Pydantic AI agent step while deferring attached tools."""

import asyncio
import inspect
from collections.abc import Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
)
from pydantic_ai.capabilities import Hooks
from pydantic_ai.messages import ModelMessage
from pydantic_ai.tools import Tool
from pydantic_ai.usage import RunUsage


_deferred_tool_calls: ContextVar[
    dict[str, tuple[RunContext[Any], dict[str, Any]]] | None
] = ContextVar("fallingwater_deferred_tool_calls", default=None)


def _defer_tool_execution(ctx: RunContext[Any], *, call, args, **_kwargs) -> None:
    """Save each tool's runtime context, then return execution to the caller."""
    deferred_calls = _deferred_tool_calls.get()
    if deferred_calls is None:
        raise RuntimeError(
            "Deferred tool execution ran outside the Fallingwater runner"
        )
    deferred_calls[call.tool_call_id] = (ctx, dict(args))
    raise CallDeferred(metadata={"tool_name": call.tool_name, "args": args})


_tool_hooks = Hooks()
_tool_hooks.on.before_tool_execute()(_defer_tool_execution)


class SubAgentRequest(BaseModel):
    """Serializable request for a nonblocking child-agent turn."""

    model_config = ConfigDict(extra="forbid")

    agent_path: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    model: str | None = None
    deps: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True)
class AgentRunResult:
    """The output or deferred requests from one model run."""

    output: Any | DeferredToolRequests
    usage: RunUsage
    new_messages: list[ModelMessage]
    tool_contexts: dict[str, tuple[RunContext[Any], dict[str, Any]]]


def _find_attached_tool(agent: Agent, tool_name: str) -> Tool[Any]:
    """Return a directly attached Pydantic AI function tool by its name."""
    for toolset in agent.toolsets:
        tools = getattr(toolset, "tools", {})
        tool = tools.get(tool_name)
        if isinstance(tool, Tool):
            return tool
    raise ValueError(
        f"No directly attached function tool named {tool_name!r} "
        f"on agent {agent.name or '<unnamed>'!r}"
    )


def _execute_tool(
    tool: Tool[Any], args: dict[str, Any], ctx: RunContext[Any] | None
) -> Any:
    """Invoke an attached function using its Pydantic-validated arguments."""
    function_schema = tool.function_schema
    kwargs = dict(args)
    if tool.takes_ctx and ctx is None:
        raise ValueError(f"Tool {tool.name!r} requires a RunContext")
    positional_args = [ctx] if tool.takes_ctx else []
    for name in function_schema.positional_fields:
        positional_args.append(kwargs.pop(name))
    if function_schema.var_positional_field:
        positional_args.extend(kwargs.pop(function_schema.var_positional_field))

    result = tool.function(*positional_args, **kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(_await_result(result))
    return result


async def _await_result(result: Any) -> Any:
    return await result


def run(
    agent: Agent,
    prompt: str | None,
    *,
    message_history: Sequence[ModelMessage] | None = None,
    deferred_tool_results: DeferredToolResults | None = None,
    deps: Any = None,
    model: Any = None,
    usage: RunUsage | None = None,
    conversation_id: str | None = None,
) -> AgentRunResult:
    """Execute exactly one ``Agent.run_sync`` call.

    Attached function tools are deferred so the caller can persist the request,
    execute its calls, and resume the agent in a later invocation.
    """
    captured_calls: dict[str, tuple[RunContext[Any], dict[str, Any]]] = {}
    calls_token = _deferred_tool_calls.set(captured_calls)
    try:
        result = agent.run_sync(
            prompt,
            message_history=list(message_history or []),
            deferred_tool_results=deferred_tool_results,
            output_type=[agent.output_type, DeferredToolRequests],
            capabilities=[_tool_hooks],
            deps=deps,
            model=model,
            usage=usage,
            conversation_id=conversation_id,
        )
    finally:
        _deferred_tool_calls.reset(calls_token)

    return AgentRunResult(
        output=result.output,
        usage=result.usage,
        new_messages=result.new_messages(),
        tool_contexts=captured_calls,
    )
