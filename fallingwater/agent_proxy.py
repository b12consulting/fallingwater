"""Agent steps, tool invocation, and subagent resolution without storage I/O."""

import asyncio
import inspect
import json
from collections.abc import Sequence
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, TypeAlias
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import TypeAdapter
from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
)
from pydantic_ai.capabilities import Hooks
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ToolCallPart,
)
from pydantic_ai.tools import Tool
from pydantic_ai.usage import RunUsage

from fallingwater.conversation import Conversation
from fallingwater.models import AgentRequest as AgentRequest
from fallingwater.models import (
    AgentRequestedPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    TurnCompletedPayload,
)
from fallingwater.turn_state import TurnStateMachine, requests_adapter


ToolContexts: TypeAlias = dict[str, tuple[RunContext[Any], dict[str, Any]]]

_deferred_tool_calls: ContextVar[ToolContexts | None] = ContextVar(
    "fallingwater_deferred_tool_calls", default=None
)


def _defer_tool_execution(
    ctx: RunContext[Any], *, call, args, **_kwargs
) -> None:
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


@dataclass(frozen=True)
class AgentRunResult:
    """The output or deferred requests from one model run."""

    output: Any | DeferredToolRequests
    usage: RunUsage
    new_messages: list[ModelMessage]
    tool_contexts: ToolContexts


def validate_deps(agent: Agent, values: dict[str, Any]) -> Any:
    """Restore serialized dependencies according to the agent's deps type."""
    if not values and agent.deps_type is type(None):
        return None
    return TypeAdapter(agent.deps_type).validate_python(values)


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


def invoke_tool(
    agent: Agent,
    tool_name: str,
    args: dict[str, Any],
    ctx: RunContext[Any] | None = None,
) -> Any:
    """Invoke an attached tool with validated arguments and optional context."""
    return _execute_tool(_find_attached_tool(agent, tool_name), args, ctx)


def execute_tool(
    agent: Agent,
    call: ToolCallPart,
    *,
    prompt: str,
    messages: Sequence[ModelMessage],
    usage: RunUsage,
    deps: Any = None,
    model: Any = None,
    conversation_id: str | None = None,
) -> Any:
    """Restore tool context; historical messages and parts are read-only."""

    tool = _find_attached_tool(agent, call.tool_name)
    args = call.args
    if isinstance(args, str):
        args = json.loads(args)
    if not isinstance(args, dict):
        raise ValueError(f"Invalid arguments for tool {call.tool_name!r}")
    ctx = (
        RunContext(
            deps=deps,
            model=agent._get_model_outside_run(model),
            usage=usage,
            agent=agent,
            prompt=prompt,
            messages=list(messages),
            tool_call_id=call.tool_call_id,
            tool_name=call.tool_name,
            conversation_id=conversation_id,
        )
        if tool.takes_ctx
        else None
    )
    return invoke_tool(agent, call.tool_name, args, ctx)


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
    Historical messages and their parts are shared and must not be mutated.
    """
    captured_calls: ToolContexts = {}
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


@dataclass(frozen=True)
class AgentStepOutcome:
    payload: ToolRequestedPayload | TurnCompletedPayload
    tool_contexts: ToolContexts


def run_agent_step(
    agent: Agent,
    turn: TurnStateMachine,
    *,
    prompt: str | None,
    deps: Any = None,
    model: Any = None,
) -> AgentStepOutcome:
    """Run one model step and propose its event without advancing the turn."""
    requests = turn.requests
    result = run(
        agent,
        prompt,
        message_history=[*turn.history, *turn.turn_messages],
        deferred_tool_results=(
            requests.build_results(
                calls=turn.tool_results, metadata=requests.metadata
            )
            if requests is not None
            else None
        ),
        deps=deps,
        model=model,
        usage=deepcopy(turn.usage),
        conversation_id=turn.conversation_id,
    )
    payload: ToolRequestedPayload | TurnCompletedPayload
    if isinstance(result.output, DeferredToolRequests):
        payload = ToolRequestedPayload(
            turn_id=turn.turn_id,
            step_id=str(uuid4()),
            requests=requests_adapter.dump_python(result.output, mode="json"),
            step_messages=ModelMessagesTypeAdapter.dump_python(
                result.new_messages, mode="json"
            ),
            usage=result.usage,
        )
    else:
        payload = TurnCompletedPayload(
            turn_id=turn.turn_id,
            message=str(result.output),
            turn_messages=ModelMessagesTypeAdapter.dump_python(
                [*turn.turn_messages, *result.new_messages], mode="json"
            ),
            usage=result.usage,
        )
    return AgentStepOutcome(payload, result.tool_contexts)


def resolve_subagent(
    conversation: Conversation,
    action: ToolCallPayload,
    request: AgentRequest,
    *,
    default_name: str | None = None,
) -> AgentRequestedPayload:
    """Resolve a delegation without loading agents or registering a session.

    Callers supply the definition's default name. Child session and turn IDs
    are deterministic for the requesting tool call, including named reuse.
    """
    name = request.name
    if request.persistent and name is None:
        name = default_name
    if request.persistent and (name is None or not name.strip()):
        raise ValueError("Persistent agent requires a name")
    registered = (
        conversation.agent_by_name.get(name)
        if request.persistent and name is not None
        else None
    )
    if registered is not None and name is not None:
        previous = conversation.agent_spec(name)
        if (
            request.agent_path is not None
            and request.agent_path != previous.agent_path
        ):
            raise ValueError("Registered agent has a different agent_path")
        if request.model is not None and request.model != previous.model:
            raise ValueError("Registered agent has a different model")
        if "deps" in request.model_fields_set and request.deps != previous.deps:
            raise ValueError("Registered agent has different dependencies")
        agent_path = previous.agent_path
        model = previous.model
        deps = previous.deps
        child_id = registered
    else:
        if request.agent_path is None:
            raise ValueError(f"No registered agent named {name!r}")
        agent_path = request.agent_path
        model = request.model or conversation.settings.model
        deps = request.deps
        child_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "fallingwater",
                        conversation.conversation_id,
                        action.turn_id,
                        action.step_id,
                        action.tool_call_id,
                    )
                ),
            )
        )
    return AgentRequestedPayload(
        turn_id=action.turn_id,
        step_id=action.step_id,
        tool_call_id=action.tool_call_id,
        child_conversation_id=child_id,
        child_turn_id=str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "fallingwater",
                        child_id,
                        "turn",
                        conversation.conversation_id,
                        action.turn_id,
                        action.step_id,
                        action.tool_call_id,
                    )
                ),
            )
        ),
        agent_path=agent_path,
        model=model,
        deps=deps,
        prompt=request.prompt,
        name=name,
        persistent=request.persistent,
    )
