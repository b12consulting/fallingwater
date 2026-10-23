"""Run nested agents and caller-executed tools through one JSON-aware runner."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextvars import ContextVar
from typing import Any

from pydantic import TypeAdapter
from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
)
from pydantic_ai.capabilities import Hooks


SPECIALIST_NAME = "multiplication_specialist"
COORDINATOR_NAME = "calculation_coordinator"
MAX_AGENT_DEPTH = 3
MAX_TOOL_ROUNDS = 20
_agent_depth_context: ContextVar[int] = ContextVar("agent_depth", default=1)

requests_adapter = TypeAdapter(DeferredToolRequests)
results_adapter = TypeAdapter(DeferredToolResults)


def round_trip_requests(requests: DeferredToolRequests) -> DeferredToolRequests:
    """Serialize and restore a deferred request and its tool calls."""
    payload = requests_adapter.dump_json(requests)
    print(f"\tDeferred requests JSON: {payload.decode()}")
    return requests_adapter.validate_json(payload)


def round_trip_results(results: DeferredToolResults) -> DeferredToolResults:
    """Serialize and restore results before passing them back to an agent."""
    payload = results_adapter.dump_json(results)
    print(f"\tDeferred results JSON: {payload.decode()}")
    return results_adapter.validate_json(payload)


def multiplier(a: int, b: int) -> int:
    """Multiply two integers exactly."""
    print(f"\tExecuting specialist multiplier({a}, {b})")
    return a * b


def auto_defer(ctx: RunContext[None], *, call, tool_def, args) -> None:
    """Defer tool execution so the recursive runner can dispatch the call."""
    print(f"\tHook deferring {call.tool_name}({args})")
    raise CallDeferred(metadata={"execution": "caller"})


tool_hooks = Hooks()
tool_hooks.on.before_tool_execute()(auto_defer)


specialist_agent = Agent(
    "ollama:gemma4",
    instructions=(
        "You are a calculation specialist. Use the multiplier tool for every "
        "multiplication, then report the exact product."
    ),
    name=SPECIALIST_NAME,
)
specialist_agent.tool_plain(multiplier)


def make_delegate_tool(
    subagents: Mapping[str, Agent]
) -> Callable[[str, str], str]:
    """Create a regular tool that runs one of the configured subagents."""

    def delegate_task(agent_name: str, task: str) -> str:
        agent_depth = _agent_depth_context.get()
        if agent_depth >= MAX_AGENT_DEPTH:
            raise RuntimeError(
                f"Delegation exceeded maximum agent depth {MAX_AGENT_DEPTH}"
            )
        try:
            subagent = subagents[agent_name]
        except KeyError as exc:
            raise ValueError(f"Unknown subagent: {agent_name}") from exc

        print(f"\tDelegating to {agent_name}: {task}")
        return str(
            run_until_complete(
                subagent,
                task,
                agent_depth=agent_depth + 1,
            )
        )

    return delegate_task


delegate_task = make_delegate_tool(
    {SPECIALIST_NAME: specialist_agent},
)

coordinator_agent = Agent(
    "ollama:gemma4",
    instructions=(
        f"Answer arithmetic questions by calling delegate_task with agent_name="
        f"'{SPECIALIST_NAME}' and the task. Then clearly report the result."
    ),
    name=COORDINATOR_NAME,
)
coordinator_agent.tool_plain(delegate_task)


def resolve_agent_tool(agent: Agent, tool_name: str) -> Callable[..., Any]:
    """Find a directly attached, context-free function tool by its name."""
    for toolset in agent.toolsets:
        tools = getattr(toolset, "tools", {})
        if tool_name in tools:
            tool = tools[tool_name]
            if tool.takes_ctx:
                raise NotImplementedError(
                    f"Cannot caller-execute context-taking tool {tool_name!r}"
                )
            breakpoint()
            return tool.function
    raise ValueError(
        f"No directly attached function tool named {tool_name!r} "
        f"on agent {agent.name!r}"
    )


def run_until_complete(
    agent: Agent,
    prompt: str,
    *,
    agent_depth: int = 1,
) -> Any:
    """Run an agent, dispatch deferred tools or subagents, and resume it.

    Deferred calls are matched to function tools attached to the agent. This
    example supports context-free tools; a tool requiring ``RunContext`` needs
    an execution adapter.
    """
    if agent.name is None:
        raise ValueError("Agents handled by this runner must have a name")

    deferred_output_type = [agent.output_type, DeferredToolRequests]
    rounds = 0
    message_history = []
    deferred_tool_results = None
    current_prompt = prompt

    while True:
        result = agent.run_sync(
            current_prompt,
            message_history=message_history,
            deferred_tool_results=deferred_tool_results,
            output_type=deferred_output_type,
            capabilities=[tool_hooks],
        )
        current_prompt = None

        if not isinstance(result.output, DeferredToolRequests):
            break

        rounds += 1
        if rounds > MAX_TOOL_ROUNDS:
            raise RuntimeError(
                f"{agent.name} exceeded {MAX_TOOL_ROUNDS} tool rounds"
            )

        requests = round_trip_requests(result.output)
        if requests.approvals:
            raise NotImplementedError(
                "This runner handles hook-deferred calls, not approval requests"
            )

        call_results: dict[str, Any] = {}
        for tool_call in requests.calls:
            print(f"\tCaller received: {tool_call.tool_name}({tool_call.args})")
            tool = resolve_agent_tool(agent, tool_call.tool_name)
            depth_token = _agent_depth_context.set(agent_depth)
            try:
                value = tool(**tool_call.args_as_dict())
            finally:
                _agent_depth_context.reset(depth_token)

            call_results[tool_call.tool_call_id] = value

        deferred_tool_results = round_trip_results(
            requests.build_results(calls=call_results)
        )
        message_history = result.all_messages()

    return result.output


def run_recursive_runner_example() -> None:
    """Ask the coordinator to compute a product through its specialist."""
    answer = run_until_complete(
        coordinator_agent,
        "What is 37 multiplied by 48?",
    )
    print("\nFinal response:")
    print(answer)


if __name__ == "__main__":
    run_recursive_runner_example()
