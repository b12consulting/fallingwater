"""Show a parent tool call that runs a specialist agent, deferred by a hook."""

from pydantic_ai import Agent, CallDeferred, DeferredToolRequests, RunContext
from pydantic_ai.capabilities import Hooks
from pydantic_ai_harness import SubAgent, SubAgents


SPECIALIST_NAME = "multiplication_specialist"


def multiplier(a: int, b: int) -> int:
    """Multiply two integers exactly."""
    print(f"Executing specialist multiplier({a}, {b})")
    return a * b


def auto_defer(ctx: RunContext[None], *, call, tool_def, args) -> None:
    """Defer tool execution so the caller can handle the call."""
    print(f"Hook deferring {call.tool_name}({args})")
    raise CallDeferred(metadata={"execution": "caller"})


tool_hooks = Hooks()
tool_hooks.on.before_tool_execute()(auto_defer)


specialist = Agent(
    "ollama:gemma4",
    instructions=(
        "You are a calculation specialist. Use the multiplier tool for every "
        "multiplication, then report the exact product."
    ),
    name=SPECIALIST_NAME,
)
specialist.tool_plain(multiplier)


def run_specialist(question: str) -> str:
    """Run the specialist and handle its deferred multiplier calls."""
    print(f"Running specialist for: {question}")
    result = specialist.run_sync(
        question,
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )

    # The specialist's multiplier call is also deferred by the hook. Dispatch
    # that call here, then resume the specialist with the exact product.
    while isinstance(result.output, DeferredToolRequests):
        requests = result.output
        call_results: dict[str, int] = {}
        for tool_call in requests.calls:
            print(f"Caller received: {tool_call.tool_name}({tool_call.args})")
            if tool_call.tool_name != "multiplier":
                raise ValueError(
                    f"Unexpected deferred tool: {tool_call.tool_name}"
                )
            call_results[tool_call.tool_call_id] = multiplier(
                **tool_call.args_as_dict()
            )

        result = specialist.run_sync(
            message_history=result.all_messages(),
            deferred_tool_results=requests.build_results(calls=call_results),
            output_type=str | DeferredToolRequests,
            capabilities=[tool_hooks],
        )

    return str(result.output)


sub_agents = SubAgents(
    agents=[SubAgent(specialist)],
)
parent = Agent(
    "ollama:gemma4",
    instructions=(
        f"Answer arithmetic questions by delegating to {SPECIALIST_NAME} with "
        "delegate_task, then clearly report the specialist's answer."
    ),
    name="calculation_coordinator",
    capabilities=[sub_agents],
)


def run_subagent_hook_example() -> None:
    """Defer a parent tool call, execute its subagent, and resume the parent."""
    prompt = "What is 37 multiplied by 48?"
    deferred_result = parent.run_sync(
        prompt,
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )

    if not isinstance(deferred_result.output, DeferredToolRequests):
        print(
            f"The parent did not request the specialist: {deferred_result.output}"
        )
        return

    requests = deferred_result.output
    call_results: dict[str, str] = {}
    for tool_call in requests.calls:
        print(f"Caller received: {tool_call.tool_name}({tool_call.args})")
        if tool_call.tool_name != "delegate_task":
            raise ValueError(f"Unexpected deferred tool: {tool_call.tool_name}")

        # The caller dispatches the deferred delegation to the named agent.
        # Its multiplier call is deferred by the same hook and handled above.
        args = tool_call.args_as_dict()
        if args["agent_name"] != SPECIALIST_NAME:
            raise ValueError(f"Unexpected subagent: {args['agent_name']}")
        call_results[tool_call.tool_call_id] = run_specialist(args["task"])

    resumed_result = parent.run_sync(
        message_history=deferred_result.all_messages(),
        deferred_tool_results=requests.build_results(calls=call_results),
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )
    print("Parent response after receiving the specialist's result:")
    print(resumed_result.output)


if __name__ == "__main__":
    run_subagent_hook_example()
