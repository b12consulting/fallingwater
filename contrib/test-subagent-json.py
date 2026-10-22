"""Show deferred subagent calls round-tripped through JSON serialization."""

from pydantic import TypeAdapter
from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
)
from pydantic_ai.capabilities import Hooks
from pydantic_ai_harness import SubAgent, SubAgents


SPECIALIST_NAME = "multiplication_specialist"
requests_adapter = TypeAdapter(DeferredToolRequests)
results_adapter = TypeAdapter(DeferredToolResults)


def round_trip_requests(requests: DeferredToolRequests) -> DeferredToolRequests:
    """Serialize and restore the deferred calls and their arguments."""
    payload = requests_adapter.dump_json(requests)
    print(f"Deferred requests JSON: {payload.decode()}")
    return requests_adapter.validate_json(payload)


def round_trip_results(results: DeferredToolResults) -> DeferredToolResults:
    """Serialize and restore the tool results before resuming an agent."""
    payload = results_adapter.dump_json(results)
    print(f"Deferred results JSON: {payload.decode()}")
    return results_adapter.validate_json(payload)


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
    """Run the specialist and handle JSON-round-tripped multiplier calls."""
    print(f"Running specialist for: {question}")
    result = specialist.run_sync(
        question,
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )

    while isinstance(result.output, DeferredToolRequests):
        requests = round_trip_requests(result.output)
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

        deferred_results = round_trip_results(
            requests.build_results(calls=call_results)
        )
        result = specialist.run_sync(
            message_history=result.all_messages(),
            deferred_tool_results=deferred_results,
            output_type=str | DeferredToolRequests,
            capabilities=[tool_hooks],
        )

    return str(result.output)


sub_agents = SubAgents(agents=[SubAgent(specialist)])
parent = Agent(
    "ollama:gemma4",
    instructions=(
        f"Answer arithmetic questions by delegating to {SPECIALIST_NAME} with "
        "delegate_task, then clearly report the specialist's answer."
    ),
    name="calculation_coordinator",
    capabilities=[sub_agents],
)


def run_subagent_json_example() -> None:
    """Round-trip deferred parent and specialist calls/results through JSON."""
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

    requests = round_trip_requests(deferred_result.output)
    call_results: dict[str, str] = {}
    for tool_call in requests.calls:
        print(f"Caller received: {tool_call.tool_name}({tool_call.args})")
        if tool_call.tool_name != "delegate_task":
            raise ValueError(f"Unexpected deferred tool: {tool_call.tool_name}")

        # Dispatch the restored delegation call. Its multiplier call is also
        # deferred, serialized, and supplied back to the specialist.
        args = tool_call.args_as_dict()
        if args["agent_name"] != SPECIALIST_NAME:
            raise ValueError(f"Unexpected subagent: {args['agent_name']}")
        call_results[tool_call.tool_call_id] = run_specialist(args["task"])

    deferred_results = round_trip_results(
        requests.build_results(calls=call_results)
    )
    resumed_result = parent.run_sync(
        message_history=deferred_result.all_messages(),
        deferred_tool_results=deferred_results,
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )
    print("Parent response after receiving the specialist's result:")
    print(resumed_result.output)


if __name__ == "__main__":
    run_subagent_json_example()
