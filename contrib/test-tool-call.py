"""Compare automatic tool execution with returning a pending tool call."""

from pydantic_ai import (
    Agent,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
)
from pydantic_ai.capabilities import HandleDeferredToolCalls, Hooks


def multiplier(a: int, b: int) -> int:
    """Multiply two integers."""
    print(f"Executing multiplier({a}, {b})")
    return a * b


instructions = """
Use the multiplier tool for multiplication
yourself. Highlight the result like this: **result**"
"""

prompt = "What is 37 multiplied by 48?"


def approve_deferred_calls(
    ctx: RunContext[None], requests: DeferredToolRequests
) -> DeferredToolResults:
    """Approve deferred calls so Pydantic AI can execute their tools."""
    for tool_call in requests.approvals:
        print(f"Capability approving {tool_call.tool_name}({tool_call.args})")
    return requests.build_results(approve_all=True)


def run_automatic_example() -> None:
    # By default, the tool is executed and its result is fed back to the model.
    automatic_agent = Agent("ollama:gemma4", instructions=instructions)
    automatic_agent.tool_plain(multiplier)

    automatic_result = automatic_agent.run_sync(prompt)
    print("Automatic tool execution:")
    print(automatic_result.output)


def run_manual_approval_example() -> None:
    # Requiring approval makes run_sync return the requested tool call.
    deferred_agent = Agent(
        "ollama:gemma4",
        instructions=instructions,
    )
    deferred_agent.tool_plain(multiplier, requires_approval=True)

    deferred_result = deferred_agent.run_sync(
        prompt,
        output_type=str | DeferredToolRequests,
    )
    print("\nTool call deferred for approval:")
    if isinstance(deferred_result.output, DeferredToolRequests):
        for tool_call in deferred_result.output.approvals:
            print(f"{tool_call.tool_name}({tool_call.args})")

        # Approving the request lets Pydantic AI execute the registered tool.
        # Resume with its result so the model can produce a final response.
        approved_results = deferred_result.output.build_results(
            approve_all=True
        )
        resumed_result = deferred_agent.run_sync(
            message_history=deferred_result.all_messages(),
            deferred_tool_results=approved_results,
            output_type=str | DeferredToolRequests,
        )
        print("Model response after the deferred tool result:")
        print(resumed_result.output)
    else:
        print(f"The model did not request a tool: {deferred_result.output}")


def run_capability_example() -> None:
    # HandleDeferredToolCalls resolves approval requests inline.
    capability_agent = Agent(
        "ollama:gemma4",
        instructions=instructions,
    )
    capability_agent.tool_plain(multiplier, requires_approval=True)

    capability_result = capability_agent.run_sync(
        prompt,
        capabilities=[HandleDeferredToolCalls(handler=approve_deferred_calls)],
    )
    print("\nDeferred call handled by HandleDeferredToolCalls:")
    print(capability_result.output)


def auto_defer(ctx: RunContext[None], *, call, tool_def, args) -> None:
    print(f"Hook deferring {call.tool_name}({args})")
    raise CallDeferred(metadata={"execution": "caller"})


def run_hook_example() -> None:
    # A before_tool_execute hook can defer a call for caller-side execution.
    tool_hooks = Hooks()
    tool_hooks.on.before_tool_execute()(auto_defer)  # tools=["multiplier"]

    hook_agent = Agent(
        "ollama:gemma4",
        instructions=instructions,
    )
    hook_agent.tool_plain(multiplier)

    hook_result = hook_agent.run_sync(
        prompt,
        output_type=str | DeferredToolRequests,
        capabilities=[tool_hooks],
    )
    print("\nTool call deferred by the execution hook:")
    if isinstance(hook_result.output, DeferredToolRequests):
        requests = hook_result.output
        for tool_call in requests.calls:
            print(f"Deferred: {tool_call.tool_name}({tool_call.args})")

        # The caller executes deferred calls and submits their return values
        # when resuming. The agent does not run the registered tool body.
        call_results = {
            tool_call.tool_call_id: multiplier(**tool_call.args_as_dict())
            for tool_call in requests.calls
        }
        resumed_hook_result = hook_agent.run_sync(
            message_history=hook_result.all_messages(),
            deferred_tool_results=requests.build_results(calls=call_results),
            output_type=str | DeferredToolRequests,
            capabilities=[tool_hooks],
        )
        print("Model response after the caller supplied the tool result:")
        print(resumed_hook_result.output)
    else:
        print(f"The model did not request a tool: {hook_result.output}")


if __name__ == "__main__":
    # run_automatic_example()
    # run_manual_approval_example()
    # run_capability_example()
    run_hook_example()
