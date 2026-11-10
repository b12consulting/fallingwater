"""A basic assistant with a current-time tool.

Try it with fw chat --agent fallingwater.demo:time_agent --model <model>.
"""

from datetime import datetime

from pydantic_ai import Agent, AgentSpec


time_agent = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "time",
            "instructions": (
                "You are a helpful assistant. Use the time tool when you need "
                "the current time. It returns the worker's local date and time "
                "with a UTC offset."
            ),
        }
    )
)


@time_agent.tool_plain
def time() -> str:
    """Return the current local date and time with its UTC offset."""
    return datetime.now().astimezone().isoformat(timespec="seconds")
