"""Example client-defined agents for trying Fallingwater integrations."""

from random import random

from pydantic_ai import Agent, AgentSpec, RunContext

from .jeopardy import jeopardy_contestant, jeopardy_host


haiku_master = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "haiku-master",
            "instructions": (
                "Answer every user message with a haiku of exactly three lines, "
                "aiming for a 5-7-5 syllable pattern. Put a Markdown hard line break "
                "(two spaces followed by a newline) after the first and second lines. "
                "Never join the lines with commas or slashes. Do not add a heading "
                "or explanation."
            ),
        }
    )
)


flaky_agent = Agent()


@flaky_agent.instructions
def sometimes_fail(ctx: RunContext[None]) -> None:
    """Draw once per turn, before its first model request."""
    if ctx.run_step == 1 and random() < 0.5:
        raise RuntimeError("Flaky demo agent failed this turn")
