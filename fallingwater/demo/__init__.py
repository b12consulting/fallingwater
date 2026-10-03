"""Example client-defined agents for trying Fallingwater integrations."""

from pydantic_ai import Agent, AgentSpec


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
