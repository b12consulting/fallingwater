"""Example client-defined agents for trying Fallingwater integrations."""

from random import random

from pydantic import BaseModel
from pydantic_ai import Agent, AgentSpec, RunContext
import requests

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


class WeatherDeps(BaseModel):
    """Runtime configuration for the weather agent."""
    base_url: str
    default_city: str | None = None

    def get_url(self, city):
        url = self.base_url.rstrip("/")
        return f"{url}/{city}"


# example call:
#  fw chat -a fallingwater.demo:weather_agent base_url=https://wttr.in default_city=berlin --msg "how is the weather today"
weather_agent = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "weather",
            "instructions": (
                "You are a weather assistant. Use the weather tool to answer "
                "weather questions."
            ),
        }
    ),
    deps_type=WeatherDeps,
)


@weather_agent.tool
def get_weather(
    ctx: RunContext[WeatherDeps], city: str | None = None
) -> str:
    """Weather lookup tool that accepts an optional city"""
    if ctx.deps:
        city = city or ctx.deps.default_city
        url = ctx.deps.get_url(city)
        print("GET", url)
        resp = requests.get(url)
        return resp.text
    return "the weather is great"
