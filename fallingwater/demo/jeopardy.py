"""
A Jeopardy host that delegates each clue to a separate contestant agent.

Start one or more workers to process the host and contestant turns::

    fw worker -w 2
    fw chat --agent fallingwater.demo:jeopardy_host --model openai:gpt-6-luna

Or print the host conversation's events until its turn finishes::

    FW_MODEL=openai:gpt-6-luna python fallingwater/demo/jeopardy.py

Set FW_REDIS_URL and FW_NAMESPACE consistently for clients and workers when
using a nondefault Redis URL or namespace.
"""

import os

from pydantic_ai import Agent, AgentSpec, RunContext

from fallingwater.agent_proxy import AgentRequest
from fallingwater.utils import poll


jeopardy_contestant = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "jeopardy-contestant",
            "instructions": (
                "You are a Jeopardy contestant. The user gives you a clue. "
                "Identify the person, place, thing, or term described by it. "
                "Phrase your response as a question, such as 'What is Paris?' "
                "Return only that response."
            ),
        }
    )
)


jeopardy_host = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "jeopardy-host",
            "instructions": (
                "You host a Jeopardy game. When given a theme, generate 3 "
                "distinct, factual clues related to it. "
                "Formulate each clue like an answer. Call ask_contestant once "
                "for each clue, passing one clue per call. "
                "After receiving the responses, present each clue "
                "with its contestant's response. Do not answer the clues "
                "yourself."
            ),
        }
    )
)


@jeopardy_host.tool
def ask_contestant(ctx: RunContext[None], clue: str) -> AgentRequest:
    """Request a separate contestant turn for one clue.

    Args:
        ctx: Context for the host's current turn.
        clue: A hint for the contestant to answer in question form.
    """
    if not clue.strip():
        raise ValueError("clue must be a nonempty string")
    model = (
        os.getenv("FW_MODEL") or f"{ctx.model.system}:{ctx.model.model_name}"
    )
    return AgentRequest(
        agent_path="fallingwater.demo:jeopardy_contestant",
        prompt=clue,
        model=model,
    )


if __name__ == "__main__":
    from redis import Redis

    from fallingwater.conversation_client import ConversationClient
    from fallingwater.models import Event

    with Redis.from_url(
        os.getenv("FW_REDIS_URL", "redis://localhost:6379/0")
    ) as redis:
        client = ConversationClient(
            redis,
            namespace=os.getenv("FW_NAMESPACE", "fw"),
            agent_path="fallingwater.demo:jeopardy_host",
            model=os.getenv("FW_MODEL"),
        )
        turn_id, _ = client.send("Start a Jeopardy game about science.")
        events = poll(client.read)
        for event in events:
            print(f"Event Type: {event.type}")
            client.acknowledge(event)
            if event.type is Event.TURN_COMPLETED:
                print(event.payload.message)
                break
