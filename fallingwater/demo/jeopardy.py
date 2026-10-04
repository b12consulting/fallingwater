"""A Jeopardy host whose tool runs independent contestants through Fallingwater.

Start at least two proxies so one can process contestants while the host waits
for its tool call::

    fw worker -p 2
    fw chat --agent fallingwater.demo:jeopardy_host --model openai:gpt-6-luna

If the worker uses a nondefault Redis URL or namespace, set FW_REDIS_URL and
FW_NAMESPACE in its environment so this tool submits to the same Redis keys.
"""

import os
from uuid import uuid4

from pydantic_ai import Agent, AgentSpec, RunContext
from redis import Redis

from fallingwater.chat import Chat


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
                "You host a Jeopardy game. When given a theme, generate three "
                "distinct, factual clues related to it. Each clue should hint "
                "at a specific person, place, thing, or term without naming it. "
                "The clue should be formulated like an answer. "
                "Submit all three clues in one call to ask_contestants. The tool "
                "sends each clue to a different contestant. Wait for the tool, "
                "then present each clue and its contestant's response to the "
                "user. Do not answer the clues yourself. Include each contestant "
                "conversation ID so the user can inspect its events."
            ),
        }
    )
)


@jeopardy_host.tool
def ask_contestants(ctx: RunContext[None], clues: list[str]) -> list[dict[str, str]]:
    """Submit each clue to a separate contestant and collect the responses.

    Args:
        ctx: Context for the host's current turn.
        clues: The hints, one per contestant, to answer in question form.
    """
    if not clues or any(not clue.strip() for clue in clues):
        raise ValueError("clues must be a nonempty list of nonempty strings")

    host_id = ctx.conversation_id or str(uuid4())
    model = os.getenv("FW_MODEL") or f"{ctx.model.system}:{ctx.model.model_name}"
    redis_url = os.getenv("FW_REDIS_URL", "redis://localhost:6379/0")
    namespace = os.getenv("FW_NAMESPACE", "fw")

    responses = []
    with Redis.from_url(redis_url, decode_responses=True) as redis:
        for clue in clues:
            contestant_id = f"jeopardy_contestant_{host_id}_{uuid4()}"
            chat = Chat(
                redis,
                namespace=namespace,
                conversation_id=contestant_id,
                agent_path="fallingwater.demo:jeopardy_contestant",
                model=model,
            )
            responses.append(
                {
                    "clue": clue,
                    "response": chat.ask(clue),
                    "contestant_conversation_id": contestant_id,
                }
            )

    return responses
