"""
A Jeopardy host that delegates each clue to a separate contestant agent.

Start at least two proxies so one can process contestants while the host waits
for its tool call::

    fw worker -p 2
    fw chat --agent fallingwater.demo:jeopardy_host --model openai:gpt-6-luna

If the worker uses a nondefault Redis URL or namespace, set FW_REDIS_URL and
FW_NAMESPACE in its environment so this tool submits to the same Redis keys.
"""

import os
from uuid import NAMESPACE_URL, uuid5

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
                "Formulate each clue like an answer. Call ask_contestant once "
                "for each clue, passing one clue per call. After receiving the "
                "responses, present each clue with its contestant's response. "
                "Do not answer the clues yourself."
            ),
        }
    )
)


@jeopardy_host.tool
def ask_contestant(ctx: RunContext[None], clue: str) -> str:
    """Submit one clue to a contestant and return their response.

    Args:
        ctx: Context for the host's current turn.
        clue: A hint for the contestant to answer in question form.
    """
    if not clue.strip():
        raise ValueError("clue must be a nonempty string")
    model = os.getenv("FW_MODEL") or f"{ctx.model.system}:{ctx.model.model_name}"
    redis_url = os.getenv("FW_REDIS_URL", "redis://localhost:6379/0")
    namespace = os.getenv("FW_NAMESPACE", "fw")

    parent_call_id = ctx.tool_call_id or "direct-call"
    child_conversation_id = str(
        uuid5(
            NAMESPACE_URL,
            f"fallingwater:{ctx.conversation_id}:{parent_call_id}",
        )
    )
    with Redis.from_url(redis_url) as redis:
        chat = Chat(
            redis,
            namespace=namespace,
            conversation_id=child_conversation_id,
            agent_path="fallingwater.demo:jeopardy_contestant",
            model=model,
        )
        return chat.ask(clue, idempotency_key="contestant-turn")
