from pydantic_ai import Agent, AgentSpec

from fallingwater.agent_proxy import AgentRequest


writer = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "session-writer",
            "instructions": (
                "Draft short texts and revise your previous drafts on request. "
                "Preserve the user's factual details unless asked to change "
                "them. Return only the requested text."
            ),
        }
    )
)

editor = Agent.from_spec(
    AgentSpec.from_dict(
        {
            "name": "editor",
            "instructions": (
                "Answer user requests by delegating to one or more writers."
            ),
        }
    )
)


@editor.tool_plain
def ask_writer(prompt: str, name: str | None = None) -> AgentRequest:
    """Ask a persistent writer and receive its answer.

    Give different names to independent writers; otherwise its name is
    session-writer. Reuses an existing writer with the same name and keeps
    its history for follow-ups. Wait for its previous answer before asking
    for an ordered revision; overlapping requests can run in parallel.
    """
    return AgentRequest(
        agent_path="fallingwater.demo.editor_writers:writer",
        prompt=prompt,
        name=name,
        persistent=True,
    )
