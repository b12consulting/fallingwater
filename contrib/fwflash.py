"""Single-deque agent executor backed by in-memory Conversation events.

Install with ``pip install -e '.[lite]'``. Omit the prompt for interactive use::

    python contrib/fwflash.py --model openai:gpt-6-luna "Hello"
    python contrib/fwflash.py --model openai:gpt-6-luna \
        --agent fallingwater.demo:editor

Conversation validates every new turn event before applying it. Sessions run
one turn at a time; all sessions share one FIFO work deque. Exceptions stop
execution, and nothing survives process exit. There is no Redis or recovery.
"""

from __future__ import annotations

import argparse
import os
from collections import deque
from dataclasses import dataclass, field
from itertools import count
from typing import Any
from uuid import uuid4

from pydantic_ai import Agent

from fallingwater.agent_proxy import (
    ToolContexts,
    invoke_tool,
    resolve_subagent,
    run_agent_step,
    validate_deps,
)
from fallingwater.catalog import reify_agent
from fallingwater.conversation import Conversation
from fallingwater.models import (
    AgentRequest,
    AgentRequestedPayload,
    AgentResumePayload,
    ConversationCreatedPayload,
    ConversationEvent,
    ConversationPayloadType,
    ConversationPokePayload,
    DispatchPayloadType,
    SubAgentResultPayload,
    ToolCallPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnQueuedPayload,
    get_cnv_event,
)

if __package__:
    from .fwlite import print_trace
else:
    from fwlite import print_trace


@dataclass
class Session:
    agent: Agent
    conversation: Conversation
    deps: Any = None
    pending: list[TurnQueuedPayload] = field(default_factory=list)
    events: dict[str, ConversationEvent] = field(default_factory=dict)
    tool_contexts: ToolContexts = field(default_factory=dict)


class Executor:
    def __init__(self) -> None:
        self.registry: dict[str, Session] = {}
        self.queue: deque[DispatchPayloadType] = deque()
        self.event_ids = count(1)

    def create(
        self,
        agent: str,
        *,
        model: str | None = None,
        deps: dict[str, Any] | None = None,
    ) -> str:
        return self._create_session(
            reify_agent(agent),
            ConversationCreatedPayload(
                agent=agent, model=model or "", deps=deps or {}
            ),
            str(uuid4()),
        )

    def _create_session(
        self,
        agent: Agent,
        settings: ConversationCreatedPayload,
        session_id: str,
    ) -> str:
        self.registry[session_id] = Session(
            agent,
            Conversation(conversation_id=session_id),
            validate_deps(agent, settings.deps),
        )
        self.record(session_id, settings)
        return session_id

    def send(self, session_id: str, prompt: str) -> str:
        queued = TurnQueuedPayload(turn_id=str(uuid4()), message=prompt)
        self.submit(session_id, queued)
        return queued.turn_id

    def submit(self, session_id: str, queued: TurnQueuedPayload) -> None:
        session = self.registry[session_id]
        session.pending.append(queued)
        if len(session.pending) == 1:
            self.record(session_id, queued)

    def record(
        self, session_id: str, payload: ConversationPayloadType
    ) -> ConversationEvent:
        session = self.registry[session_id]
        prepared = (
            None
            if isinstance(payload, ConversationCreatedPayload)
            else session.conversation.prepare(payload)
        )
        event = ConversationEvent(
            entry_id=f"{next(self.event_ids)}-0",
            type=get_cnv_event(type(payload)),
            payload=payload,
        )
        session.conversation.apply((event,))
        session.events[event.entry_id] = event
        # A delegation's child queued event schedules its poke once ready.
        if prepared is not None and not isinstance(
            payload, AgentRequestedPayload
        ):
            for action in prepared.dispatch(event.entry_id):
                self.queue.appendleft(action)
        if isinstance(payload, TurnCompletedPayload):
            turn = session.conversation.turn_state[payload.turn_id]
            if turn.parent is None:
                print(payload.message)
            session.pending.pop(0)
            if session.pending:
                self.record(session_id, session.pending[0])
        return event

    def drain(self) -> None:
        while self.queue:
            action = self.queue.pop()
            session = self.registry[action.conversation_id]
            if isinstance(action, ToolCallPayload):
                self.call_tool(session, action)
            elif isinstance(action, SubAgentResultPayload):
                child = self.registry[action.child_conversation_id]
                event = child.events[action.child_terminal_event_id]
                outcome = session.conversation.child_outcome(action, event)
                assert isinstance(outcome, ToolResultPayload)
                print_trace("result", value=outcome.result)
                self.record(action.conversation_id, outcome)
            else:
                self.run_agent(session, action)

    def run_agent(
        self,
        session: Session,
        action: ConversationPokePayload | AgentResumePayload,
    ) -> None:
        turn = session.conversation.turn_state[action.turn_id]
        prompt = (
            turn.prompt if isinstance(action, ConversationPokePayload) else None
        )
        print_trace(
            "agent",
            name=session.agent.name or "unnamed",
            **({"resume": True} if prompt is None else {"prompt": prompt}),
        )
        outcome = run_agent_step(
            session.agent,
            turn,
            prompt=prompt,
            deps=session.deps,
            model=session.conversation.settings.model or None,
        )
        session.tool_contexts.update(outcome.tool_contexts)
        self.record(action.conversation_id, outcome.payload)

    def call_tool(self, session: Session, action: ToolCallPayload) -> None:
        turn = session.conversation.turn_state[action.turn_id]
        call = turn.calls[action.tool_call_id]
        context, args = session.tool_contexts.pop(action.tool_call_id)
        print_trace("tool", name=call.tool_name, args=args)
        value = invoke_tool(session.agent, call.tool_name, args, context)
        if isinstance(value, AgentRequest):
            request = self.resolve_subagent(session.conversation, action, value)
            self.record(action.conversation_id, request)
            settings, queued = session.conversation.subagent_payloads(request)
            child_id = request.child_conversation_id
            if child_id not in self.registry:
                self._create_session(
                    reify_agent(settings.agent), settings, child_id
                )
            self.submit(child_id, queued)
        else:
            print_trace("result", value=value)
            self.record(
                action.conversation_id,
                ToolResultPayload(
                    turn_id=action.turn_id,
                    step_id=action.step_id,
                    tool_call_id=action.tool_call_id,
                    result=value,
                ),
            )

    @staticmethod
    def resolve_subagent(
        conversation: Conversation,
        action: ToolCallPayload,
        request: AgentRequest,
    ) -> AgentRequestedPayload:
        default_name = (
            reify_agent(request.agent_path).name
            if request.persistent
            and request.name is None
            and request.agent_path is not None
            else None
        )
        return resolve_subagent(
            conversation,
            action,
            request,
            default_name=default_name,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prompt", nargs="?")
    parser.add_argument(
        "--agent", default=os.getenv("FW_AGENT", "pydantic_ai:Agent")
    )
    parser.add_argument("--model", default=os.getenv("FW_MODEL"))
    args = parser.parse_args()
    executor = Executor()
    session_id = executor.create(args.agent, model=args.model)
    if args.prompt is not None:
        executor.send(session_id, args.prompt)
        executor.drain()
        return
    while True:
        try:
            prompt = input("> ")
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if prompt.strip():
            executor.send(session_id, prompt)
            executor.drain()


if __name__ == "__main__":
    main()
