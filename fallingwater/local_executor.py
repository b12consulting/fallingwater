"""Embedded FIFO execution with process-local conversation event logs."""

from __future__ import annotations

from collections import deque
from contextvars import copy_context
from dataclasses import dataclass, field
from threading import Condition, Lock, Thread, current_thread
from time import time_ns
from traceback import format_exc
from typing import Any
from uuid import uuid4

from pydantic_ai import Agent

from .archive import load_events, save_events
from .agent_proxy import (
    ToolContexts,
    invoke_tool,
    resolve_subagent,
    run_agent_step,
    validate_deps,
)
from .catalog import reify_agent
from .conversation import Conversation
from .models import (
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
    TurnFailedPayload,
    TurnQueuedPayload,
    get_cnv_event,
)
from .utils import logger


@dataclass
class Session:
    agent: Agent
    conversation: Conversation
    deps: Any = None
    pending: list[TurnQueuedPayload] = field(default_factory=list)
    events: dict[str, ConversationEvent] = field(default_factory=dict)
    tool_contexts: ToolContexts = field(default_factory=dict)
    acknowledged: dict[str, set[str]] = field(default_factory=dict)


class LocalExecutor:
    """Run one action at a time, retaining sequential history per session.

    Use ``drain()`` synchronously or ``start()`` for background execution.
    ``close()`` finishes the active action, leaving remaining work in memory.
    Registry contents are read-only to callers; coordinate inspection with
    ``condition`` while execution is active. Archives persist recorded events,
    not pending actions or captured tool contexts.
    """

    def __init__(self) -> None:
        self.registry: dict[str, Session] = {}
        self.queue: deque[DispatchPayloadType] = deque()
        self.condition = Condition()
        self._execution_lock = Lock()
        self._thread: Thread | None = None
        self._closed = False
        self._failure: Exception | None = None
        self._milliseconds = 0
        self._sequence = 0

    def check(self) -> None:
        if self._failure is not None:
            raise RuntimeError("Local executor failed") from self._failure
        if self._closed:
            raise RuntimeError("Local executor is closed")

    def create(
        self,
        agent: str,
        *,
        model: str | None = None,
        deps: dict[str, Any] | None = None,
        conversation_id: str | None = None,
    ) -> str:
        with self.condition:
            self.check()
            return self._create_session(
                reify_agent(agent),
                ConversationCreatedPayload(
                    agent=agent, model=model or "", deps=deps or {}
                ),
                conversation_id or str(uuid4()),
            )

    def _create_session(
        self,
        agent: Agent,
        settings: ConversationCreatedPayload,
        session_id: str,
    ) -> str:
        if session_id in self.registry:
            raise ValueError(f"Conversation already exists: {session_id}")
        self.registry[session_id] = Session(
            agent,
            Conversation(conversation_id=session_id),
            validate_deps(agent, settings.deps),
        )
        self.record(session_id, settings)
        return session_id

    def send(self, session_id: str, prompt: str) -> str:
        with self.condition:
            self.check()
            queued = TurnQueuedPayload(turn_id=str(uuid4()), message=prompt)
            self.submit(session_id, queued)
            return queued.turn_id

    def save(self, session_id: str, store: str = ".fw-store") -> int:
        """Archive a snapshot of this session and all referenced descendants."""
        with self.condition:
            snapshots: dict[str, tuple[ConversationEvent, ...]] = {}
            pending = [session_id]
            while pending:
                current = pending.pop()
                if current in snapshots:
                    continue
                events = tuple(self.registry[current].events.values())
                snapshots[current] = events
                pending.extend(
                    event.payload.child_conversation_id
                    for event in events
                    if isinstance(event.payload, AgentRequestedPayload)
                )
        return sum(
            save_events(events, current, store)
            for current, events in snapshots.items()
        )

    def load(self, session_id: str, store: str = ".fw-store") -> str:
        """Restore settled sessions and their descendants without scheduling."""
        with self.condition:
            self.check()
            sessions: dict[str, Session] = {}
            pending = [session_id]
            latest_id = (self._milliseconds, self._sequence)
            while pending:
                current = pending.pop()
                if current in sessions:
                    continue
                if current in self.registry:
                    raise ValueError(f"Conversation already exists: {current}")
                events = list(load_events(current, store))
                previous = (0, 0)
                for event in events:
                    parts = event.entry_id.split("-")
                    if len(parts) != 2 or not all(
                        part.isascii() and part.isdigit() for part in parts
                    ):
                        raise ValueError(f"Invalid event ID: {event.entry_id}")
                    entry_id = (int(parts[0]), int(parts[1]))
                    if entry_id <= previous:
                        raise ValueError(
                            f"Event IDs are not increasing in {current}"
                        )
                    previous = entry_id
                latest_id = max(latest_id, previous)
                conversation = Conversation(events, conversation_id=current)
                if any(
                    turn.running for turn in conversation.turn_state.values()
                ):
                    raise ValueError(
                        f"Conversation {current} has unfinished turns"
                    )
                settings = conversation.settings
                agent = reify_agent(settings.agent)
                sessions[current] = Session(
                    agent=agent,
                    conversation=conversation,
                    deps=validate_deps(agent, settings.deps),
                    events={event.entry_id: event for event in events},
                )
                pending.extend(
                    event.payload.child_conversation_id
                    for event in events
                    if isinstance(event.payload, AgentRequestedPayload)
                )
            self.registry.update(sessions)
            self._milliseconds, self._sequence = latest_id
            return session_id

    def submit(self, session_id: str, queued: TurnQueuedPayload) -> None:
        with self.condition:
            session = self.registry[session_id]
            session.pending.append(queued)
            if len(session.pending) == 1:
                self.record(session_id, queued)

    def record(
        self, session_id: str, payload: ConversationPayloadType
    ) -> ConversationEvent:
        with self.condition:
            session = self.registry[session_id]
            prepared = (
                None
                if isinstance(payload, ConversationCreatedPayload)
                else session.conversation.prepare(payload)
            )
            milliseconds = max(time_ns() // 1_000_000, self._milliseconds)
            self._sequence = (
                self._sequence + 1 if milliseconds == self._milliseconds else 0
            )
            self._milliseconds = milliseconds
            event = ConversationEvent(
                entry_id=f"{milliseconds}-{self._sequence}",
                type=get_cnv_event(type(payload)),
                payload=payload,
            )
            session.conversation.apply((event,))
            session.events[event.entry_id] = event
            # Child submission schedules its poke only once that turn is ready.
            if prepared is not None and not isinstance(
                payload, AgentRequestedPayload
            ):
                self.queue.extendleft(prepared.dispatch(event.entry_id))
            if isinstance(payload, (TurnCompletedPayload, TurnFailedPayload)):
                turn = session.conversation.turn_state[payload.turn_id]
                for tool_call_id in turn.calls:
                    session.tool_contexts.pop(tool_call_id, None)
                session.pending.pop(0)
                if session.pending:
                    self.record(session_id, session.pending[0])
            self.condition.notify_all()
            return event

    def drain(self) -> None:
        """Execute all available actions without starting a background thread."""
        with self._execution_lock:
            with self.condition:
                self.check()
                if self._thread is not None:
                    raise RuntimeError("Local executor is already running")
            while self._step():
                pass

    def _step(self) -> bool:
        with self.condition:
            if self._closed or not self.queue:
                return False
            action = self.queue.pop()
            session = self.registry[action.conversation_id]
            status = session.conversation.dispatch_status(action)
            if status == "stale":
                return True
            if status == "wait":
                raise RuntimeError("Local action is not ready")
        try:
            if isinstance(action, ToolCallPayload):
                self.call_tool(session, action)
            elif isinstance(action, SubAgentResultPayload):
                child = self.registry[action.child_conversation_id]
                event = child.events[action.child_terminal_event_id]
                outcome = session.conversation.child_outcome(action, event)
                if outcome is not None:
                    self.record(action.conversation_id, outcome)
            else:
                self.run_agent(session, action)
        except Exception as error:
            logger.exception(
                "Local turn %s failed in conversation %s",
                action.turn_id,
                action.conversation_id,
            )
            self.record(
                action.conversation_id,
                TurnFailedPayload(
                    turn_id=action.turn_id,
                    error=str(error),
                    traceback=format_exc(),
                ),
            )
        return True

    def run_agent(
        self,
        session: Session,
        action: ConversationPokePayload | AgentResumePayload,
    ) -> None:
        turn = session.conversation.turn_state[action.turn_id]
        outcome = run_agent_step(
            session.agent,
            turn,
            prompt=(
                turn.prompt
                if isinstance(action, ConversationPokePayload)
                else None
            ),
            deps=session.deps,
            model=session.conversation.settings.model or None,
        )
        session.tool_contexts.update(outcome.tool_contexts)
        self.record(action.conversation_id, outcome.payload)

    def call_tool(self, session: Session, action: ToolCallPayload) -> None:
        turn = session.conversation.turn_state[action.turn_id]
        call = turn.calls[action.tool_call_id]
        context, args = session.tool_contexts.pop(action.tool_call_id)
        value = invoke_tool(session.agent, call.tool_name, args, context)
        if isinstance(value, AgentRequest):
            request = self.resolve_subagent(session.conversation, action, value)
            with self.condition:
                settings, queued = session.conversation.subagent_payloads(
                    request
                )
                child_id = request.child_conversation_id
                if child_id not in self.registry:
                    self._create_session(
                        reify_agent(settings.agent), settings, child_id
                    )
                self.record(action.conversation_id, request)
                self.submit(child_id, queued)
        else:
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
            conversation, action, request, default_name=default_name
        )

    def start(self) -> None:
        """Start the single execution thread, if it is not already running."""
        with self._execution_lock:
            with self.condition:
                self.check()
                if self._thread is None:
                    self._thread = Thread(
                        target=copy_context().run,
                        args=(self._run,),
                        name="fw-local",
                        daemon=True,
                    )
                    self._thread.start()

    def _run(self) -> None:
        try:
            while True:
                with self.condition:
                    self.condition.wait_for(
                        lambda: self._closed or bool(self.queue)
                    )
                    if self._closed:
                        return
                self._step()
        except Exception as error:
            logger.exception("Local executor failed")
            with self.condition:
                self._failure = error
                self.condition.notify_all()

    def close(self) -> None:
        """Stop after the active action; this can wait for a model or tool."""
        with self.condition:
            self._closed = True
            self.condition.notify_all()
            thread = self._thread
        if thread is not None and thread is not current_thread():
            thread.join()
        with self._execution_lock:
            pass

    def __enter__(self) -> LocalExecutor:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
