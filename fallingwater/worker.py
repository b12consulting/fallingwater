"""Execute actions enabled by turn machines and checkpoint their outcomes."""

from collections import OrderedDict
from threading import Event as ThreadEvent, Lock
from traceback import format_exc
from typing import Any
from uuid import uuid4

from pydantic import TypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from fallingwater.catalog import reify_agent
from fallingwater.agent_proxy import (
    execute_tool,
    resolve_subagent,
    run_agent_step,
    validate_deps,
)
from fallingwater.conversation import Conversation
from fallingwater.stream import ConversationStream, DispatchStream
from fallingwater.models import (
    ConversationPokePayload,
    DispatchPayloadType,
    DispatchEvent,
    Event,
    InvalidDispatchEvent,
    SubAgentResultPayload,
    AgentRequestedPayload,
    AgentRequest,
    ToolCallPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
)
from fallingwater.turn_state import (
    TurnPayload,
    TurnStateMachine,
)
from fallingwater.utils import logger


_json_value_adapter = TypeAdapter(Any)


class ConversationCache:
    """Bounded replay snapshots for workers sharing one Redis database."""

    def __init__(self, *, maxsize: int = 128) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self.maxsize = maxsize
        self._snapshots: OrderedDict[str, Conversation] = OrderedDict()
        self._lock = Lock()

    def load(self, stream: ConversationStream) -> Conversation:
        """Replay the unseen tail and return an isolated working clone."""
        with self._lock:
            cached = self._snapshots.get(stream.name)
            if cached is not None:
                self._snapshots.move_to_end(stream.name)
        if cached is None:
            snapshot = Conversation(
                stream.iter_events(), conversation_id=stream.conversation_id
            )
        else:
            snapshot = cached.clone()
            snapshot.apply(stream.iter_events(after_id=snapshot.last_event_id))

        with self._lock:
            current = self._snapshots.get(stream.name)
            # A concurrent load may already have cached a newer snapshot.
            if current is None or tuple(
                map(int, snapshot.last_event_id.split("-"))
            ) > tuple(map(int, current.last_event_id.split("-"))):
                self._snapshots[stream.name] = snapshot
                self._snapshots.move_to_end(stream.name)
                if len(self._snapshots) > self.maxsize:
                    self._snapshots.popitem(last=False)
        # Keep worker mutations separate from the snapshot owned by the cache.
        return snapshot.clone()


class Worker:
    """Claim independent agent, tool, and child-result actions."""

    group = "workers"

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        claim_idle_ms: int = 300_000,
        conversation_cache: ConversationCache | None = None,
    ) -> None:
        if claim_idle_ms < 0:
            raise ValueError("claim_idle_ms must be nonnegative")
        self.dsp_stream = DispatchStream(redis, namespace=namespace)
        self.conversation_cache = (
            conversation_cache
            if conversation_cache is not None
            else ConversationCache()
        )
        self.claim_idle_ms = claim_idle_ms
        self.consumer = str(uuid4())
        self.stop_event = ThreadEvent()
        self.claim_cursor = "0-0"

    def stop(self) -> None:
        """Stop reading more actions after the active action is checkpointed."""
        self.stop_event.set()

    @staticmethod
    def refresh(
        cnv_stream: ConversationStream, conversation: Conversation
    ) -> None:
        conversation.apply(
            cnv_stream.iter_events(after_id=conversation.last_event_id)
        )

    @staticmethod
    def publish(
        cnv_stream: ConversationStream,
        actions: tuple[DispatchPayloadType, ...],
    ) -> None:
        for payload in actions:
            cnv_stream.dispatch.append_payload(payload)

    def checkpoint(
        self,
        cnv_stream: ConversationStream,
        conversation: Conversation,
        payload: TurnPayload,
    ) -> str:
        prepared = conversation.prepare(payload)
        entry_id = cnv_stream.append_payload(prepared.payload)
        self.publish(cnv_stream, prepared.dispatch(entry_id))
        return entry_id

    def save_tool_result(
        self,
        cnv_stream: ConversationStream,
        conversation: Conversation,
        payload: ToolResultPayload,
    ) -> bool:
        self.refresh(cnv_stream, conversation)
        while conversation.accepts_result(payload):
            prepared = conversation.prepare(payload)
            entry_id, events = cnv_stream.append_if_current(
                Event.TURN_TOOL_RESULT,
                prepared.payload,
                after_id=conversation.last_event_id,
            )
            conversation.apply(events)
            if entry_id is not None:
                # We received an entry, so our result was appended.
                # Check readiness without reading later writes, which
                # could cause duplicate resumes.
                self.publish(cnv_stream, conversation.resume_actions(payload))
                return True
        return False

    def queue_subagent(
        self,
        cnv_stream: ConversationStream,
        conversation: Conversation,
        action: ToolCallPayload,
        child: AgentRequest,
    ) -> None:
        self.refresh(cnv_stream, conversation)
        while conversation.turn_state[action.turn_id].running:
            request = conversation.turn_state[action.turn_id].delegations.get(
                action.tool_call_id
            )
            if request is not None:
                self.ensure_subagent(conversation, request)
                return
            request = self.resolve_subagent(conversation, action, child)
            prepared = conversation.prepare(request)
            entry_id, events = cnv_stream.append_if_current(
                Event.TURN_AGENT_REQUESTED,
                prepared.payload,
                after_id=conversation.last_event_id,
            )
            conversation.apply(events)
            if entry_id is not None:
                self.ensure_subagent(conversation, request)
                return

    @staticmethod
    def resolve_subagent(
        conversation: Conversation,
        action: ToolCallPayload,
        child: AgentRequest,
    ) -> AgentRequestedPayload:
        default_name = (
            reify_agent(child.agent_path).name
            if child.persistent
            and child.name is None
            and child.agent_path is not None
            else None
        )
        return resolve_subagent(
            conversation,
            action,
            child,
            default_name=default_name,
        )

    def ensure_subagent(
        self,
        conversation: Conversation,
        request: AgentRequestedPayload,
    ) -> None:
        settings, queued = conversation.subagent_payloads(request)
        child_stream = self.dsp_stream.conversation_stream(
            request.child_conversation_id
        )
        if not child_stream.read_conversation_range(count=1):
            child_stream.append_if_current(
                Event.CONVERSATION_CREATED, settings, after_id="0-0"
            )
        child_conversation = self.conversation_cache.load(child_stream)
        saved = child_conversation.settings
        if not saved.has_same_settings(settings):
            raise ValueError("Agent conversation has conflicting settings")
        while True:
            existing = child_conversation.turn_state.get(queued.turn_id)
            if existing is not None:
                if existing.queued != queued:
                    raise ValueError("Agent turn has a conflicting request")
                poke = ConversationPokePayload(
                    conversation_id=request.child_conversation_id,
                    turn_id=queued.turn_id,
                )
                if child_conversation.dispatch_status(poke) == "ready":
                    self.publish(child_stream, (poke,))
                return
            prepared = child_conversation.prepare(queued)
            entry_id, tail = child_stream.append_if_current(
                Event.TURN_QUEUED,
                prepared.payload,
                after_id=child_conversation.last_event_id,
            )
            child_conversation.apply(tail)
            if entry_id is not None:
                self.publish(child_stream, prepared.dispatch(entry_id))
                return

    def run_agent_step(
        self,
        *,
        conversation: Conversation,
        turn: TurnStateMachine,
        cnv_stream: ConversationStream,
        prompt: str | None,
    ) -> None:
        settings = conversation.settings
        agent = reify_agent(settings.agent)
        outcome = run_agent_step(
            agent,
            turn,
            prompt=prompt,
            deps=validate_deps(agent, settings.deps),
            model=settings.model or None,
        )
        self.checkpoint(cnv_stream, conversation, outcome.payload)
        if isinstance(outcome.payload, TurnCompletedPayload):
            logger.info(
                "Sent response for turn %s in conversation %s",
                turn.turn_id,
                cnv_stream.conversation_id,
            )

    def execute_tool_call(
        self,
        *,
        conversation: Conversation,
        turn: TurnStateMachine,
        cnv_stream: ConversationStream,
        action: ToolCallPayload,
    ) -> None:
        settings = conversation.settings
        agent = reify_agent(settings.agent)
        value = execute_tool(
            agent,
            turn.calls[action.tool_call_id],
            prompt=turn.prompt,
            messages=[*turn.history, *turn.turn_messages],
            usage=turn.usage or RunUsage(),
            deps=validate_deps(agent, settings.deps),
            model=settings.model or None,
            conversation_id=conversation.conversation_id,
        )
        if isinstance(value, AgentRequest):
            self.queue_subagent(
                cnv_stream,
                conversation,
                action,
                value,
            )
            return

        result = _json_value_adapter.validate_json(
            _json_value_adapter.dump_json(value)
        )
        self.save_tool_result(
            cnv_stream,
            conversation,
            ToolResultPayload(
                turn_id=turn.turn_id,
                step_id=action.step_id,
                tool_call_id=action.tool_call_id,
                result=result,
            ),
        )

    def process_subagent_result(
        self,
        cnv_stream: ConversationStream,
        action: SubAgentResultPayload,
        conversation: Conversation,
    ) -> bool:
        child_stream = self.dsp_stream.conversation_stream(
            action.child_conversation_id
        )
        events = child_stream.read_conversation_range(
            min=action.child_terminal_event_id, count=1
        )
        if not events or events[0].entry_id != action.child_terminal_event_id:
            return False
        payload = conversation.child_outcome(action, events[0])
        if isinstance(payload, ToolResultPayload):
            self.save_tool_result(cnv_stream, conversation, payload)
        elif isinstance(payload, TurnFailedPayload):
            self.checkpoint(cnv_stream, conversation, payload)
        return True

    def process(
        self,
        action: DispatchEvent,
        cnv_stream: ConversationStream,
        conversation: Conversation,
    ) -> bool:
        """Execute ready hints, acknowledge stale ones, and defer waiting ones."""
        payload = action.payload
        turn = conversation.turn_state[payload.turn_id]
        if isinstance(payload, ToolCallPayload) and turn.running:
            delegated = turn.delegations.get(payload.tool_call_id)
            if delegated is not None and delegated.step_id == payload.step_id:
                self.ensure_subagent(conversation, delegated)
                return True
        match conversation.dispatch_status(payload):
            case "stale":
                return True
            case "wait":
                return False
        if isinstance(payload, SubAgentResultPayload):
            return self.process_subagent_result(
                cnv_stream, payload, conversation
            )

        if isinstance(payload, ToolCallPayload):
            self.execute_tool_call(
                conversation=conversation,
                turn=turn,
                cnv_stream=cnv_stream,
                action=payload,
            )
        else:
            self.run_agent_step(
                conversation=conversation,
                turn=turn,
                cnv_stream=cnv_stream,
                prompt=(
                    turn.prompt
                    if isinstance(payload, ConversationPokePayload)
                    else None
                ),
            )
        return True

    def run(self) -> None:
        """Read independent actions until stopped; acknowledge after checkpoint."""
        self.dsp_stream.create_group(self.group, id="0", mkstream=True)
        while not self.stop_event.is_set():
            claim_cursor, claimed, _deleted = self.dsp_stream.claim(
                self.group,
                self.consumer,
                min_idle_time=self.claim_idle_ms,
                start_id=self.claim_cursor,
                count=1,
            )
            self.claim_cursor = claim_cursor
            entries = claimed or self.dsp_stream.read_group(
                self.group, self.consumer, ">", count=1, block=500
            )
            if not entries:
                continue
            action = entries[0]
            if isinstance(action, InvalidDispatchEvent):
                logger.warning(
                    "Skipping malformed dispatch fields %s: %s",
                    action.fields,
                    action.error,
                )
                self.dsp_stream.acknowledge(self.group, action.entry_id)
                continue
            conversation_id = action.payload.conversation_id
            turn_id = action.payload.turn_id
            logger.info(
                "Process turn %s in conversation %s", turn_id, conversation_id
            )
            cnv_stream = self.dsp_stream.conversation_stream(conversation_id)
            try:
                conversation = self.conversation_cache.load(cnv_stream)
                if turn_id not in conversation.turn_state:
                    raise ValueError(f"No prompt found for turn: {turn_id}")
            except ValueError:
                logger.exception(
                    "Could not load turn %s in conversation %s",
                    turn_id,
                    conversation_id,
                )
                self.dsp_stream.acknowledge(self.group, action.entry_id)
                continue

            try:
                processed = self.process(action, cnv_stream, conversation)
            except RedisError:
                raise
            except Exception as error:
                logger.exception(
                    "Turn %s failed in conversation %s",
                    turn_id,
                    conversation_id,
                )
                self.checkpoint(
                    cnv_stream,
                    conversation,
                    TurnFailedPayload(
                        turn_id=turn_id,
                        error=str(error),
                        traceback=format_exc(),
                    ),
                )
                processed = True
            if processed:
                self.dsp_stream.acknowledge(self.group, action.entry_id)
