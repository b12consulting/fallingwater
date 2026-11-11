"""Execute actions enabled by turn machines and checkpoint their outcomes."""

from threading import Event as ThreadEvent
from traceback import format_exc
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import TypeAdapter
from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from fallingwater.catalog import reify_agent
from fallingwater.agent_proxy import AgentRequest, execute_tool, run
from fallingwater.conversation import Conversation, reconstruct_conversation
from fallingwater.stream import ConversationStream, DispatchStream
from fallingwater.models import (
    ConversationPokePayload,
    DispatchPayloadType,
    DispatchEvent,
    Event,
    InvalidDispatchEvent,
    SubAgentResultPayload,
    AgentRequestedPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
    get_cnv_event,
)
from fallingwater.turn_state import (
    TurnPayload,
    TurnStateMachine,
    requests_adapter,
)
from fallingwater.utils import logger


_json_value_adapter = TypeAdapter(Any)


class Worker:
    """Claim independent agent, tool, and child-result actions."""

    group = "workers"

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        claim_idle_ms: int = 300_000,
    ) -> None:
        if claim_idle_ms < 0:
            raise ValueError("claim_idle_ms must be nonnegative")
        self.dsp_stream = DispatchStream(redis, namespace=namespace)
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
        for event in cnv_stream.iter_events(
            after_id=conversation.last_event_id
        ):
            conversation.apply(event)

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
        self.refresh(cnv_stream, conversation)
        while True:
            turn = conversation.turn_state.get(payload.turn_id)
            if turn is not None and not turn.running:
                if turn.terminal_event_id is None:
                    raise ValueError("Terminal turn has no saved event ID")
                return turn.terminal_event_id
            prepared = conversation.prepare(payload)
            entry_id, events = cnv_stream.append_if_current(
                get_cnv_event(type(payload)),
                prepared.payload,
                after_id=conversation.last_event_id,
            )
            for event in events:
                conversation.apply(event)
            if entry_id is not None:
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
            for event in events:
                # Resume the conversation state on new events
                conversation.apply(event)
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
            for event in events:
                conversation.apply(event)
            if entry_id is not None:
                self.ensure_subagent(conversation, request)
                return

    @staticmethod
    def resolve_subagent(
        conversation: Conversation,
        action: ToolCallPayload,
        child: AgentRequest,
    ) -> AgentRequestedPayload:
        name = child.name
        if child.persistent and name is None and child.agent_path is not None:
            name = reify_agent(child.agent_path).name
        if child.persistent and (name is None or not name.strip()):
            raise ValueError("Persistent agent requires a name")
        registered = (
            conversation.agent_by_name.get(name)
            if child.persistent and name is not None
            else None
        )
        if registered is not None and name is not None:
            previous = conversation.agent_spec(name)
            if (
                child.agent_path is not None
                and child.agent_path != previous.agent_path
            ):
                raise ValueError("Registered agent has a different agent_path")
            if child.model is not None and child.model != previous.model:
                raise ValueError("Registered agent has a different model")
            if "deps" in child.model_fields_set and child.deps != previous.deps:
                raise ValueError("Registered agent has different dependencies")
            agent_path = previous.agent_path
            model = previous.model
            deps = previous.deps
            conversation_id = registered
        else:
            if child.agent_path is None:
                raise ValueError(f"No registered agent named {name!r}")
            agent_path = child.agent_path
            model = child.model or conversation.settings.model
            deps = child.deps
            conversation_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{conversation.conversation_id}:{action.turn_id}:{action.step_id}:{action.tool_call_id}",
                )
            )
        turn_id = str(
            uuid5(
                NAMESPACE_URL,
                f"fallingwater:{conversation_id}:turn:{conversation.conversation_id}:{action.turn_id}:{action.step_id}:{action.tool_call_id}",
            )
        )
        return AgentRequestedPayload(
            turn_id=action.turn_id,
            step_id=action.step_id,
            tool_call_id=action.tool_call_id,
            child_conversation_id=conversation_id,
            child_turn_id=turn_id,
            agent_path=agent_path,
            model=model,
            deps=deps,
            prompt=child.prompt,
            name=name,
            persistent=child.persistent,
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
        events = list(child_stream.iter_events())
        if not events:
            child_stream.append_if_current(
                Event.CONVERSATION_CREATED, settings, after_id="0-0"
            )
            events = list(child_stream.iter_events())
        child_conversation = reconstruct_conversation(
            events, conversation_id=request.child_conversation_id
        )
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
            for event in tail:
                child_conversation.apply(event)
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
        requests = turn.requests
        agent = reify_agent(conversation.settings.agent)
        deps = (
            None
            if not conversation.settings.deps and agent.deps_type is type(None)
            else TypeAdapter(agent.deps_type).validate_python(
                conversation.settings.deps
            )
        )

        result = run(
            agent,
            prompt,
            message_history=[*turn.history, *turn.turn_messages],
            deferred_tool_results=(
                requests.build_results(
                    calls=turn.tool_results, metadata=requests.metadata
                )
                if requests is not None
                else None
            ),
            deps=deps,
            model=conversation.settings.model or None,
            usage=turn.usage,
            conversation_id=cnv_stream.conversation_id,
        )
        if isinstance(result.output, DeferredToolRequests):
            payload = ToolRequestedPayload(
                turn_id=turn.turn_id,
                step_id=str(uuid4()),
                requests=requests_adapter.dump_python(
                    result.output, mode="json"
                ),
                step_messages=ModelMessagesTypeAdapter.dump_python(
                    result.new_messages, mode="json"
                ),
                usage=result.usage,
            )
            self.checkpoint(cnv_stream, conversation, payload)
            return

        self.checkpoint(
            cnv_stream,
            conversation,
            TurnCompletedPayload(
                turn_id=turn.turn_id,
                message=str(result.output),
                turn_messages=ModelMessagesTypeAdapter.dump_python(
                    [*turn.turn_messages, *result.new_messages], mode="json"
                ),
                usage=result.usage,
            ),
        )
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
        value = execute_tool(
            conversation,
            turn.calls[action.tool_call_id],
            prompt=turn.prompt,
            messages=[*turn.history, *turn.turn_messages],
            usage=turn.usage or RunUsage(),
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
                conversation = reconstruct_conversation(
                    cnv_stream.iter_events(), conversation_id=conversation_id
                )
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
