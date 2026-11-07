"""Execute actions enabled by turn machines and checkpoint their outcomes."""

from threading import Event as ThreadEvent
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import TypeAdapter
from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from .agent_proxy import SubAgentRequest, execute_tool, run
from .catalog import reify_agent
from .conversation import Conversation, reconstruct_conversation
from .stream import ConversationStream, DispatchStream
from .models import (
    ConversationPokePayload,
    DispatchPayloadType,
    DispatchEvent,
    InvalidDispatchEvent,
    SubAgentResultPayload,
    SubAgentRequestedPayload,
    ToolCallPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
)
from .turn_state import (
    TurnPayload,
    TurnStateMachine,
    requests_adapter,
)
from .utils import logger


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
        if not conversation.accepts_result(payload):
            return False
        prepared = conversation.prepare(payload)
        cnv_stream.append_payload(prepared.payload)
        # Replay from the old cursor so a preceding sibling write is included.
        self.refresh(cnv_stream, conversation)
        self.publish(cnv_stream, conversation.resume_actions(payload))
        return True

    def queue_subagent(
        self,
        cnv_stream: ConversationStream,
        conversation: Conversation,
        request: SubAgentRequestedPayload,
        child: SubAgentRequest,
    ) -> None:
        settings, queued, prepared = conversation.prepare_subagent(
            request,
            agent=child.agent_path,
            model=child.model or conversation.settings.model,
            deps=child.deps,
            prompt=child.prompt,
        )
        child_stream = cnv_stream.dispatch.conversation_stream(
            request.child_conversation_id
        )
        # A queued child can reconstruct its parent request during recovery.
        child_stream.append_payload(settings)
        child_stream.append_payload(queued)
        entry_id = cnv_stream.append_payload(prepared.payload)
        self.publish(cnv_stream, prepared.dispatch(entry_id))

    def run_agent_step(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        machine: TurnStateMachine,
        cnv_stream: ConversationStream,
        deps: Any,
        prompt: str | None,
    ) -> None:
        requests = machine.requests
        result = run(
            agent,
            prompt,
            message_history=[*machine.history, *machine.turn_messages],
            deferred_tool_results=(
                requests.build_results(
                    calls=machine.tool_results, metadata=requests.metadata
                )
                if requests is not None
                else None
            ),
            deps=deps,
            model=conversation.settings.model or None,
            usage=machine.usage,
            conversation_id=cnv_stream.conversation_id,
        )
        if isinstance(result.output, DeferredToolRequests):
            payload = ToolRequestedPayload(
                turn_id=machine.turn_id,
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
                turn_id=machine.turn_id,
                message=str(result.output),
                turn_messages=ModelMessagesTypeAdapter.dump_python(
                    [*machine.turn_messages, *result.new_messages], mode="json"
                ),
                usage=result.usage,
            ),
        )
        logger.info(
            "Sent response for turn %s in conversation %s",
            machine.turn_id,
            cnv_stream.conversation_id,
        )

    def execute_tool_call(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        machine: TurnStateMachine,
        cnv_stream: ConversationStream,
        deps: Any,
        action: ToolCallPayload,
    ) -> None:
        value = execute_tool(
            agent,
            machine.calls[action.tool_call_id],
            deps=deps,
            prompt=machine.prompt,
            messages=[*machine.history, *machine.turn_messages],
            model=conversation.settings.model or None,
            usage=machine.usage or RunUsage(),
            conversation_id=cnv_stream.conversation_id,
        )
        if isinstance(value, SubAgentRequest):
            child_conversation_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{cnv_stream.conversation_id}:{machine.turn_id}:{action.step_id}:{action.tool_call_id}",
                )
            )
            child_turn_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{child_conversation_id}:turn:{action.tool_call_id}",
                )
            )
            self.queue_subagent(
                cnv_stream,
                conversation,
                SubAgentRequestedPayload(
                    turn_id=machine.turn_id,
                    step_id=action.step_id,
                    tool_call_id=action.tool_call_id,
                    child_conversation_id=child_conversation_id,
                    child_turn_id=child_turn_id,
                ),
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
                turn_id=machine.turn_id,
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
        machine = conversation.turn_state[payload.turn_id]
        match conversation.dispatch_status(payload):
            case "stale":
                return True
            case "wait":
                return False
        if isinstance(payload, SubAgentResultPayload):
            return self.process_subagent_result(
                cnv_stream, payload, conversation
            )

        settings = conversation.settings
        agent = reify_agent(settings.agent)
        deps = (
            None
            if not settings.deps and agent.deps_type is type(None)
            else TypeAdapter(agent.deps_type).validate_python(settings.deps)
        )
        if isinstance(payload, ToolCallPayload):
            self.execute_tool_call(
                agent=agent,
                conversation=conversation,
                machine=machine,
                cnv_stream=cnv_stream,
                deps=deps,
                action=payload,
            )
        else:
            self.run_agent_step(
                agent=agent,
                conversation=conversation,
                machine=machine,
                cnv_stream=cnv_stream,
                deps=deps,
                prompt=(
                    machine.prompt
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
                    TurnFailedPayload(turn_id=turn_id, error=str(error)),
                )
                processed = True
            if processed:
                self.dsp_stream.acknowledge(self.group, action.entry_id)
