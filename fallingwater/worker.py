"""Run and checkpoint Pydantic AI actions from the Redis dispatch stream."""

from threading import Event as ThreadEvent
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import TypeAdapter
from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from .agent_proxy import (
    SubAgentRequest,
    execute_tool,
    run,
)
from .catalog import reify_agent
from .stream import ConversationStream, DispatchStream
from .stream_utils import (
    AgentResumeAction,
    ConversationPokeAction,
    DispatchAction,
    Event,
    InvalidDispatchAction,
    SubAgentResultAction,
    SubAgentRequestedPayload,
    ToolCallAction,
    ToolRequestedPayload,
    ToolResultPayload,
    TurnCompletedPayload,
    TurnFailedPayload,
)
from .turn_state import Conversation, reconstruct_turn
from .utils import logger


_requests_adapter = TypeAdapter(DeferredToolRequests)
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

    def _append_failed(
        self,
        cnv_stream: ConversationStream,
        conversation: Conversation,
        error: Exception,
    ) -> None:
        cnv_stream.finish_turn(
            Event.TURN_FAILED,
            TurnFailedPayload(
                turn_id=conversation.current_turn_id, error=str(error)
            ),
            parent=conversation.parent,
        )

    @staticmethod
    def _call_ids(requests: DeferredToolRequests) -> list[str]:
        if requests.approvals:
            raise NotImplementedError(
                "Fallingwater does not handle deferred tool approvals"
            )
        return [call.tool_call_id for call in requests.calls]

    def _run_agent_step(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        cnv_stream: ConversationStream,
        deps: Any,
        prompt: str | None,
        requests: DeferredToolRequests | None,
        resume_step_id: str,
    ) -> bool:
        """Run once and atomically queue tool jobs or save a terminal result."""
        lease_seconds = max(5.0, min(self.claim_idle_ms / 1000 or 5.0, 60.0))
        with cnv_stream.lock_agent_run(
            conversation.current_turn_id,
            resume_step_id,
            lease_seconds=lease_seconds,
        ) as lease:
            if lease is None:
                return False
            # The action can be redelivered after a prior worker checkpointed it.
            refreshed = reconstruct_turn(
                cnv_stream.iter_events(),
                conversation.current_turn_id,
                conversation_stream=cnv_stream.name,
            )
            if refreshed.completed or refreshed.failed is not None:
                return True
            if requests is None:
                if refreshed.tool_requests is not None:
                    return True
            elif refreshed.tool_step_id != resume_step_id:
                return True
            elif not set(self._call_ids(requests)) <= set(
                refreshed.tool_results
            ):
                return False

            deferred_results = (
                requests.build_results(
                    calls=refreshed.tool_results,
                    metadata=requests.metadata,
                )
                if requests is not None
                else None
            )
            result = run(
                agent,
                prompt,
                message_history=[*refreshed.history, *refreshed.turn_messages],
                deferred_tool_results=deferred_results,
                deps=deps,
                model=refreshed.model or None,
                usage=refreshed.usage,
                conversation_id=cnv_stream.conversation_id,
            )
            if isinstance(result.output, DeferredToolRequests):
                step_id = str(uuid4())
                request_payload = ToolRequestedPayload(
                    turn_id=refreshed.current_turn_id,
                    step_id=step_id,
                    requests=_requests_adapter.dump_python(
                        result.output, mode="json"
                    ),
                    history=ModelMessagesTypeAdapter.dump_python(
                        result.new_messages, mode="json"
                    ),
                    usage=result.usage,
                )
                lease.ensure_held()
                cnv_stream.queue_tool_requests(
                    request_payload,
                    self._call_ids(result.output),
                )
                return True

            completed = TurnCompletedPayload(
                turn_id=refreshed.current_turn_id,
                message=str(result.output),
                history=ModelMessagesTypeAdapter.dump_python(
                    [*refreshed.turn_messages, *result.new_messages],
                    mode="json",
                ),
                usage=result.usage,
            )
            lease.ensure_held()
            cnv_stream.finish_turn(
                Event.TURN_COMPLETED, completed, parent=refreshed.parent
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                refreshed.current_turn_id,
                cnv_stream.conversation_id,
            )
            return True

    def _execute_tool_call(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        cnv_stream: ConversationStream,
        deps: Any,
        step_id: str,
        tool_call_id: str,
    ) -> bool:
        """Execute one deferred tool and persist its result or child request."""
        if (
            conversation.tool_step_id != step_id
            or conversation.tool_requests is None
        ):
            return True
        requests = _requests_adapter.validate_python(conversation.tool_requests)
        call = next(
            (
                call
                for call in requests.calls
                if call.tool_call_id == tool_call_id
            ),
            None,
        )
        if call is None:
            return True
        if (
            tool_call_id in conversation.tool_results
            or tool_call_id in conversation.subagent_requests
        ):
            return True

        value = execute_tool(
            agent,
            call,
            deps=deps,
            prompt=conversation.next_prompt,
            messages=[*conversation.history, *conversation.turn_messages],
            model=conversation.model or None,
            usage=conversation.usage or RunUsage(),
            conversation_id=cnv_stream.conversation_id,
        )
        if isinstance(value, SubAgentRequest):
            child_conversation_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{cnv_stream.conversation_id}:{conversation.current_turn_id}:{step_id}:{tool_call_id}",
                )
            )
            child_turn_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{child_conversation_id}:turn:{tool_call_id}",
                )
            )
            parent_request = SubAgentRequestedPayload(
                turn_id=conversation.current_turn_id,
                step_id=step_id,
                tool_call_id=tool_call_id,
                conversation_id=child_conversation_id,
                child_turn_id=child_turn_id,
            )
            cnv_stream.queue_subagent(
                parent_request=parent_request,
                child_agent=value.agent_path,
                child_model=value.model or conversation.model,
                child_deps=value.deps,
                child_prompt=value.prompt,
            )
            return True

        result = _json_value_adapter.validate_json(
            _json_value_adapter.dump_json(value)
        )
        cnv_stream.save_tool_result(
            ToolResultPayload(
                turn_id=conversation.current_turn_id,
                step_id=step_id,
                tool_call_id=tool_call_id,
                result=result,
            ),
            self._call_ids(requests),
        )
        return True

    def _process_subagent_result(
        self,
        cnv_stream: ConversationStream,
        action: SubAgentResultAction,
        conversation: Conversation,
    ) -> bool:
        """Fan a child terminal event into the parent tool result or failure."""
        child_conversation_id = action.child_conversation_id
        child_turn_id = action.child_turn_id
        child_stream = self.dsp_stream.conversation_stream(
            child_conversation_id
        )
        child_terminal: TurnCompletedPayload | TurnFailedPayload | None = None
        for event in child_stream.iter_events():
            if getattr(event.payload, "turn_id", None) != child_turn_id:
                continue
            if event.type == Event.TURN_COMPLETED:
                child_terminal = cast(TurnCompletedPayload, event.payload)
            elif event.type == Event.TURN_FAILED:
                child_terminal = cast(TurnFailedPayload, event.payload)
            if child_terminal is not None:
                break
        if child_terminal is None:
            return False
        if conversation.completed or conversation.failed is not None:
            return True
        request = conversation.subagent_requests.get(action.tool_call_id)
        if (
            request is None
            or request.step_id != action.step_id
            or request.conversation_id != child_conversation_id
            or request.child_turn_id != child_turn_id
        ):
            return True
        if isinstance(child_terminal, TurnFailedPayload):
            self._append_failed(
                cnv_stream,
                conversation,
                RuntimeError(f"Subagent failed: {child_terminal.error}"),
            )
            return True

        if (
            conversation.tool_requests is None
            or conversation.tool_step_id != action.step_id
        ):
            return True
        requests = _requests_adapter.validate_python(conversation.tool_requests)
        cnv_stream.save_tool_result(
            ToolResultPayload(
                turn_id=action.turn_id,
                step_id=action.step_id,
                tool_call_id=action.tool_call_id,
                result=child_terminal.message,
            ),
            self._call_ids(requests),
        )
        return True

    def process(
        self,
        action: DispatchAction,
        cnv_stream: ConversationStream,
        conversation: Conversation,
    ) -> bool:
        """Process one loaded dispatch action and report whether it can be acknowledged."""
        if conversation.completed or conversation.failed is not None:
            return True
        if isinstance(action, SubAgentResultAction):
            return self._process_subagent_result(
                cnv_stream, action, conversation
            )

        agent = reify_agent(conversation.agent_path)
        deps = (
            None
            if not conversation.deps and agent.deps_type is type(None)
            else TypeAdapter(agent.deps_type).validate_python(conversation.deps)
        )
        match action:
            case ConversationPokeAction():
                if conversation.tool_requests is not None:
                    return True
                return self._run_agent_step(
                    agent=agent,
                    conversation=conversation,
                    cnv_stream=cnv_stream,
                    deps=deps,
                    prompt=conversation.next_prompt,
                    requests=None,
                    resume_step_id="initial",
                )
            case AgentResumeAction():
                step_id = action.step_id
                if (
                    conversation.tool_requests is None
                    or conversation.tool_step_id != step_id
                ):
                    return True
                requests = _requests_adapter.validate_python(
                    conversation.tool_requests
                )
                if not set(self._call_ids(requests)) <= set(
                    conversation.tool_results
                ):
                    return False
                return self._run_agent_step(
                    agent=agent,
                    conversation=conversation,
                    cnv_stream=cnv_stream,
                    deps=deps,
                    prompt=None,
                    requests=requests,
                    resume_step_id=step_id,
                )
            case ToolCallAction():
                return self._execute_tool_call(
                    agent=agent,
                    conversation=conversation,
                    cnv_stream=cnv_stream,
                    deps=deps,
                    step_id=action.step_id,
                    tool_call_id=action.tool_call_id,
                )

    def run(self) -> None:
        """Read independent actions until stopped; each action is checkpointed once."""
        try:
            self.dsp_stream.create_group(self.group, id="0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s",
                    self.dsp_stream.name,
                )
                raise
            logger.debug("Consumer group %s already exists", self.group)

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
                self.group,
                self.consumer,
                ">",
                count=1,
                block=500,
            )
            if not entries:
                continue
            action = entries[0]
            if isinstance(action, InvalidDispatchAction):
                logger.warning(
                    "Skipping malformed dispatch fields %s: %s",
                    action.fields,
                    action.error,
                )
                self.dsp_stream.acknowledge(self.group, action.entry_id)
                continue
            conversation_id = action.conversation_id
            turn_id = action.turn_id
            logger.info(
                "Process turn %s in conversation %s", turn_id, conversation_id
            )

            try:
                cnv_stream = self.dsp_stream.conversation_stream(
                    conversation_id
                )
                conversation = reconstruct_turn(
                    cnv_stream.iter_events(),
                    turn_id,
                    conversation_stream=cnv_stream.name,
                )
            except ValueError:
                logger.exception(
                    "Could not load turn %s in conversation %s",
                    turn_id,
                    conversation_id,
                )
                # No trusted turn state is available for a failure event.
                self.dsp_stream.acknowledge(self.group, action.entry_id)
                continue

            try:
                processed = self.process(action, cnv_stream, conversation)
            except RedisError:
                # Redis failures leave the action pending for recovery.
                raise
            except Exception as error:
                logger.exception(
                    "Turn %s failed in conversation %s",
                    turn_id,
                    conversation_id,
                )
                self._append_failed(cnv_stream, conversation, error)
                processed = True
            if processed:
                self.dsp_stream.acknowledge(self.group, action.entry_id)
