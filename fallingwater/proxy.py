"""Run and checkpoint Pydantic AI actions from the Redis dispatch stream."""

import json
from collections.abc import Sequence
from threading import Event as ThreadEvent, Thread
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic_ai import DeferredToolRequests, DeferredToolResults, RunContext
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import LockError, RedisError, ResponseError

from .catalog import reify_agent
from .conversation import ConversationClient
from .runner import (
    SubAgentRequest,
    _execute_tool,
    _find_attached_tool,
    run,
)
from .streams import (
    ConversationCreatedPayload,
    DispatchType,
    EventType,
    ParentToolReference,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    SubAgentRequestedPayload,
    ToolRequestedPayload,
    ToolResultPayload,
    validate_conversation_id,
)
from .utils import logger


_requests_adapter = TypeAdapter(DeferredToolRequests)
_json_value_adapter = TypeAdapter(Any)


class Conversation(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    agent_path: str
    model: str
    next_prompt: str
    current_turn_id: str
    history: Sequence[ModelMessage]
    deps: dict[str, Any]
    usage: RunUsage | None
    turn_messages: Sequence[ModelMessage]
    tool_requests: dict[str, Any] | None
    tool_step_id: str | None
    tool_results: dict[str, Any]
    subagent_requests: dict[str, SubAgentRequestedPayload]
    parent: ParentToolReference | None = None
    completed: bool = False
    failed: TurnFailedPayload | None = None


class Proxy:
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
        self.client = ConversationClient(redis, namespace=namespace)
        self.claim_idle_ms = claim_idle_ms
        self.consumer = str(uuid4())
        self.stop_event = ThreadEvent()
        self.dispatch_stream = self.client.dispatch_stream
        self.claim_cursor = "0-0"

    def stop(self) -> None:
        """Stop reading more actions after the active action is checkpointed."""
        self.stop_event.set()

    def load_conversation(
        self, conversation_stream: str, current_turn_id: str
    ) -> Conversation:
        """Rebuild the selected turn and its current deferred-call state."""
        history: list[ModelMessage] = []
        turn_messages: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        deps: dict[str, Any] = {}
        parent: ParentToolReference | None = None
        next_prompt: str | None = None
        usage: RunUsage | None = None
        requests: dict[str, Any] | None = None
        step_id: str | None = None
        tool_results: dict[str, Any] = {}
        subagent_requests: dict[str, SubAgentRequestedPayload] = {}
        completed = False
        failed: TurnFailedPayload | None = None
        queued = False

        cursor = "-"
        while True:
            entries = self.client.read_conversation_range(
                conversation_stream, min=cursor, count=100
            )
            if not entries:
                break
            for _entry_id, event in entries:
                if event.type == EventType.CONVERSATION_CREATED:
                    created = cast(ConversationCreatedPayload, event.payload)
                    agent_path = created.agent
                    model = created.model
                    deps = created.deps
                    parent = created.parent
                    continue

                if event.type == EventType.TURN_QUEUED:
                    queued_payload = cast(TurnQueuedPayload, event.payload)
                    if queued_payload.turn_id == current_turn_id and not queued:
                        next_prompt = queued_payload.message
                        queued = True
                    continue

                if not queued:
                    if event.type == EventType.TURN_COMPLETED:
                        previous = cast(TurnCompletedPayload, event.payload)
                        history.extend(
                            ModelMessagesTypeAdapter.validate_python(previous.history)
                        )
                        usage = previous.usage
                    continue

                payload = event.payload
                if getattr(payload, "turn_id", None) != current_turn_id:
                    continue
                if event.type == EventType.TURN_TOOL_REQUESTED:
                    requested = cast(ToolRequestedPayload, payload)
                    turn_messages.extend(
                        ModelMessagesTypeAdapter.validate_python(requested.history)
                    )
                    usage = requested.usage
                    requests = requested.requests
                    step_id = requested.step_id
                    tool_results = {}
                    subagent_requests = {}
                elif event.type == EventType.TURN_TOOL_RESULT:
                    tool_result = cast(ToolResultPayload, payload)
                    if tool_result.step_id == step_id:
                        tool_results[tool_result.tool_call_id] = tool_result.result
                elif event.type == EventType.TURN_SUBAGENT_REQUESTED:
                    subagent = cast(SubAgentRequestedPayload, payload)
                    if subagent.step_id == step_id:
                        subagent_requests[subagent.tool_call_id] = subagent
                elif event.type == EventType.TURN_COMPLETED:
                    completed = True
                elif event.type == EventType.TURN_FAILED:
                    failed = cast(TurnFailedPayload, payload)

            cursor = f"({entries[-1][0]}"

        if next_prompt is None:
            raise ValueError(f"No prompt found for turn: {current_turn_id}")
        if agent_path is None:
            raise ValueError(f"Conversation settings not found in {conversation_stream}")

        return Conversation(
            agent_path=agent_path,
            model=model,
            next_prompt=next_prompt,
            current_turn_id=current_turn_id,
            history=history,
            deps=deps,
            usage=usage,
            turn_messages=turn_messages,
            tool_requests=requests,
            tool_step_id=step_id,
            tool_results=tool_results,
            subagent_requests=subagent_requests,
            parent=parent,
            completed=completed,
            failed=failed,
        )

    def _append_failed(
        self,
        conversation_id: str,
        conversation: Conversation,
        error: Exception,
    ) -> None:
        self.client.finish_turn(
            EventType.TURN_FAILED,
            TurnFailedPayload(turn_id=conversation.current_turn_id, error=str(error)),
            parent=conversation.parent,
            conversation_id=conversation_id,
        )

    @staticmethod
    def _tool_args(tool_call: Any) -> dict[str, Any]:
        args = tool_call.args
        if isinstance(args, str):
            args = json.loads(args)
        if not isinstance(args, dict):
            raise ValueError(f"Invalid arguments for tool {tool_call.tool_name!r}")
        return args

    def _restore_tool_context(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        deps: Any,
        history: Sequence[ModelMessage],
        tool_call: Any,
        usage: RunUsage,
        conversation_id: str,
    ) -> RunContext[Any]:
        """Rebuild stable RunContext fields after a worker restart."""
        model = agent._get_model_outside_run(conversation.model or None)
        return RunContext(
            deps=deps,
            model=model,
            usage=usage,
            agent=agent,
            prompt=conversation.next_prompt,
            messages=list(history),
            tool_call_id=tool_call.tool_call_id,
            tool_name=tool_call.tool_name,
            conversation_id=conversation_id,
        )

    @staticmethod
    def _call_ids(requests: DeferredToolRequests) -> list[str]:
        if requests.approvals:
            raise NotImplementedError(
                "Fallingwater does not handle deferred tool approvals"
            )
        return [call.tool_call_id for call in requests.calls]

    def _claim_run(self, conversation_id: str, turn_id: str, step_id: str):
        """Acquire a renewable per-step lease to suppress duplicate model runs."""
        lock_name = (
            f"{self.client.namespace}:agent-run:{conversation_id}:{turn_id}:{step_id}"
        )
        lease_seconds = max(5.0, min(self.claim_idle_ms / 1000 or 5.0, 60.0))
        lock = self.client.redis.lock(
            lock_name,
            timeout=lease_seconds,
            blocking=False,
            thread_local=False,
        )
        if not lock.acquire(blocking=False):
            return None

        stop_heartbeat = ThreadEvent()
        lease_lost = ThreadEvent()

        def renew() -> None:
            while not stop_heartbeat.wait(lease_seconds / 3):
                try:
                    if not lock.extend(lease_seconds, replace_ttl=True):
                        lease_lost.set()
                        return
                except RedisError:
                    lease_lost.set()
                    return

        heartbeat = Thread(target=renew, daemon=True)
        heartbeat.start()
        return lock, heartbeat, stop_heartbeat, lease_lost

    @staticmethod
    def _release_run_claim(claim: Any) -> None:
        lock, heartbeat, stop_heartbeat, _lease_lost = claim
        stop_heartbeat.set()
        heartbeat.join()
        try:
            lock.release()
        except LockError:
            pass

    def _run_agent_step(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        conversation_id: str,
        deps: Any,
        prompt: str | None,
        requests: DeferredToolRequests | None,
        resume_step_id: str,
    ) -> bool:
        """Run once and atomically queue tool jobs or save a terminal result."""
        claim = self._claim_run(
            conversation_id, conversation.current_turn_id, resume_step_id
        )
        if claim is None:
            return False
        try:
            _lock, _heartbeat, _stop, lease_lost = claim
            # The action can be redelivered after a prior worker checkpointed it.
            refreshed = self.load_conversation(
                self.client.conversation_stream(conversation_id),
                conversation.current_turn_id,
            )
            if refreshed.completed or refreshed.failed is not None:
                return True
            if requests is None:
                if refreshed.tool_requests is not None:
                    return True
            elif refreshed.tool_step_id != resume_step_id:
                return True
            elif not set(self._call_ids(requests)) <= set(refreshed.tool_results):
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
                conversation_id=conversation_id,
            )
            if lease_lost.is_set():
                raise RedisError("agent run lease was lost while running")

            if isinstance(result.output, DeferredToolRequests):
                step_id = str(uuid4())
                request_payload = ToolRequestedPayload(
                    turn_id=refreshed.current_turn_id,
                    step_id=step_id,
                    requests=_requests_adapter.dump_python(result.output, mode="json"),
                    history=ModelMessagesTypeAdapter.dump_python(
                        result.new_messages, mode="json"
                    ),
                    usage=result.usage,
                )
                self.client.queue_tool_requests(
                    request_payload,
                    self._call_ids(result.output),
                    conversation_id=conversation_id,
                )
                return True

            self.client.finish_turn(
                EventType.TURN_COMPLETED,
                TurnCompletedPayload(
                    turn_id=refreshed.current_turn_id,
                    message=str(result.output),
                    history=ModelMessagesTypeAdapter.dump_python(
                        [*refreshed.turn_messages, *result.new_messages], mode="json"
                    ),
                    usage=result.usage,
                ),
                parent=refreshed.parent,
                conversation_id=conversation_id,
            )
            logger.info(
                "Sent response for turn %s in conversation %s",
                refreshed.current_turn_id,
                conversation_id,
            )
            return True
        finally:
            self._release_run_claim(claim)

    def _execute_tool_call(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        conversation_id: str,
        deps: Any,
        step_id: str,
        tool_call_id: str,
    ) -> bool:
        """Execute one deferred tool and persist its result or child request."""
        if conversation.tool_step_id != step_id or conversation.tool_requests is None:
            return True
        requests = _requests_adapter.validate_python(conversation.tool_requests)
        call = next(
            (call for call in requests.calls if call.tool_call_id == tool_call_id), None
        )
        if call is None:
            return True
        if tool_call_id in conversation.tool_results or tool_call_id in conversation.subagent_requests:
            return True

        tool = _find_attached_tool(agent, call.tool_name)
        args = self._tool_args(call)
        ctx = (
            self._restore_tool_context(
                agent=agent,
                conversation=conversation,
                deps=deps,
                history=[*conversation.history, *conversation.turn_messages],
                tool_call=call,
                usage=conversation.usage or RunUsage(),
                conversation_id=conversation_id,
            )
            if tool.takes_ctx
            else None
        )
        value = _execute_tool(tool, args, ctx)
        if isinstance(value, SubAgentRequest):
            child_conversation_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"fallingwater:{conversation_id}:{conversation.current_turn_id}:{step_id}:{tool_call_id}",
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
            self.client.queue_subagent(
                parent_request=parent_request,
                child_agent=value.agent_path,
                child_model=value.model or conversation.model,
                child_deps=value.deps,
                child_prompt=value.prompt,
                parent_conversation_id=conversation_id,
            )
            return True

        result = _json_value_adapter.validate_json(_json_value_adapter.dump_json(value))
        self.client.save_tool_result(
            ToolResultPayload(
                turn_id=conversation.current_turn_id,
                step_id=step_id,
                tool_call_id=tool_call_id,
                result=result,
            ),
            self._call_ids(requests),
            conversation_id=conversation_id,
        )
        return True

    def _process_subagent_result(
        self, fields: dict[str, str], conversation: Conversation
    ) -> bool:
        """Fan a child terminal event into the parent tool result or failure."""
        child_conversation_id = fields["child_conversation_id"]
        child_turn_id = fields["child_turn_id"]
        child_stream = self.client.conversation_stream(child_conversation_id)
        child_terminal: TurnCompletedPayload | TurnFailedPayload | None = None
        for _entry_id, event in self.client.read_conversation_range(child_stream):
            if getattr(event.payload, "turn_id", None) != child_turn_id:
                continue
            if event.type == EventType.TURN_COMPLETED:
                child_terminal = cast(TurnCompletedPayload, event.payload)
            elif event.type == EventType.TURN_FAILED:
                child_terminal = cast(TurnFailedPayload, event.payload)
        if child_terminal is None:
            return False
        if conversation.completed or conversation.failed is not None:
            return True
        request = conversation.subagent_requests.get(fields["tool_call_id"])
        if (
            request is None
            or request.step_id != fields["step_id"]
            or request.conversation_id != child_conversation_id
            or request.child_turn_id != child_turn_id
        ):
            return True
        if isinstance(child_terminal, TurnFailedPayload):
            self._append_failed(
                fields["conversation_id"],
                conversation,
                RuntimeError(f"Subagent failed: {child_terminal.error}"),
            )
            return True

        if conversation.tool_requests is None or conversation.tool_step_id != fields["step_id"]:
            return True
        requests = _requests_adapter.validate_python(conversation.tool_requests)
        self.client.save_tool_result(
            ToolResultPayload(
                turn_id=fields["turn_id"],
                step_id=fields["step_id"],
                tool_call_id=fields["tool_call_id"],
                result=child_terminal.message,
            ),
            self._call_ids(requests),
            conversation_id=fields["conversation_id"],
        )
        return True

    def process(self, fields: dict[str, str]) -> bool:
        """Process one dispatch action and return whether it can be acknowledged."""
        action = fields.get("type", DispatchType.CONVERSATION_POKE)
        conversation_id = fields.get("conversation_id")
        turn_id = fields.get("turn_id")
        logger.info("Process turn %s in conversation %s", turn_id, conversation_id)

        if not conversation_id or not turn_id:
            logger.warning("Skipping malformed dispatch fields: %s", fields)
            return True
        try:
            validate_conversation_id(conversation_id)
            conversation_stream = self.client.conversation_stream(conversation_id)
            conversation = self.load_conversation(conversation_stream, turn_id)
        except ValueError as error:
            logger.exception("Could not load turn %s in conversation %s", turn_id, conversation_id)
            # A malformed existing turn can still be marked failed only when its
            # queued record and conversation metadata were readable.
            return True

        if conversation.completed or conversation.failed is not None:
            return True

        try:
            agent = reify_agent(conversation.agent_path)
            deps = (
                None
                if not conversation.deps and agent.deps_type is type(None)
                else TypeAdapter(agent.deps_type).validate_python(conversation.deps)
            )
            if action == DispatchType.CONVERSATION_POKE:
                if conversation.tool_requests is not None:
                    return True
                return self._run_agent_step(
                    agent=agent,
                    conversation=conversation,
                    conversation_id=conversation_id,
                    deps=deps,
                    prompt=conversation.next_prompt,
                    requests=None,
                    resume_step_id="initial",
                )
            if action == DispatchType.AGENT_RESUME:
                step_id = fields["step_id"]
                if conversation.tool_requests is None or conversation.tool_step_id != step_id:
                    return True
                requests = _requests_adapter.validate_python(conversation.tool_requests)
                if not set(self._call_ids(requests)) <= set(conversation.tool_results):
                    return False
                return self._run_agent_step(
                    agent=agent,
                    conversation=conversation,
                    conversation_id=conversation_id,
                    deps=deps,
                    prompt=None,
                    requests=requests,
                    resume_step_id=step_id,
                )
            if action == DispatchType.TOOL_CALL:
                return self._execute_tool_call(
                    agent=agent,
                    conversation=conversation,
                    conversation_id=conversation_id,
                    deps=deps,
                    step_id=fields["step_id"],
                    tool_call_id=fields["tool_call_id"],
                )
            if action == DispatchType.SUBAGENT_RESULT:
                return self._process_subagent_result(fields, conversation)
            logger.warning("Skipping unknown dispatch action %r", action)
            return True
        except RedisError:
            # Redis failures leave the action pending for recovery.
            raise
        except Exception as error:
            logger.exception("Turn %s failed in conversation %s", turn_id, conversation_id)
            self._append_failed(conversation_id, conversation, error)
            return True

    def run(self) -> None:
        """Read independent actions until stopped; each action is checkpointed once."""
        try:
            self.client.create_group(
                self.dispatch_stream, self.group, id="0", mkstream=True
            )
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception("Could not create consumer group for %s", self.dispatch_stream)
                raise
            logger.debug("Consumer group %s already exists", self.group)

        while not self.stop_event.is_set():
            claim_cursor, claimed, _deleted = self.client.claim_dispatch(
                self.group,
                self.consumer,
                min_idle_time=self.claim_idle_ms,
                start_id=self.claim_cursor,
                count=1,
            )
            self.claim_cursor = claim_cursor
            entries = claimed or self.client.read_dispatch_group(
                self.group,
                self.consumer,
                ">",
                count=1,
                block=500,
            )
            for entry_id, fields in entries:
                if self.process(fields):
                    self.client.acknowledge(self.dispatch_stream, self.group, entry_id)
