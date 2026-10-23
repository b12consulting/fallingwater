"""Run and checkpoint Pydantic AI steps from the Redis dispatch stream."""

import json
from collections.abc import Sequence
from threading import Event
from typing import Any, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic_ai import DeferredToolRequests, DeferredToolResults, RunContext
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from .catalog import reify_agent
from .conversation import ConversationClient
from .runner import _execute_tool, _find_attached_tool, run
from .streams import (
    ConversationCreatedPayload,
    EventType,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
    TurnToolRequestedPayload,
    TurnToolResultPayload,
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
    completed: bool = False
    failed: TurnFailedPayload | None = None


class Proxy:
    """Claim queued turns, run their agent, and publish conversation events."""

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
        self.stop_event = Event()
        self.dispatch_stream = self.client.dispatch_stream
        self.claim_cursor = "0-0"

    def stop(self) -> None:
        """Stop after the active run or tool call reaches a checkpoint."""
        self.stop_event.set()

    def load_conversation(
        self, conversation_stream: str, current_turn_id: str
    ) -> Conversation:
        """Load prior history and any persisted state for the queued turn."""
        history: list[ModelMessage] = []
        turn_messages: list[ModelMessage] = []
        agent_path: str | None = None
        model = ""
        deps: dict[str, Any] = {}
        next_prompt: str | None = None
        usage: RunUsage | None = None
        requests: dict[str, Any] | None = None
        step_id: str | None = None
        tool_results: dict[str, Any] = {}
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
                    requested = cast(TurnToolRequestedPayload, payload)
                    turn_messages.extend(
                        ModelMessagesTypeAdapter.validate_python(requested.history)
                    )
                    usage = requested.usage
                    requests = requested.requests
                    step_id = requested.step_id
                    tool_results = {}
                elif event.type == EventType.TURN_TOOL_RESULT:
                    tool_result = cast(TurnToolResultPayload, payload)
                    if tool_result.step_id == step_id:
                        tool_results[tool_result.tool_call_id] = tool_result.result
                elif event.type == EventType.TURN_COMPLETED:
                    completed = True
                elif event.type == EventType.TURN_FAILED:
                    failed = cast(TurnFailedPayload, payload)

            cursor = f"({entries[-1][0]}"

        if next_prompt is None:
            raise ValueError(f"No prompt found for turn: {current_turn_id}")
        if agent_path is None:
            raise ValueError(
                f"Conversation settings not found in {conversation_stream}"
            )

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
            completed=completed,
            failed=failed,
        )

    def _append_failed(self, stream: str, turn_id: str, error: Exception) -> None:
        self.client.append_event(
            stream,
            EventType.TURN_FAILED,
            TurnFailedPayload(turn_id=turn_id, error=str(error)),
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
        """Rebuild the stable fields of RunContext after a worker restart."""
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

    def _process_deferred_tools(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        conversation_stream: str,
        conversation_id: str,
        deps: Any,
        requests: DeferredToolRequests,
    ) -> DeferredToolResults | None:
        """Save one missing tool result, or collect results ready for resume."""
        if requests.approvals:
            raise NotImplementedError(
                "Fallingwater's deferred runner does not handle tool approvals"
            )
        step_id = conversation.tool_step_id
        if step_id is None:
            raise ValueError("Deferred tool request has no step ID")

        tool_call = next(
            (
                call
                for call in requests.calls
                if call.tool_call_id not in conversation.tool_results
            ),
            None,
        )
        if tool_call is None:
            return requests.build_results(
                calls=conversation.tool_results,
                metadata=requests.metadata,
            )

        tool = _find_attached_tool(agent, tool_call.tool_name)
        args = self._tool_args(tool_call)
        ctx = (
            self._restore_tool_context(
                agent=agent,
                conversation=conversation,
                deps=deps,
                history=[*conversation.history, *conversation.turn_messages],
                tool_call=tool_call,
                usage=conversation.usage or RunUsage(),
                conversation_id=conversation_id,
            )
            if tool.takes_ctx
            else None
        )
        result = _execute_tool(tool, args, ctx)
        result = _json_value_adapter.validate_json(_json_value_adapter.dump_json(result))
        self.client.append_event(
            conversation_stream,
            EventType.TURN_TOOL_RESULT,
            TurnToolResultPayload(
                turn_id=conversation.current_turn_id,
                step_id=step_id,
                tool_call_id=tool_call.tool_call_id,
                result=result,
            ),
        )
        return None

    def _run_agent_step(
        self,
        *,
        agent: Any,
        conversation: Conversation,
        conversation_stream: str,
        conversation_id: str,
        deps: Any,
        prompt: str | None,
        deferred_results: DeferredToolResults | None,
    ) -> bool:
        """Run the agent once and persist its request or final response."""
        result = run(
            agent,
            prompt,
            message_history=[*conversation.history, *conversation.turn_messages],
            deferred_tool_results=deferred_results,
            deps=deps,
            model=conversation.model or None,
            usage=conversation.usage,
            conversation_id=conversation_id,
        )
        turn_messages = [*conversation.turn_messages, *result.new_messages]

        if isinstance(result.output, DeferredToolRequests):
            self.client.append_event(
                conversation_stream,
                EventType.TURN_TOOL_REQUESTED,
                TurnToolRequestedPayload(
                    turn_id=conversation.current_turn_id,
                    step_id=str(uuid4()),
                    requests=_requests_adapter.dump_python(result.output, mode="json"),
                    history=ModelMessagesTypeAdapter.dump_python(
                        result.new_messages, mode="json"
                    ),
                    usage=result.usage,
                ),
            )
            return False

        self.client.append_event(
            conversation_stream,
            EventType.TURN_COMPLETED,
            TurnCompletedPayload(
                turn_id=conversation.current_turn_id,
                message=str(result.output),
                history=ModelMessagesTypeAdapter.dump_python(
                    turn_messages, mode="json"
                ),
                usage=result.usage,
            ),
        )
        logger.info(
            "Sent response for turn %s in conversation %s",
            conversation.current_turn_id,
            conversation_id,
        )
        return True

    def process(self, fields: dict[str, str]) -> bool:
        """Process one step; return whether the turn is complete."""
        current_turn_id = fields["turn_id"]
        conversation_id = fields["conversation_id"]
        conversation_stream = self.client.conversation_stream(conversation_id)

        try:
            conversation = self.load_conversation(conversation_stream, current_turn_id)
        except ValueError as error:
            logger.exception(
                "Could not load turn %s in conversation %s",
                current_turn_id,
                conversation_id,
            )
            self._append_failed(conversation_stream, current_turn_id, error)
            return True

        # A retry after completion or failure only needs to acknowledge dispatch.
        if conversation.completed or conversation.failed is not None:
            return True

        try:
            agent = reify_agent(conversation.agent_path)
            deps = (
                TypeAdapter(agent.deps_type).validate_python(conversation.deps)
                if conversation.deps
                else None
            )
            requests = (
                _requests_adapter.validate_python(conversation.tool_requests)
                if conversation.tool_requests is not None
                else None
            )
            deferred_results = None
            prompt = conversation.next_prompt if requests is None else None
            if requests is not None:
                deferred_results = self._process_deferred_tools(
                    agent=agent,
                    conversation=conversation,
                    conversation_stream=conversation_stream,
                    conversation_id=conversation_id,
                    deps=deps,
                    requests=requests,
                )
                if deferred_results is None:
                    return False

            return self._run_agent_step(
                agent=agent,
                conversation=conversation,
                conversation_stream=conversation_stream,
                conversation_id=conversation_id,
                deps=deps,
                prompt=prompt,
                deferred_results=deferred_results,
            )
        except RedisError:
            # Redis failures must leave the dispatch pending for recovery.
            raise
        except Exception as error:
            logger.exception(
                "Turn %s failed in conversation %s",
                current_turn_id,
                conversation_id,
            )
            self._append_failed(conversation_stream, current_turn_id, error)
            return True

    def run(self) -> None:
        """Read work until stopped, checkpointing each turn step."""
        try:
            self.client.create_group(
                self.dispatch_stream, self.group, id="0", mkstream=True
            )
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception(
                    "Could not create consumer group for %s",
                    self.dispatch_stream,
                )
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
            if claimed:
                entries = claimed
            else:
                entries = self.client.read_dispatch_group(
                    self.group,
                    self.consumer,
                    ">",
                    count=1,
                    block=500,
                )
            for entry_id, fields in entries:
                should_ack = True
                if fields.get("type") == EventType.CONVERSATION_POKE:
                    conversation_id = fields.get("conversation_id")
                    current_turn_id = fields.get("turn_id")
                    try:
                        if not conversation_id or not current_turn_id:
                            raise ValueError(
                                "poke requires conversation_id and turn_id"
                            )
                        validate_conversation_id(conversation_id)
                    except ValueError as error:
                        logger.warning(
                            "Skipping malformed dispatch entry %s: %s",
                            entry_id,
                            error,
                        )
                    else:
                        logger.info(
                            "Received poke for turn %s in conversation %s",
                            current_turn_id,
                            conversation_id,
                        )
                        should_ack = False
                        while not self.stop_event.is_set():
                            should_ack = self.process(fields)
                            if should_ack:
                                break
                if should_ack:
                    self.client.acknowledge(self.dispatch_stream, self.group, entry_id)
