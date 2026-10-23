"""Checks for loading and processing turns from conversation events."""

from unittest.mock import MagicMock

from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    UserPromptPart,
)
from pydantic_ai.usage import RunUsage
from redis import Redis

from fallingwater.proxy import Proxy
from fallingwater.streams import (
    ConversationCreatedPayload,
    ConversationPayload,
    EventType,
    TurnCompletedPayload,
    TurnQueuedPayload,
    decode_conversation_event,
    encode_conversation_event,
)


def _entry(entry_id: str, event_type: EventType, payload: ConversationPayload):
    return (entry_id, encode_conversation_event(event_type, payload))


def _messages(prompt: str) -> list[ModelRequest]:
    return [ModelRequest(parts=[UserPromptPart(content=prompt)])]


def test_completed_turn_stores_messages_from_agent_run(
    monkeypatch,
) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        _entry(
            "1-0",
            EventType.CONVERSATION_CREATED,
            ConversationCreatedPayload(agent="example:agent", model="test"),
        ),
        _entry(
            "2-0",
            EventType.TURN_QUEUED,
            TurnQueuedPayload(turn_id="turn-1", message="hello"),
        ),
    ]
    result = MagicMock(
        output="reply",
        usage=RunUsage(),
        new_messages=_messages("hello"),
    )
    agent = MagicMock()
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: agent)
    monkeypatch.setattr("fallingwater.proxy.reify_tool_executors", lambda _path: {})
    run_agent = MagicMock(return_value=result)
    monkeypatch.setattr("fallingwater.proxy.run_until_complete", run_agent)

    Proxy(redis).process({"turn_id": "turn-1", "conversation_id": "abc"})

    run_agent.assert_called_once()
    assert run_agent.call_args.args == (agent, "hello")
    assert run_agent.call_args.kwargs["model"] == "test"
    redis.xadd.assert_called_once()
    _stream, fields = redis.xadd.call_args.args
    completed = decode_conversation_event(fields)
    assert completed.type == EventType.TURN_COMPLETED
    assert completed.payload.message == "reply"
    restored = ModelMessagesTypeAdapter.validate_python(completed.payload.history)
    assert restored[0].parts[0].content == "hello"


def test_agent_failure_is_recorded_as_turn_failed(monkeypatch) -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = [
        _entry(
            "1-0",
            EventType.CONVERSATION_CREATED,
            ConversationCreatedPayload(agent="example:agent"),
        ),
        _entry(
            "2-0",
            EventType.TURN_QUEUED,
            TurnQueuedPayload(turn_id="turn-1", message="hello"),
        ),
    ]
    monkeypatch.setattr("fallingwater.proxy.reify_agent", lambda _path: object())
    monkeypatch.setattr("fallingwater.proxy.reify_tool_executors", lambda _path: {})

    def fail_run(*_args, **_kwargs):
        raise RuntimeError("agent failed")

    monkeypatch.setattr("fallingwater.proxy.run_until_complete", fail_run)
    Proxy(redis).process({"turn_id": "turn-1", "conversation_id": "abc"})

    _stream, fields = redis.xadd.call_args.args
    failed = decode_conversation_event(fields)
    assert failed.type == EventType.TURN_FAILED
    assert failed.payload.error == "agent failed"
