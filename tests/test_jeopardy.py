"""The host's contestant tool submits idempotent child turns."""

from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from fallingwater.demo import jeopardy


def test_contestant_tool_uses_a_separate_stable_conversation(monkeypatch) -> None:
    submitted: list[tuple[str, str]] = []
    chats: list[dict[str, object]] = []

    class FakeRedis:
        def __enter__(self) -> "FakeRedis":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class FakeChat:
        def __init__(self, redis: FakeRedis, **kwargs: object) -> None:
            assert isinstance(redis, FakeRedis)
            chats.append(kwargs)

        def ask(self, clue: str, *, turn_id: str) -> str:
            submitted.append((clue, turn_id))
            return "What is Paris?"

    monkeypatch.setattr(jeopardy.Redis, "from_url", lambda *a, **kw: FakeRedis())
    monkeypatch.setattr(jeopardy, "Chat", FakeChat)
    monkeypatch.delenv("FW_MODEL", raising=False)

    ctx = RunContext(
        deps=None,
        model=TestModel(),
        usage=RunUsage(),
        conversation_id="host-conversation",
        tool_call_id="parent-tool-call",
    )
    result = jeopardy.ask_contestant(ctx, "sport clue")

    assert result == "What is Paris?"
    assert len(submitted) == 1
    assert submitted[0][0] == "sport clue"
    assert chats[0]["agent_path"] == "fallingwater.demo:jeopardy_contestant"
    assert chats[0]["model"] == "test:test"
    assert chats[0]["conversation_id"]


def test_each_tool_call_uses_a_distinct_repeatable_child_turn(monkeypatch) -> None:
    submitted: list[tuple[str, str]] = []
    conversations: list[str] = []

    class FakeRedis:
        def __enter__(self) -> "FakeRedis":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class FakeChat:
        def __init__(self, redis: FakeRedis, **kwargs: object):
            assert isinstance(redis, FakeRedis)
            conversations.append(str(kwargs["conversation_id"]))

        def ask(self, clue: str, *, turn_id: str) -> str:
            submitted.append((clue, turn_id))
            return f"Response to {clue}"

    monkeypatch.setattr(jeopardy.Redis, "from_url", lambda *a, **kw: FakeRedis())
    monkeypatch.setattr(jeopardy, "Chat", FakeChat)

    def context(tool_call_id: str) -> RunContext[None]:
        return RunContext(
            deps=None,
            model=TestModel(),
            usage=RunUsage(),
            conversation_id="host-conversation",
            tool_call_id=tool_call_id,
        )

    calls = [
        ("call-1", "first clue"),
        ("call-2", "second clue"),
        ("call-3", "third clue"),
    ]
    first = [jeopardy.ask_contestant(context(call_id), clue) for call_id, clue in calls]
    first_ids = list(zip(conversations, submitted, strict=True))
    second = [
        jeopardy.ask_contestant(context(call_id), clue) for call_id, clue in calls
    ]
    second_ids = list(
        zip(conversations[len(calls) :], submitted[len(calls) :], strict=True)
    )

    assert first == second == [f"Response to {clue}" for _, clue in calls]
    assert len(set(conversations[: len(calls)])) == len(calls)
    assert first_ids == second_ids
