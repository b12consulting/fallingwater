"""The host's tool submits contestant turns through Fallingwater."""

import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from fallingwater.catalog import reify_agent
from fallingwater.demo import jeopardy


def test_host_calls_contestant_tool_through_fallingwater(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted: list[str] = []
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

        def ask(self, clue: str) -> str:
            submitted.append(clue)
            return "What is Paris?"

    monkeypatch.setattr(jeopardy.Redis, "from_url", lambda *a, **kw: FakeRedis())
    monkeypatch.setattr(jeopardy, "Chat", FakeChat)
    monkeypatch.delenv("FW_MODEL", raising=False)

    host = reify_agent("fallingwater.demo:jeopardy_host")
    result = host.run_sync(
        "sport", model=TestModel(call_tools=["ask_contestants"]), conversation_id="host"
    )

    assert submitted
    assert len(chats) == 1
    assert chats[0]["agent_path"] == "fallingwater.demo:jeopardy_contestant"
    assert chats[0]["model"] == "test:test"
    assert str(chats[0]["conversation_id"]).startswith("jeopardy_contestant_host_")
    assert reify_agent(chats[0]["agent_path"]) is jeopardy.jeopardy_contestant
    assert "What is Paris?" in str(result.output)


def test_each_clue_gets_a_separate_contestant_conversation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conversations: list[str] = []
    submitted: list[str] = []

    class FakeRedis:
        def __enter__(self) -> "FakeRedis":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    class FakeChat:
        def __init__(self, redis: FakeRedis, *, conversation_id: str, **_kw: object):
            assert isinstance(redis, FakeRedis)
            conversations.append(conversation_id)

        def ask(self, clue: str) -> str:
            submitted.append(clue)
            return f"Response to {clue}"

    monkeypatch.setattr(jeopardy.Redis, "from_url", lambda *a, **kw: FakeRedis())
    monkeypatch.setattr(jeopardy, "Chat", FakeChat)
    ctx = RunContext(
        deps=None, model=TestModel(), usage=RunUsage(), conversation_id="host"
    )
    clues = ["first clue", "second clue", "third clue"]

    results = jeopardy.ask_contestants(ctx, clues)

    assert submitted == clues
    assert len(set(conversations)) == len(clues)
    assert [result["contestant_conversation_id"] for result in results] == conversations
    assert [result["clue"] for result in results] == clues
    assert [result["response"] for result in results] == [
        f"Response to {clue}" for clue in clues
    ]
