"""Embedded FIFO scheduling, delegation, failure, and replay checks."""

from threading import Thread
import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from fallingwater.conversation import Conversation
from fallingwater.local_executor import LocalExecutor
from fallingwater.models import (
    AgentRequest,
    ToolResultPayload,
    TurnFailedPayload,
)
from fallingwater.archive import (
    save_conversation,
    save_events,
    load_conversation,
    load_events,
    load_local_conversation,
    save_local_conversation,
)


def prompts(messages) -> list[str]:
    return [
        part.content
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]


def writer_response(messages, _info):
    return ModelResponse(parts=[TextPart("/".join(prompts(messages)))])


writer = Agent(FunctionModel(writer_response), name="writer")
middle = Agent(TestModel(call_tools=["ask_writer"]))


@middle.tool_plain
def ask_writer() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:writer", prompt="nested")


def editor_response(messages, _info):
    if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
        return ModelResponse(
            parts=[
                TextPart(
                    "|".join(str(part.content) for part in messages[-1].parts)
                )
            ]
        )
    return ModelResponse(
        parts=[
            ToolCallPart("delegate", {"prompt": "draft"}, tool_call_id="a"),
            ToolCallPart("delegate", {"prompt": "revise"}, tool_call_id="b"),
            ToolCallPart("nested", {}, tool_call_id="c"),
        ]
    )


editor = Agent(FunctionModel(editor_response))


@editor.tool_plain
def delegate(prompt: str) -> AgentRequest:
    return AgentRequest(
        agent_path=f"{__name__}:writer", persistent=True, prompt=prompt
    )


@editor.tool_plain
def nested() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:middle", prompt="delegate")


failing = Agent(TestModel(call_tools=["fail", "unused"]))


@failing.tool_plain
def fail() -> str:
    raise ValueError("broken tool")


@failing.tool_plain
def unused() -> str:
    pytest.fail("A failed turn must not execute leftover tool actions")


parent = Agent(TestModel(call_tools=["ask_failure"]))


@parent.tool_plain
def ask_failure() -> AgentRequest:
    return AgentRequest(agent_path=f"{__name__}:failing", prompt="fail")


def test_serial_history_events_and_independent_sessions(capsys) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        other = executor.create(f"{__name__}:writer")
        first = executor.send(root, "first")
        second = executor.send(root, "second")
        independent = executor.send(other, "other")
        assert second not in executor.registry[root].conversation.turn_state

        executor.drain()

        conversation = executor.registry[root].conversation
        assert conversation.turn_state[first].state.message == "first"
        assert conversation.turn_state[second].state.message == "first/second"
        assert (
            executor.registry[other]
            .conversation.turn_state[independent]
            .state.message
            == "other"
        )
        replayed = Conversation(
            executor.registry[root].events.values(), conversation_id=root
        )
        assert replayed.turn_state[second].state.message == "first/second"
        assert not executor.queue
        assert not executor.registry[root].pending
        assert capsys.readouterr() == ("", "")


def test_nested_fan_in_and_named_child_reuse() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        first = executor.send(root, "edit")
        executor.drain()
        conversation = executor.registry[root].conversation
        answer = conversation.turn_state[first].state.message
        assert "draft|draft/revise|" in answer
        assert "nested" in answer
        child_id = conversation.agent_by_name["writer"]
        second = executor.send(root, "edit again")
        executor.drain()
        assert "draft/revise/draft" in (
            conversation.turn_state[second].state.message
        )
        assert conversation.agent_by_name["writer"] == child_id
        assert len(executor.registry[child_id].conversation.turn_state) == 4
        for session in executor.registry.values():
            replayed = Conversation(
                session.events.values(),
                conversation_id=session.conversation.conversation_id,
            )
            assert all(turn.completed for turn in replayed.turn_state.values())
            assert not session.pending
            assert not session.tool_contexts


def test_export_restores_nested_and_persistent_sessions(monkeypatch) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        executor.send(root, "edit")
        executor.drain()
        unrelated = executor.create(f"{__name__}:writer")
        child = executor.registry[root].conversation.agent_by_name["writer"]
        original = {
            key: list(session.events.values())
            for key, session in executor.registry.items()
            if key != unrelated
        }
        snapshots = executor.export(root)
    assert unrelated not in snapshots
    assert set(snapshots) == set(original)
    with LocalExecutor() as restored:
        restored.restore(snapshots)
        assert set(restored.registry) == set(original)
        assert not restored.queue
        for key, events in original.items():
            assert list(restored.registry[key].events.values()) == events
        monkeypatch.setattr("fallingwater.local_executor.time_ns", lambda: 0)
        second = restored.send(root, "again")
        restored.drain()
        conversation = restored.registry[root].conversation
        assert conversation.agent_by_name["writer"] == child
        assert "draft/revise/draft" in (
            conversation.turn_state[second].state.message
        )
        for key, session in restored.registry.items():
            assert list(session.events) == sorted(
                session.events,
                key=lambda value: tuple(map(int, value.split("-"))),
            )


def test_export_snapshot_is_independent_of_later_turns() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        executor.send(root, "first")
        executor.drain()
        snapshots = executor.export(root)
        executor.send(root, "second")
        executor.drain()
    with LocalExecutor() as restored:
        restored.restore(snapshots)
        turn = restored.send(root, "next")
        restored.drain()
        assert (
            restored.registry[root].conversation.turn_state[turn].state.message
            == "first/next"
        )


def test_restore_requires_all_referenced_children_before_installing() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        executor.send(root, "edit")
        executor.drain()
        snapshots = executor.export(root)
        child = executor.registry[root].conversation.agent_by_name["writer"]
    del snapshots[child]
    with LocalExecutor() as restored:
        with pytest.raises(ValueError, match="Child conversation not supplied"):
            restored.restore(snapshots)
        assert not restored.registry


def test_redis_archive_loads_and_continues_locally(
    tmp_path, isolated_redis
) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        executor.send(root, "redis history")
        executor.drain()
        save_local_conversation(executor, root, str(tmp_path))
    load_conversation(isolated_redis, "fw-test", root, str(tmp_path))
    redis_store = tmp_path / "redis"
    redis_store.mkdir()
    save_conversation(isolated_redis, "fw-test", root, str(redis_store))
    with LocalExecutor() as executor:
        load_local_conversation(executor, root, str(redis_store))
        turn = executor.send(root, "local continuation")
        executor.drain()
        assert (
            executor.registry[root].conversation.turn_state[turn].state.message
            == "redis history/local continuation"
        )


def test_save_after_close_retains_unfinished_events(tmp_path) -> None:
    executor = LocalExecutor()
    root = executor.create(f"{__name__}:writer")
    executor.send(root, "active")
    executor.send(root, "waiting")
    executor.close()
    save_local_conversation(executor, root, str(tmp_path))
    assert len(list(load_events(root, str(tmp_path)))) == 2
    with LocalExecutor() as restored:
        with pytest.raises(ValueError, match="unfinished turns"):
            load_local_conversation(restored, root, str(tmp_path))
        assert not restored.registry


def test_load_missing_child_does_not_install_partial_tree(tmp_path) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        executor.send(root, "edit")
        executor.drain()
        save_local_conversation(executor, root, str(tmp_path))
        child = executor.registry[root].conversation.agent_by_name["writer"]
    (tmp_path / f"{child}.jsonl.zst").unlink()
    with LocalExecutor() as restored:
        with pytest.raises(ValueError, match="not found"):
            load_local_conversation(restored, root, str(tmp_path))
        assert not restored.registry


def test_failed_conversation_can_be_saved_and_loaded(tmp_path) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:failing")
        turn = executor.send(root, "fail")
        executor.drain()
        save_local_conversation(executor, root, str(tmp_path))
    with LocalExecutor() as restored:
        load_local_conversation(restored, root, str(tmp_path))
        assert restored.registry[root].conversation.turn_state[turn].failed
        assert not restored.queue


@pytest.mark.parametrize("entry_id", ["0-0", "1-0"])
def test_load_rejects_nonincreasing_ids(tmp_path, entry_id) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        executor.send(root, "first")
        executor.drain()
        events = list(executor.registry[root].events.values())
    events[0] = events[0].model_copy(update={"entry_id": "1-0"})
    events[1] = events[1].model_copy(update={"entry_id": entry_id})
    save_events(events, root, str(tmp_path))
    with LocalExecutor() as restored:
        with pytest.raises(ValueError, match="Event IDs"):
            load_local_conversation(restored, root, str(tmp_path))
        assert not restored.registry


def test_load_refuses_existing_child_without_installing_parent(
    tmp_path,
) -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:editor")
        executor.send(root, "edit")
        executor.drain()
        save_local_conversation(executor, root, str(tmp_path))
        child = executor.registry[root].conversation.agent_by_name["writer"]
    with LocalExecutor() as restored:
        restored.create(f"{__name__}:writer", conversation_id=child)
        with pytest.raises(ValueError, match="already exists"):
            load_local_conversation(restored, root, str(tmp_path))
        assert set(restored.registry) == {child}


def test_failed_child_propagates_and_next_turns_continue() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:parent")
        first = executor.send(root, "first")
        second = executor.send(root, "second")
        other = executor.create(f"{__name__}:writer")
        independent = executor.send(other, "unaffected")
        executor.drain()

        session = executor.registry[root]
        for turn_id in (first, second):
            payload = session.conversation.turn_state[turn_id].state
            assert isinstance(payload, TurnFailedPayload)
            assert payload.error == "Subagent failed: broken tool"
        assert (
            executor.registry[other]
            .conversation.turn_state[independent]
            .completed
        )
        assert not executor.queue
        assert all(not item.pending for item in executor.registry.values())
        assert all(
            not item.tool_contexts for item in executor.registry.values()
        )


def test_invalid_record_does_not_change_event_log() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        turn_id = executor.send(root, "hello")
        before = list(executor.registry[root].events)
        with pytest.raises(ValueError):
            executor.record(
                root,
                ToolResultPayload(
                    turn_id=turn_id,
                    step_id="missing",
                    tool_call_id="missing",
                    result="invalid",
                ),
            )
        assert list(executor.registry[root].events) == before
        executor.drain()


def test_incoming_prompts_are_applied_only_by_the_execution_loop() -> None:
    with LocalExecutor() as executor:
        root = executor.create(f"{__name__}:writer")
        turn, result = executor.enqueue(root, "first")
        assert turn not in executor.registry[root].conversation.turn_state
        assert len(executor.events(root)) == 1
        thread = Thread(target=executor.run)
        thread.start()
        try:
            assert result.result(timeout=5) == "first"
            following, answer = executor.enqueue(root, "second")
            assert answer.result(timeout=5) == "first/second"
            assert executor.snapshot(root).turn_state[following].completed
        finally:
            executor.close()
            thread.join(timeout=5)
        assert not thread.is_alive()
