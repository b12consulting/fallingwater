"""Local client delivery, background execution, and terminal integration."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Event as ThreadEvent

import pytest
from prompt_toolkit.application import create_app_session
from prompt_toolkit import PromptSession
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from fallingwater.chat import Chat
from fallingwater.local_client import LocalClient
from fallingwater.local_executor import LocalExecutor
from fallingwater.models import TurnCompletedPayload, TurnFailedPayload


@pytest.fixture(autouse=True)
def _app_session():
    with create_app_session():
        yield


def respond(messages, _info):
    prompts = [
        part.content
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]
    return ModelResponse(parts=[TextPart("/".join(prompts))])


writer = Agent(FunctionModel(respond))
failing = Agent(TestModel(call_tools=["fail"]))


@failing.tool_plain
def fail() -> str:
    raise ValueError("tool failed")


def test_ask_is_silent_and_does_not_acknowledge_events(capsys) -> None:
    with LocalClient(agent_path=f"{__name__}:writer") as client:
        assert client.ask("hello") == "hello"
        events = list(client.read(ffwd=True))
        assert len(events) == 3
        assert isinstance(events[-1].payload, TurnCompletedPayload)
        assert list(client.read(ffwd=True)) == events
        for event in events:
            client.acknowledge(event)
        assert list(client.read(ffwd=True)) == []
        assert client.queued_event_id(events[-1].payload.turn_id) == (
            events[1].entry_id
        )
        assert (
            datetime.fromtimestamp(
                int(events[1].entry_id.split("-")[0]) / 1000
            ).date()
            == datetime.now().date()
        )
        snapshot = client.conversation
        snapshot.turn_state.clear()
        assert client.conversation.turn_state
        assert capsys.readouterr() == ("", "")


def test_groups_reopen_and_rewind_without_rerunning_work() -> None:
    with LocalExecutor() as executor:
        with LocalClient(
            executor, agent_path=f"{__name__}:writer", group="one"
        ) as first:
            turn_id = first.send("hello")
            assert first.receive(turn_id) == "hello"
            original = list(first.read(ffwd=True))
            for event in original:
                first.acknowledge(event)
            conversation_id = first.conversation_id
        with LocalClient(
            executor, conversation_id=conversation_id, group="one"
        ) as reopened, LocalClient(
            executor, conversation_id=conversation_id, group="two"
        ) as independent:
            assert list(reopened.read(ffwd=True)) == []
            assert list(independent.read(ffwd=True)) == original
            for event in independent.read(ffwd=True):
                independent.acknowledge(event)
            reopened.rewind()
            assert list(reopened.read(ffwd=True)) == original
            assert list(independent.read(ffwd=True)) == []
            assert len(reopened.conversation.turn_state) == 1
            assert not executor.queue
            assert reopened.ask("again") == "hello/again"


def test_send_is_nonblocking_and_waiting_turn_captures_sequential_history(
    monkeypatch,
) -> None:
    started = ThreadEvent()
    release = ThreadEvent()

    def blocking_response(messages, info):
        started.set()
        assert release.wait(5)
        return respond(messages, info)

    agent = Agent(FunctionModel(blocking_response))
    monkeypatch.setattr(
        "fallingwater.local_executor.reify_agent", lambda _path: agent
    )
    with LocalClient(agent_path=f"{__name__}:writer") as client:
        try:
            first = client.send("first")
            assert started.wait(5)
            second = client.send("second")
            assert second not in client.conversation.turn_state
            with pytest.raises(ValueError, match="No prompt found"):
                client.queued_event_id(second)
            initial = list(client.read(ffwd=True))
            assert len(initial) == 2
            for event in initial:
                client.acknowledge(event)
            with ThreadPoolExecutor() as readers:
                waiting = readers.submit(lambda: list(client.read()))
                release.set()
                assert waiting.result(timeout=5)
            assert client.receive(first) == "first"
            assert client.receive(second) == "first/second"
        finally:
            release.set()


def test_failure_is_delivered_to_receive_and_chat(capsys) -> None:
    with LocalClient(agent_path=f"{__name__}:failing") as client:
        with pytest.raises(RuntimeError, match="tool failed"):
            client.ask("fail")
        events = list(client.read(ffwd=True))
        assert isinstance(events[-1].payload, TurnFailedPayload)
        Chat(client).run_once("try again")
        output = capsys.readouterr().out
        assert output.count("Turn failed: tool failed") == 2


def test_chat_run_once_accepts_local_client(capsys) -> None:
    with LocalClient(agent_path=f"{__name__}:writer") as client:
        Chat(client).run_once("hello")
        output = capsys.readouterr().out
        assert "› hello" in output
        assert "hello" in output
        assert list(client.read(ffwd=True)) == []


def test_terminal_accepts_second_prompt_before_first_answer(
    monkeypatch, capsys
) -> None:
    second_sent = ThreadEvent()

    def wait_for_second(messages, info):
        assert second_sent.wait(5), "Input blocked on agent execution"
        return respond(messages, info)

    agent = Agent(FunctionModel(wait_for_second))
    monkeypatch.setattr(
        "fallingwater.local_executor.reify_agent", lambda _path: agent
    )
    with LocalClient(
        agent_path=f"{__name__}:writer"
    ) as client, create_pipe_input() as pipe, create_app_session(input=pipe):
        session: PromptSession[str] = PromptSession(
            input=pipe, output=DummyOutput(), erase_when_done=True
        )
        monkeypatch.setattr(
            "fallingwater.chat.PromptSession", lambda **_kwargs: session
        )
        send = client.send
        acknowledge = client.acknowledge
        terminal_count = 0

        def submit(prompt):
            turn_id = send(prompt)
            if prompt == "second":
                second_sent.set()
            return turn_id

        def handled(event):
            nonlocal terminal_count
            acknowledge(event)
            if isinstance(
                event.payload, (TurnCompletedPayload, TurnFailedPayload)
            ):
                terminal_count += 1
                if terminal_count == 2:
                    pipe.send_text("\x04")

        monkeypatch.setattr(client, "send", submit)
        monkeypatch.setattr(client, "acknowledge", handled)
        pipe.send_text("first\nsecond\n")
        Chat(client).run()
        assert terminal_count == 2
        assert all(
            turn.completed for turn in client.conversation.turn_state.values()
        )
        assert "first/second" in capsys.readouterr().out


def test_close_waits_for_active_action_without_draining_pending_turns(
    monkeypatch,
) -> None:
    started = ThreadEvent()
    release = ThreadEvent()

    def blocking_response(messages, info):
        started.set()
        assert release.wait(5)
        return respond(messages, info)

    agent = Agent(FunctionModel(blocking_response))
    monkeypatch.setattr(
        "fallingwater.local_executor.reify_agent", lambda _path: agent
    )
    client = LocalClient(agent_path=f"{__name__}:writer")
    try:
        first = client.send("first")
        assert started.wait(5)
        second = client.send("second")
        with ThreadPoolExecutor() as threads:
            closing = threads.submit(client.close)
            try:
                with client.executor.condition:
                    assert client.executor.condition.wait_for(
                        lambda: client.executor._closed, timeout=5
                    )
                assert not closing.done()
            finally:
                release.set()
            closing.result(timeout=5)
        turns = client.conversation.turn_state
        assert turns[first].completed
        assert turns[second].running
        assert client.executor.queue
    finally:
        release.set()
        client.close()


def test_close_wakes_readers_and_stops_owned_executor() -> None:
    client = LocalClient(agent_path=f"{__name__}:writer")
    assert client.ask("hello") == "hello"
    for event in client.read(ffwd=True):
        client.acknowledge(event)
    with ThreadPoolExecutor() as readers:
        waiting = readers.submit(lambda: list(client.read()))
        client.close()
        with pytest.raises(RuntimeError, match="Local client is closed"):
            waiting.result(timeout=5)
    assert client.executor._thread is not None
    assert not client.executor._thread.is_alive()
    with pytest.raises(RuntimeError, match="Local client is closed"):
        client.send("too late")


def test_invalid_inputs_and_unknown_turns() -> None:
    with pytest.raises(ValueError, match="nonempty"):
        LocalClient(agent_path=f"{__name__}:writer", group="")
    with pytest.raises(ValueError, match="agent is required"):
        LocalClient()
    with LocalExecutor() as executor, LocalClient(
        executor, agent_path=f"{__name__}:writer"
    ) as client:
        with pytest.raises(ValueError, match="No prompt found"):
            client.receive("missing")
        with pytest.raises(ValueError, match="dependencies can only"):
            LocalClient(
                executor,
                conversation_id=client.conversation_id,
                deps={"unexpected": "value"},
            )
