"""Checks for Redis conversation access without terminal interaction."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent
from unittest.mock import MagicMock

import pytest
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
)
from pydantic_ai.usage import RunUsage
from redis import Redis
from redis.exceptions import RedisError

from fallingwater.redis_client import RedisClient
from fallingwater.models import (
    Action,
    ConversationCreatedPayload,
    Event,
    TurnCompletedPayload,
    TurnFailedPayload,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import (
    decode_conversation_event,
    encode_conversation_event,
)


def _client(redis: Redis, **kwargs) -> RedisClient:
    redis.xrange.return_value = []
    redis.xadd.return_value = "1-0"
    return RedisClient(
        redis,
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
        **kwargs,
    )


def _queued(turn_id: str):
    return encode_conversation_event(
        Event.TURN_QUEUED,
        TurnQueuedPayload(turn_id=turn_id, message="hello"),
    )


def test_send_pokes_dispatch_only_after_persisting_the_prompt() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    redis.xadd.return_value = "2-0"

    turn_id = client.send("hello")

    queued_stream, queued_fields = redis.xadd.call_args_list[-2].args
    queued = decode_conversation_event("2-0", queued_fields)
    assert queued_stream == "fw:conversation:abc:events"
    assert queued.payload == TurnQueuedPayload(turn_id=turn_id, message="hello")
    dispatch_stream, dispatch_fields = redis.xadd.call_args_list[-1].args
    assert dispatch_stream == "fw:dispatch"
    assert dispatch_fields == {
        "type": Action.CONVERSATION_POKE,
        "turn_id": turn_id,
        "conversation_id": "abc",
    }
    redis.pipeline.assert_not_called()


def test_send_queues_repeated_prompts_as_distinct_turns() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    redis.xadd.side_effect = ["2-0", "1-0", "3-0", "2-0"]

    first_turn = client.send("hello")
    second_turn = client.send("hello")

    assert first_turn != second_turn
    queued = [
        decode_conversation_event(cursor, call.args[1]).payload
        for cursor, call in zip(
            ("2-0", "3-0"), redis.xadd.call_args_list[-4::2]
        )
    ]
    assert queued == [
        TurnQueuedPayload(turn_id=first_turn, message="hello"),
        TurnQueuedPayload(turn_id=second_turn, message="hello"),
    ]
    assert client.conversation.turn_state == {}
    assert client.conversation.last_event_id == "1-0"


def test_send_does_not_dispatch_when_persistence_fails() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    redis.xadd.reset_mock()
    redis.xadd.side_effect = RuntimeError("write failed")

    with pytest.raises(RuntimeError, match="write failed"):
        client.send("hello")

    assert redis.xadd.call_count == 1
    assert redis.xadd.call_args.args[0] == "fw:conversation:abc:events"
    assert client.conversation.turn_state == {}
    assert client.conversation.last_event_id == "1-0"


def test_new_conversation_uses_supplied_settings() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis, deps={"name": "Alice"})

    assert client.conversation_id == "abc"
    assert client.settings == ConversationCreatedPayload(
        agent="pydantic_ai:Agent", model="test", deps={"name": "Alice"}
    )
    created = decode_conversation_event("1-0", redis.xadd.call_args.args[1])
    assert created.payload == client.settings
    redis.xgroup_create.assert_called_once_with(
        "fw:conversation:abc:events", "user", id="0"
    )


def test_invalid_agent_does_not_create_conversation() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []

    with pytest.raises(ValueError, match="cannot instantiate agent"):
        RedisClient(
            redis,
            conversation_id="test",
            agent_path="fallingwater.demo:missing_agent",
            model="test",
        )

    redis.xadd.assert_not_called()


def test_new_conversation_requires_model_when_agent_has_none() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.return_value = []

    with pytest.raises(ValueError, match="no model"):
        RedisClient(
            redis,
            conversation_id="test",
            agent_path="fallingwater.demo:haiku_master",
        )

    redis.xadd.assert_not_called()


def test_resume_uses_saved_settings_and_validates_them() -> None:
    redis = MagicMock(spec=Redis)
    settings = ConversationCreatedPayload(
        agent="pydantic_ai:Agent", model="test", deps={"name": "Alice"}
    )
    created = (
        "1-0",
        encode_conversation_event(Event.CONVERSATION_CREATED, settings),
    )
    redis.xrange.side_effect = [[created], []]

    client = RedisClient(
        redis,
        conversation_id="abc",
        agent_path="fallingwater.demo:missing_agent",
        model="another-model",
    )

    assert client.settings == settings
    redis.xadd.assert_not_called()
    redis.xrange.side_effect = [[created], []]
    with pytest.raises(ValueError, match="dependencies can only be set"):
        RedisClient(redis, conversation_id="abc", deps={"name": "Bob"})


def test_resume_rejects_unavailable_saved_agent() -> None:
    redis = MagicMock(spec=Redis)
    redis.xrange.side_effect = [
        [
            (
                "1-0",
                encode_conversation_event(
                    Event.CONVERSATION_CREATED,
                    ConversationCreatedPayload(
                        agent="fallingwater.demo:missing_agent", model="test"
                    ),
                ),
            )
        ],
        [],
    ]

    with pytest.raises(ValueError, match="cannot instantiate agent"):
        RedisClient(
            redis,
            conversation_id="abc",
            agent_path="pydantic_ai:Agent",
            model="test",
        )


def test_client_opens_with_latest_settings_and_refresh_updates_them() -> None:
    redis = MagicMock(spec=Redis)
    latest = ConversationCreatedPayload(
        agent="pydantic_ai:Agent", model="test", deps={"value": 42}
    )
    redis.xrange.side_effect = [
        [
            (
                "1-0",
                encode_conversation_event(
                    Event.CONVERSATION_CREATED,
                    ConversationCreatedPayload(
                        agent="fallingwater.demo:missing_agent"
                    ),
                ),
            ),
            ("2-0", _queued("turn-1")),
            (
                "3-0",
                encode_conversation_event(Event.CONVERSATION_CREATED, latest),
            ),
        ],
        [],
    ]

    client = RedisClient(redis, conversation_id="abc")

    assert client.settings == latest
    assert client.conversation.last_event_id == "3-0"
    assert client.conversation.turn_state["turn-1"].running
    redis.xadd.assert_not_called()
    updated = latest.model_copy(update={"deps": {"value": 84}})
    redis.xrange.side_effect = [
        [
            (
                "4-0",
                encode_conversation_event(Event.CONVERSATION_CREATED, updated),
            )
        ],
        [],
    ]

    client.refresh()

    assert client.settings == updated
    assert client.conversation.last_event_id == "4-0"
    assert client.conversation.turn_state["turn-1"].running


@pytest.mark.parametrize("failed", [False, True])
def test_ask_waits_for_matching_outcome_without_group_reads_or_output(
    capsys, failed
) -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    client.send = MagicMock(return_value="turn-1")
    client._queued_event_ids["turn-1"] = "2-0"
    outcome = (
        TurnFailedPayload(turn_id="turn-1", error="boom")
        if failed
        else TurnCompletedPayload(
            turn_id="turn-1",
            message="answer",
            turn_messages=[],
            usage=RunUsage(),
        )
    )
    redis.xread.side_effect = [
        [
            (
                client.stream.name,
                [
                    ("3-0", _queued("other")),
                    (
                        "4-0",
                        encode_conversation_event(
                            Event.TURN_FAILED,
                            TurnFailedPayload(turn_id="other", error="ignore"),
                        ),
                    ),
                ],
            )
        ],
        [
            (
                client.stream.name,
                [
                    (
                        "5-0",
                        encode_conversation_event(
                            Event.TURN_FAILED
                            if failed
                            else Event.TURN_COMPLETED,
                            outcome,
                        ),
                    )
                ],
            )
        ],
    ]

    if failed:
        with pytest.raises(RuntimeError, match="boom"):
            client.ask("hello")
    else:
        assert client.ask("hello") == "answer"

    client.send.assert_called_once_with("hello")
    assert [call.args[0] for call in redis.xread.call_args_list] == [
        {client.stream.name: "2-0"},
        {client.stream.name: "4-0"},
    ]
    redis.xreadgroup.assert_not_called()
    redis.xack.assert_not_called()
    assert capsys.readouterr() == ("", "")


def test_group_events_require_explicit_acknowledgement() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    stream = client.stream.name
    redis.xreadgroup.side_effect = [
        [(stream, [("2-0", _queued("turn-1"))])],
        [],
        [],
        [(stream, [("3-0", _queued("turn-2"))])],
    ]

    events = list(client.read(ffwd=True))

    assert [event.entry_id for event in events] == ["2-0"]
    assert client.cursor == ">"
    redis.xack.assert_not_called()
    client.acknowledge(events[0])
    redis.xack.assert_called_once_with(stream, "user", "2-0")
    assert [event.entry_id for event in client.read()] == ["3-0"]
    assert [call.args[2] for call in redis.xreadgroup.call_args_list] == [
        {stream: "0"},
        {stream: "2-0"},
        {stream: ">"},
        {stream: ">"},
    ]
    assert [
        call.kwargs["block"] for call in redis.xreadgroup.call_args_list
    ] == [None, None, None, 1000]
    assert client.queued_event_id("turn-1") == "2-0"
    assert client.queued_event_id("turn-2") == "3-0"


def test_prompt_lookup_recovers_acknowledged_events_and_caches_raw_ids() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    redis.xrange.reset_mock()
    redis.xrange.side_effect = [
        [("2-0", _queued("first")), ("3-0", _queued("second"))],
        [],
    ]

    assert client.queued_event_id("first") == "2-0"
    assert client.queued_event_id("second") == "3-0"
    assert redis.xrange.call_count == 2


def test_prompt_lookup_reports_missing_prompt() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)

    with pytest.raises(ValueError, match="No prompt found for turn: missing"):
        client.queued_event_id("missing")


def test_rewind_failure_does_not_change_local_reader_cursor() -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    client.cursor = "123-0"
    redis.xgroup_setid.side_effect = RedisError("rewind failed")

    with pytest.raises(RedisError, match="rewind failed"):
        client.rewind()

    assert client.cursor == "123-0"


def test_open_reconstructs_all_pages_even_when_the_group_has_acknowledged_them(
    isolated_redis,
) -> None:
    client = RedisClient(
        isolated_redis,
        namespace="fw-test",
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
    )
    for index in range(101):
        last_id = client.stream.append_payload(
            TurnQueuedPayload(turn_id=f"turn-{index}", message="hello")
        )
        if index == 0:
            client.stream.append_payload(
                TurnCompletedPayload(
                    turn_id="turn-0",
                    message="answer",
                    turn_messages=[],
                    usage=RunUsage(requests=1),
                )
            )
    for event in client.read(ffwd=True):
        client.acknowledge(event)

    resumed = RedisClient(
        isolated_redis, namespace="fw-test", conversation_id="abc"
    )

    assert len(resumed.conversation.turn_state) == 101
    assert resumed.conversation.turn_state["turn-0"].completed
    assert resumed.conversation.turn_state["turn-100"].running
    assert resumed.conversation.turn_state["turn-100"].usage == RunUsage(
        requests=1
    )
    assert resumed.conversation.last_event_id == last_id
    assert list(resumed.read(ffwd=True)) == []


def test_explicit_refresh_is_independent_of_submission_delivery_and_rewind(
    isolated_redis, monkeypatch
) -> None:
    client = RedisClient(
        isolated_redis,
        namespace="fw-test",
        conversation_id="abc",
        agent_path="pydantic_ai:Agent",
        model="test",
    )
    conversation = client.conversation
    initial_id = conversation.last_event_id
    turn_id = client.send("hello")
    overlapping_turn = client.send("overlap")
    messages = [ModelResponse(parts=[TextPart("answer")])]
    outcome_id = client.stream.append_payload(
        TurnCompletedPayload(
            turn_id=turn_id,
            message="answer",
            turn_messages=ModelMessagesTypeAdapter.dump_python(
                messages, mode="json"
            ),
            usage=RunUsage(requests=2),
        )
    )
    client.stream.append_payload(
        TurnFailedPayload(turn_id=turn_id, error="late failure")
    )
    next_turn = client.send("next")
    last_id = client.queued_event_id(next_turn)

    assert conversation.turn_state == {}
    assert client.receive(turn_id) == "answer"
    for event in client.read(ffwd=True):
        client.acknowledge(event)
    assert conversation.turn_state == {}
    assert conversation.last_event_id == initial_id
    delivery_cursor = client.cursor
    groups_before = isolated_redis.xinfo_groups(client.stream.name)
    dispatch_before = isolated_redis.xrange(client.stream.dispatch.name)
    tail = MagicMock(wraps=client.stream.iter_events)
    monkeypatch.setattr(client.stream, "iter_events", tail)

    client.refresh()

    tail.assert_called_once_with(after_id=initial_id)
    assert client.conversation is conversation
    assert conversation.last_event_id == last_id
    assert conversation.turn_state[turn_id].completed
    assert conversation.turn_state[turn_id].terminal_event_id == outcome_id
    assert conversation.turn_state[overlapping_turn].history == []
    assert conversation.turn_state[next_turn].history == messages
    assert conversation.turn_state[next_turn].usage == RunUsage(requests=2)
    assert client.cursor == delivery_cursor
    assert isolated_redis.xinfo_groups(client.stream.name) == groups_before
    assert isolated_redis.xrange(client.stream.dispatch.name) == dispatch_before

    client.rewind()
    assert conversation.last_event_id == last_id
    for event in client.read(ffwd=True):
        client.acknowledge(event)
    client.refresh()

    assert tail.call_args.kwargs == {"after_id": last_id}
    assert conversation.last_event_id == last_id
    assert conversation.turn_state[next_turn].history == messages
    assert conversation.turn_state[turn_id].terminal_event_id == outcome_id


def test_refresh_propagates_read_failure_and_retries_from_last_applied_event():
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    redis.xrange.side_effect = [
        [("2-0", _queued("turn-1"))],
        RedisError("tail read failed"),
    ]

    with pytest.raises(RedisError, match="tail read failed"):
        client.refresh()

    assert client.conversation.last_event_id == "2-0"
    assert client.conversation.turn_state["turn-1"].running
    redis.xrange.side_effect = [
        [
            (
                "3-0",
                encode_conversation_event(
                    Event.TURN_FAILED,
                    TurnFailedPayload(turn_id="turn-1", error="failed"),
                ),
            )
        ],
        [],
    ]

    client.refresh()

    assert redis.xrange.call_args_list[-2].kwargs["min"] == "(2-0"
    assert client.conversation.last_event_id == "3-0"
    assert client.conversation.turn_state["turn-1"].failed


def test_open_reports_unreplayable_history_without_creating_a_reader_group():
    redis = MagicMock(spec=Redis)
    redis.xrange.side_effect = [
        [
            (
                "1-0",
                encode_conversation_event(
                    Event.CONVERSATION_CREATED,
                    ConversationCreatedPayload(
                        agent="pydantic_ai:Agent", model="test"
                    ),
                ),
            )
        ],
        [
            (
                "2-0",
                encode_conversation_event(
                    Event.TURN_FAILED,
                    TurnFailedPayload(turn_id="missing", error="failed"),
                ),
            )
        ],
    ]

    with pytest.raises(ValueError, match="No prompt found for turn: missing"):
        RedisClient(redis, conversation_id="abc")

    redis.xgroup_create.assert_not_called()
    redis.xadd.assert_not_called()


def test_concurrent_refreshes_read_from_the_serialized_replay_cursor(
    monkeypatch,
) -> None:
    redis = MagicMock(spec=Redis)
    client = _client(redis)
    first_read = ThreadEvent()
    second_started = ThreadEvent()
    release_first = ThreadEvent()
    cursors = []

    def tail(*, after_id):
        cursors.append(after_id)
        if after_id == "1-0":
            first_read.set()
            assert release_first.wait(timeout=2)
            yield decode_conversation_event("2-0", _queued("turn-1"))
        else:
            assert after_id == "2-0"
            yield decode_conversation_event(
                "3-0",
                encode_conversation_event(
                    Event.TURN_FAILED,
                    TurnFailedPayload(turn_id="turn-1", error="failed"),
                ),
            )

    def second_refresh():
        second_started.set()
        client.refresh()

    monkeypatch.setattr(client.stream, "iter_events", tail)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.refresh)
        try:
            assert first_read.wait(timeout=2)
            second = pool.submit(second_refresh)
            assert second_started.wait(timeout=2)
        finally:
            release_first.set()
        first.result(timeout=2)
        second.result(timeout=2)

    assert cursors == ["1-0", "2-0"]
    assert client.conversation.last_event_id == "3-0"
    assert client.conversation.turn_state["turn-1"].failed
