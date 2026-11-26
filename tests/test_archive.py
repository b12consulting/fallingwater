"""Checks Zstandard archive export and restoration."""

from unittest.mock import MagicMock, call

import pytest
import zstandard as zstd
from redis import Redis

from fallingwater import archive
from fallingwater.local_executor import LocalExecutor
from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    TurnQueuedPayload,
)
from fallingwater.stream_utils import encode_conversation_event


def test_zstd_archive_round_trip(tmp_path, monkeypatch) -> None:
    events = [
        ConversationEvent(
            entry_id="1-0",
            type=Event.CONVERSATION_CREATED,
            payload=ConversationCreatedPayload(
                agent="example:agent", model="test"
            ),
        ),
        ConversationEvent(
            entry_id="2-0",
            type=Event.TURN_QUEUED,
            payload=TurnQueuedPayload(turn_id="turn-1", message="hello"),
        ),
    ]
    dispatch = MagicMock()
    stream = dispatch.conversation_stream.return_value
    stream.iter_events.return_value = events
    stream.read_conversation_range.return_value = []
    monkeypatch.setattr(
        archive, "DispatchStream", lambda *_args, **_kwargs: dispatch
    )

    redis = MagicMock(spec=Redis)
    redis.exists.return_value = True
    length = archive.save_conversation(redis, "fw", "abc", str(tmp_path))
    store_file = tmp_path / "abc.jsonl.zst"

    assert length == store_file.stat().st_size
    assert (
        zstd.ZstdDecompressor().decompress(
            store_file.read_bytes(), max_output_size=1024
        )
        == b"\n".join(archive.adapter.dump_json(event) for event in events)
        + b"\n"
    )
    assert (
        archive.adapter.validate_json(
            archive.adapter.dump_json(events[0])
        ).entry_id
        == "1-0"
    )
    redis.exists.return_value = False
    assert archive.load_conversation(redis, "fw", "abc", str(tmp_path)) == len(
        events
    )
    assert stream.append_event.call_args_list == [
        call(event.type, event.payload) for event in events
    ]


def test_load_zstd_rejects_incomplete_jsonl_record(
    tmp_path, monkeypatch
) -> None:
    event = ConversationEvent(
        entry_id="1-0",
        type=Event.CONVERSATION_CREATED,
        payload=ConversationCreatedPayload(agent="example:agent", model="test"),
    )
    (tmp_path / "abc.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress(archive.adapter.dump_json(event))
    )

    dispatch = MagicMock()
    dispatch.conversation_stream.return_value.read_conversation_range.return_value = (
        []
    )
    monkeypatch.setattr(
        archive, "DispatchStream", lambda *_args, **_kwargs: dispatch
    )

    with pytest.raises(ValueError, match="truncated"):
        redis = MagicMock(spec=Redis)
        redis.exists.return_value = False
        archive.load_conversation(redis, "fw", "abc", str(tmp_path))


def test_load_zstd_requires_entry_id(tmp_path, monkeypatch) -> None:
    event = ConversationEvent(
        entry_id="1-0",
        type=Event.CONVERSATION_CREATED,
        payload=ConversationCreatedPayload(agent="example:agent", model="test"),
    )
    record = event.model_dump_json(exclude={"entry_id"}).encode() + b"\n"
    (tmp_path / "abc.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress(record)
    )
    dispatch = MagicMock()
    dispatch.conversation_stream.return_value.read_conversation_range.return_value = (
        []
    )
    monkeypatch.setattr(
        archive, "DispatchStream", lambda *_args, **_kwargs: dispatch
    )

    with pytest.raises(ValueError, match="entry_id"):
        redis = MagicMock(spec=Redis)
        redis.exists.return_value = False
        archive.load_conversation(redis, "fw", "abc", str(tmp_path))
    dispatch.conversation_stream.return_value.append_event.assert_not_called()


def test_sync_copies_all_streams_and_preserves_state(
    isolated_redis: Redis,
) -> None:
    redis = isolated_redis
    sources = {
        "source:dispatch": {b"type": b"poke", b"conversation_id": b"abc"},
        "source:conversation:abc:events": {
            b"type": b"conversation.created",
            b"payload": b"\x00\xff",
        },
        "source:custom": {b"field": b"value"},
    }
    for source, fields in sources.items():
        redis.xadd(source, fields, id="1-0")
    redis.xgroup_create("source:dispatch", "workers", id="0")
    redis.xreadgroup("workers", "worker-1", {"source:dispatch": ">"})
    redis.pexpire("source:custom", 60_000)
    redis.set("source:settings", "not a stream")
    redis.xadd("source-other:dispatch", {"field": "unrelated"})
    redis.set("destination:unrelated", "keep")

    assert archive.sync_streams(redis, "source", "destination") == 3

    for source in sources:
        destination = source.replace("source:", "destination:", 1)
        assert redis.xrange(destination) == redis.xrange(source)
    assert redis.xinfo_groups("destination:dispatch") == redis.xinfo_groups(
        "source:dispatch"
    )
    assert (
        redis.xpending_range("destination:dispatch", "workers", "-", "+", 10)[
            0
        ]["message_id"]
        == b"1-0"
    )
    assert 0 < redis.pttl("destination:custom") <= 60_000
    assert not redis.exists("destination:settings")
    assert not redis.exists("destination-other:dispatch")
    assert redis.get("destination:unrelated") == b"keep"


@pytest.mark.parametrize("conflict_type", ["stream", "string"])
def test_sync_refuses_conflicts_before_copying(
    isolated_redis: Redis, conflict_type: str
) -> None:
    redis = isolated_redis
    redis.xadd("source:a", {"field": "first"})
    redis.xadd("source:z", {"field": "last"})
    if conflict_type == "stream":
        redis.xadd("destination:z", {"field": "existing"})
    else:
        redis.set("destination:z", "existing")
    original = redis.dump("destination:z")

    with pytest.raises(ValueError, match="destination:z already exists"):
        archive.sync_streams(redis, "source", "destination")

    assert not redis.exists("destination:a")
    assert redis.dump("destination:z") == original


@pytest.mark.parametrize(
    ("source", "destination"),
    [("fw", "fw"), ("bad:*", "fw"), ("fw", "BAD")],
)
def test_sync_rejects_invalid_namespaces(source: str, destination: str) -> None:
    redis = MagicMock(spec=Redis)
    with pytest.raises(ValueError):
        archive.sync_streams(redis, source, destination)
    redis.scan_iter.assert_not_called()


def test_sync_empty_namespace(isolated_redis: Redis) -> None:
    assert archive.sync_streams(isolated_redis, "source", "destination") == 0


def test_sync_reports_failed_copy() -> None:
    redis = MagicMock(spec=Redis)
    redis.scan_iter.return_value = [b"source:dispatch"]
    redis.exists.return_value = False
    redis.copy.return_value = False

    with pytest.raises(ValueError, match="could not copy source:dispatch"):
        archive.sync_streams(redis, "source", "destination")


def test_namespace_archive_round_trip(tmp_path, isolated_redis: Redis) -> None:
    redis = isolated_redis
    payload = ConversationCreatedPayload(agent="example:agent", model="test")
    fields = encode_conversation_event(Event.CONVERSATION_CREATED, payload)
    for conversation_id in ("abc", "xyz"):
        redis.xadd(f"source:conversation:{conversation_id}:events", fields)
    redis.xadd("source-other:conversation:other:events", fields)
    redis.xadd("source:dispatch", {"type": "poke"})
    redis.set("source:conversation:not-a-stream:events", "ignore")
    (tmp_path / "notes.txt").write_text("ignore")

    length = archive.save_namespace(redis, "source", str(tmp_path))

    assert {path.name for path in tmp_path.glob("*.jsonl.zst")} == {
        "abc.jsonl.zst",
        "xyz.jsonl.zst",
    }
    assert length == sum(
        path.stat().st_size for path in tmp_path.glob("*.jsonl.zst")
    )
    assert archive.load_namespace(redis, "destination", str(tmp_path)) == 2
    for conversation_id in ("abc", "xyz"):
        stream = archive.DispatchStream(
            redis, namespace="destination"
        ).conversation_stream(conversation_id)
        assert [event.payload for event in stream.iter_events()] == [payload]
    assert not redis.exists("destination:dispatch")
    assert not redis.exists("destination:conversation:other:events")


def test_namespace_restore_preflights_all_conflicts(
    tmp_path, isolated_redis: Redis
) -> None:
    redis = isolated_redis
    payload = ConversationCreatedPayload(agent="example:agent", model="test")
    fields = encode_conversation_event(Event.CONVERSATION_CREATED, payload)
    for conversation_id in ("abc", "xyz"):
        redis.xadd(f"source:conversation:{conversation_id}:events", fields)
    archive.save_namespace(redis, "source", str(tmp_path))
    redis.xgroup_create(
        "destination:conversation:xyz:events", "reader", mkstream=True
    )

    with pytest.raises(ValueError, match="xyz:events already exists"):
        archive.load_namespace(redis, "destination", str(tmp_path))
    assert not redis.exists("destination:conversation:abc:events")
    assert redis.xlen("destination:conversation:xyz:events") == 0


@pytest.mark.parametrize(
    "operation", [archive.save_namespace, archive.load_namespace]
)
def test_namespace_archive_validation(tmp_path, operation) -> None:
    redis = MagicMock(spec=Redis)
    with pytest.raises(ValueError, match="namespace"):
        operation(redis, "bad:*", str(tmp_path))
    with pytest.raises(ValueError, match="not a directory"):
        operation(redis, "fw", str(tmp_path / "missing"))
    redis.scan_iter.assert_not_called()
    redis.exists.assert_not_called()


def test_empty_namespace_archive_and_store(
    tmp_path, isolated_redis: Redis
) -> None:
    assert archive.save_namespace(isolated_redis, "fw", str(tmp_path)) == 0
    assert archive.load_namespace(isolated_redis, "fw", str(tmp_path)) == 0


def test_namespace_archive_rejects_unsafe_conversation_filename(
    tmp_path, isolated_redis: Redis
) -> None:
    isolated_redis.xadd(
        "fw:conversation:../outside:events", {"type": "invalid"}
    )
    with pytest.raises(ValueError, match="not a file name"):
        archive.save_namespace(isolated_redis, "fw", str(tmp_path))
    assert not list(tmp_path.glob("*.jsonl.zst"))


@pytest.fixture
def conversation_tree(tmp_path, isolated_redis):
    with LocalExecutor() as executor:
        root = executor.create("tests.test_local_executor:editor")
        executor.send(root, "edit")
        executor.drain()
        executor.send(root, "edit again")
        executor.drain()
        snapshots = {
            key: list(session.events.values())
            for key, session in executor.registry.items()
        }
        executor.save(root, str(tmp_path))
    assert archive.load_namespace(
        isolated_redis, "source", str(tmp_path)
    ) == sum(len(events) for events in snapshots.values())
    return root, snapshots


@pytest.mark.parametrize("namespace_mode", [False, True])
def test_redis_tree_save_load_deduplicates_and_preserves_history(
    tmp_path, isolated_redis, conversation_tree, namespace_mode
) -> None:
    root, snapshots = conversation_tree
    store = tmp_path / "saved"
    store.mkdir()
    redis = isolated_redis
    if namespace_mode:
        length = archive.save_namespace(redis, "source", str(store))
        count = archive.load_namespace(redis, "destination", str(store))
    else:
        redis.xadd(
            "source:conversation:unrelated:events",
            encode_conversation_event(
                Event.CONVERSATION_CREATED,
                ConversationCreatedPayload(agent="example:agent"),
            ),
        )
        length = archive.save_conversation(redis, "source", root, str(store))
        count = archive.load_conversation(
            redis, "destination", root, str(store)
        )
        assert not redis.exists("destination:conversation:unrelated:events")
    assert {path.name for path in store.glob("*.jsonl.zst")} == {
        f"{key}.jsonl.zst" for key in snapshots
    }
    assert length == sum(
        path.stat().st_size for path in store.glob("*.jsonl.zst")
    )
    assert count == sum(len(events) for events in snapshots.values())
    for key, events in snapshots.items():
        stream = archive.DispatchStream(
            redis, namespace="destination"
        ).conversation_stream(key)
        assert [event.payload for event in stream.iter_events()] == [
            event.payload for event in events
        ]
    with LocalExecutor() as executor:
        executor.load(root, str(store))
        child = executor.registry[root].conversation.agent_by_name["writer"]
        turn = executor.send(root, "continue")
        executor.drain()
        assert executor.registry[root].conversation.turn_state[turn].completed
        assert (
            executor.registry[root].conversation.agent_by_name["writer"]
            == child
        )
        assert len(executor.registry[child].conversation.turn_state) == 6


@pytest.mark.parametrize("conflict_type", ["stream", "empty-stream", "string"])
def test_redis_tree_load_preflights_descendant_conflicts(
    tmp_path, isolated_redis, conversation_tree, conflict_type
) -> None:
    root, snapshots = conversation_tree
    child = next(key for key in snapshots if key != root)
    redis = isolated_redis
    key = f"destination:conversation:{child}:events"
    if conflict_type == "stream":
        redis.xadd(key, {"keep": "original"})
    elif conflict_type == "empty-stream":
        redis.xgroup_create(key, "reader", mkstream=True)
    else:
        redis.set(key, "original")
    original = redis.dump(key)
    with pytest.raises(ValueError, match="already exists"):
        archive.load_conversation(redis, "destination", root, str(tmp_path))
    assert list(redis.scan_iter(match="destination:*")) == [key.encode()]
    assert redis.dump(key) == original


@pytest.mark.parametrize("damage", ["missing", "truncated"])
def test_redis_tree_load_validates_descendant_files_before_writing(
    tmp_path, isolated_redis, conversation_tree, damage
) -> None:
    root, snapshots = conversation_tree
    child = next(key for key in snapshots if key != root)
    path = tmp_path / f"{child}.jsonl.zst"
    if damage == "missing":
        path.unlink()
        error = "not found"
    else:
        path.write_bytes(
            zstd.ZstdCompressor().compress(
                archive.adapter.dump_json(snapshots[child][0])
            )
        )
        error = "truncated"
    with pytest.raises(ValueError, match=error):
        archive.load_conversation(
            isolated_redis, "destination", root, str(tmp_path)
        )
    assert not list(isolated_redis.scan_iter(match="destination:*"))


def test_redis_tree_save_reports_missing_descendant(
    tmp_path, isolated_redis, conversation_tree
) -> None:
    root, snapshots = conversation_tree
    child = next(key for key in snapshots if key != root)
    isolated_redis.delete(f"source:conversation:{child}:events")
    with pytest.raises(ValueError, match="Conversation not found"):
        archive.save_conversation(isolated_redis, "source", root, str(tmp_path))
