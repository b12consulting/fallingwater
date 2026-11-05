"""Checks Zstandard archive export and restoration."""

from unittest.mock import MagicMock, call

import pytest
import zstandard as zstd
from redis import Redis

from fallingwater import archive
from fallingwater.models import (
    ConversationCreatedPayload,
    ConversationEvent,
    Event,
    TurnQueuedPayload,
)


def test_zstd_archive_round_trip(tmp_path, monkeypatch) -> None:
    events = [
        ConversationEvent(
            entry_id="1-0",
            type=Event.CONVERSATION_CREATED,
            payload=ConversationCreatedPayload(agent="example:agent", model="test"),
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
    monkeypatch.setattr(archive, "DispatchStream", lambda *_args, **_kwargs: dispatch)

    length = archive.dump_conversation_stream(
        MagicMock(spec=Redis), "fw", "abc", str(tmp_path)
    )
    store_file = tmp_path / "abc.jsonl.zst"

    assert length == store_file.stat().st_size
    assert zstd.ZstdDecompressor().decompress(
        store_file.read_bytes(), max_output_size=1024
    ) == b"\n".join(archive.adapter.dump_json(event) for event in events) + b"\n"
    assert archive.adapter.validate_json(
        archive.adapter.dump_json(events[0])
    ).entry_id == "1-0"
    assert archive.load_conversation_stream(
        MagicMock(spec=Redis), "fw", "abc", str(tmp_path)
    ) == len(events)
    assert stream.append_event.call_args_list == [
        call(event.type, event.payload) for event in events
    ]


def test_load_zstd_rejects_incomplete_jsonl_record(tmp_path, monkeypatch) -> None:
    event = ConversationEvent(
        entry_id="1-0",
        type=Event.CONVERSATION_CREATED,
        payload=ConversationCreatedPayload(agent="example:agent", model="test"),
    )
    (tmp_path / "abc.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress(archive.adapter.dump_json(event))
    )

    dispatch = MagicMock()
    dispatch.conversation_stream.return_value.read_conversation_range.return_value = []
    monkeypatch.setattr(archive, "DispatchStream", lambda *_args, **_kwargs: dispatch)

    with pytest.raises(ValueError, match="truncated"):
        archive.load_conversation_stream(
            MagicMock(spec=Redis), "fw", "abc", str(tmp_path)
        )


def test_load_zstd_requires_entry_id(tmp_path, monkeypatch) -> None:
    event = ConversationEvent(
        entry_id="1-0",
        type=Event.CONVERSATION_CREATED,
        payload=ConversationCreatedPayload(agent="example:agent", model="test"),
    )
    record = event.model_dump_json(exclude={"entry_id"}).encode() + b"\n"
    (tmp_path / "abc.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress(record))
    dispatch = MagicMock()
    dispatch.conversation_stream.return_value.read_conversation_range.return_value = []
    monkeypatch.setattr(archive, "DispatchStream", lambda *_args, **_kwargs: dispatch)

    with pytest.raises(ValueError, match="entry_id"):
        archive.load_conversation_stream(
            MagicMock(spec=Redis), "fw", "abc", str(tmp_path)
        )
    dispatch.conversation_stream.return_value.append_event.assert_not_called()
