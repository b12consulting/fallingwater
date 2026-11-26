import io
from collections.abc import Iterable, Iterator
from pathlib import Path

import zstandard as zstd
from redis import Redis
from pydantic import TypeAdapter

from fallingwater.stream import DispatchStream
from fallingwater.models import AgentRequestedPayload, ConversationEvent
from fallingwater.stream_utils import (
    decode_text,
    fmt_conversation_stream,
    validate_conversation_id,
    validate_namespace,
)


adapter = TypeAdapter(ConversationEvent)


def sync_streams(redis: Redis, source_ns: str, destination_ns: str) -> int:
    """Copy namespace streams without replacing existing destination keys."""
    validate_namespace(source_ns)
    validate_namespace(destination_ns)
    if source_ns == destination_ns:
        raise ValueError("source and destination namespaces must differ")

    prefix = f"{source_ns}:"
    streams = sorted(
        {
            decode_text(key)
            for key in redis.scan_iter(
                match=f"{prefix}*", _type="stream", count=1000
            )
        }
    )
    destinations = [
        f"{destination_ns}:{stream[len(prefix):]}" for stream in streams
    ]
    for destination in destinations:
        if redis.exists(destination):
            raise ValueError(f"{destination} already exists")

    for source, destination in zip(streams, destinations):
        if not redis.copy(source, destination):
            raise ValueError(
                f"could not copy {source} to {destination}: "
                "source disappeared or destination already exists"
            )
    return len(streams)


def save_namespace(redis: Redis, namespace: str, store: str) -> int:
    """Save every conversation history in a namespace; return bytes written."""
    pattern = fmt_conversation_stream(namespace, "*")
    store_path = Path(store)
    if not store_path.is_dir():
        raise ValueError(f"store path {store_path} is not a directory")
    prefix = f"{namespace}:conversation:"
    suffix = ":events"
    conversation_ids = sorted(
        {
            decode_text(key)[len(prefix) : -len(suffix)]
            for key in redis.scan_iter(
                match=pattern, _type="stream", count=1000
            )
        }
    )
    for conversation_id in conversation_ids:
        validate_conversation_id(conversation_id)
        if Path(conversation_id).name != conversation_id:
            raise ValueError(
                f"conversation ID {conversation_id!r} is not a file name"
            )
    return _save_conversations(redis, namespace, conversation_ids, store)


def load_namespace(redis: Redis, namespace: str, store: str) -> int:
    """Load all conversation archives into a namespace; return event count."""
    validate_namespace(namespace)
    store_path = Path(store)
    if not store_path.is_dir():
        raise ValueError(f"store path {store_path} is not a directory")
    conversation_ids = [
        path.name.removesuffix(".jsonl.zst")
        for path in sorted(store_path.glob("*.jsonl.zst"))
        if path.is_file()
    ]
    return _load_conversations(redis, namespace, conversation_ids, store)


def save_conversation(
    redis: Redis, namespace: str, conversation_id: str, store: str
) -> int:
    """Save a conversation and referenced descendants; return bytes written."""
    return _save_conversations(redis, namespace, [conversation_id], store)


def _save_conversations(
    redis: Redis, namespace: str, conversation_ids: list[str], store: str
) -> int:
    dispatch = DispatchStream(redis, namespace=namespace)
    pending = list(reversed(conversation_ids))
    seen: set[str] = set()
    length = 0
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        _store_file(current, store)
        stream = dispatch.conversation_stream(current)
        if not redis.exists(stream.name):
            raise ValueError(f"Conversation not found: {stream.name}")
        seen.add(current)

        def events() -> Iterator[ConversationEvent]:
            for event in stream.iter_events():
                if isinstance(event.payload, AgentRequestedPayload):
                    pending.append(event.payload.child_conversation_id)
                yield event

        length += save_events(events(), current, store)
    return length


def _store_file(conversation_id: str, store: str) -> Path:
    validate_conversation_id(conversation_id)
    if Path(conversation_id).name != conversation_id or conversation_id in (
        ".",
        "..",
    ):
        raise ValueError(
            f"conversation ID {conversation_id!r} is not a file name"
        )
    store_path = Path(store)
    if not store_path.is_dir():
        raise ValueError(f"store path {store_path} is not a directory")
    return store_path / f"{conversation_id}.jsonl.zst"


def save_events(
    events: Iterable[ConversationEvent], conversation_id: str, store: str
) -> int:
    """Write backend-independent conversation events; return bytes written."""
    store_file = _store_file(conversation_id, store)
    with store_file.open("wb") as f:
        with zstd.ZstdCompressor().stream_writer(f, closefd=False) as writer:
            for event in events:
                writer.write(adapter.dump_json(event) + b"\n")
        length = f.tell()

    return length


def load_events(
    conversation_id: str, store: str
) -> Iterator[ConversationEvent]:
    """Read validated conversation events, preserving archived entry IDs."""
    store_file = _store_file(conversation_id, store)
    if not store_file.is_file():
        raise ValueError(f"store file {store_file} not found")
    with store_file.open("rb") as f:
        with io.BufferedReader(
            zstd.ZstdDecompressor().stream_reader(f, closefd=False)
        ) as lines:
            for line in lines:
                if not line.endswith(b"\n"):
                    raise ValueError(f"store file {store_file} is truncated")
                yield adapter.validate_json(line)


def load_conversation(
    redis: Redis, namespace: str, conversation_id: str, store: str
) -> int:
    """Load a conversation and referenced descendants; return event count."""
    return _load_conversations(redis, namespace, [conversation_id], store)


def _load_conversations(
    redis: Redis, namespace: str, conversation_ids: list[str], store: str
) -> int:
    dispatch = DispatchStream(redis, namespace=namespace)
    pending = list(reversed(conversation_ids))
    snapshots: dict[str, list[ConversationEvent]] = {}
    while pending:
        current = pending.pop()
        if current in snapshots:
            continue
        stream = dispatch.conversation_stream(current)
        if redis.exists(stream.name):
            raise ValueError(f"{stream.name} already exists")
        events = list(load_events(current, store))
        snapshots[current] = events
        pending.extend(
            event.payload.child_conversation_id
            for event in events
            if isinstance(event.payload, AgentRequestedPayload)
        )
    count = 0
    for current, events in snapshots.items():
        stream = dispatch.conversation_stream(current)
        for event in events:
            stream.append_event(event.type, event.payload)
            count += 1
    return count
