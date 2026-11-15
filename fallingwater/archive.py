import io
from pathlib import Path

import zstandard as zstd
from redis import Redis
from pydantic import TypeAdapter

from fallingwater.stream import DispatchStream
from fallingwater.models import ConversationEvent
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


def dump_namespace(redis: Redis, namespace: str, store: str) -> int:
    """Archive every conversation history in a namespace; return bytes written."""
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
    return sum(
        dump_conversation_stream(redis, namespace, conversation_id, store)
        for conversation_id in conversation_ids
    )


def load_namespace(redis: Redis, namespace: str, store: str) -> int:
    """Restore all conversation archives into a namespace; return event count."""
    validate_namespace(namespace)
    store_path = Path(store)
    if not store_path.is_dir():
        raise ValueError(f"store path {store_path} is not a directory")
    conversation_ids = [
        path.name.removesuffix(".jsonl.zst")
        for path in sorted(store_path.glob("*.jsonl.zst"))
        if path.is_file()
    ]
    for conversation_id in conversation_ids:
        stream = fmt_conversation_stream(namespace, conversation_id)
        if redis.exists(stream):
            raise ValueError(f"{stream} already exists")
    return sum(
        load_conversation_stream(redis, namespace, conversation_id, store)
        for conversation_id in conversation_ids
    )


def dump_conversation_stream(
    redis: Redis, namespace: str, conversation_id: str, store: str
) -> int:
    """
    Dump all events in a conversation stream to storage.
    """
    stream = DispatchStream(redis, namespace=namespace).conversation_stream(
        conversation_id
    )

    # instantiate store
    # TODO handle other backends (like S3)
    store_path = Path(store)
    if not store_path.is_dir():
        raise ValueError(f"store path {store_path} is not a directory")
    store_file = store_path / f"{conversation_id}.jsonl.zst"

    with store_file.open("wb") as f:
        with zstd.ZstdCompressor().stream_writer(f, closefd=False) as writer:
            for event in stream.iter_events():
                writer.write(adapter.dump_json(event) + b"\n")
        length = f.tell()

    return length


def load_conversation_stream(
    redis: Redis, namespace: str, conversation_id: str, store: str
) -> int:
    """
    Load all events in a conversation stream from storage.
    """
    stream = DispatchStream(redis, namespace=namespace).conversation_stream(
        conversation_id
    )
    records = stream.read_conversation_range(count=1)
    if records:
        raise ValueError(f"{stream.name} already exists")

    # instantiate store
    store_path = Path(store)
    store_file = store_path / f"{conversation_id}.jsonl.zst"
    if not store_file.is_file():
        raise ValueError(f"store file {store_file} not found")

    count = 0
    with store_file.open("rb") as f:
        with io.BufferedReader(
            zstd.ZstdDecompressor().stream_reader(f, closefd=False)
        ) as lines:
            for line in lines:
                if not line.endswith(b"\n"):
                    raise ValueError(f"store file {store_file} is truncated")
                event = adapter.validate_json(line)
                stream.append_event(event.type, event.payload)
                count += 1

    return count
