import io
from pathlib import Path

import zstandard as zstd
from redis import Redis
from pydantic import TypeAdapter

from fallingwater.stream import DispatchStream
from fallingwater.stream_models import ConversationEvent


adapter = TypeAdapter(ConversationEvent)


def dump_conversation_stream(redis: Redis, namespace: str, conversation_id: str, store: str) -> int:
    """
    Dump all events in a conversation stream to storage.
    """
    stream = DispatchStream(
        redis, namespace=namespace
    ).conversation_stream(conversation_id)

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


def load_conversation_stream(redis: Redis, namespace: str, conversation_id: str, store: str) -> int:
    """
    Load all events in a conversation stream from storage.
    """
    stream = DispatchStream(
        redis, namespace=namespace
    ).conversation_stream(conversation_id)
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
