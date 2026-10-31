"""
Compare compression of one serialized Pydantic AI turn.
"""

import argparse
import json
from datetime import datetime, timezone
from time import perf_counter
from typing import Callable

import brotli
import snappy
import zstandard
from lz4 import frame as lz4_frame
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)


def sample_turn() -> bytes:
    """
    Build a representative turn with two messages and usage metadata.
    """
    timestamp = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
    messages = [
        ModelRequest(
            parts=[
                UserPromptPart(
                    content=(
                        "Summarize the tradeoffs between Redis Streams and a "
                        "database queue for a small Python service. Include "
                        "delivery guarantees, operational costs, and a short "
                        "recommendation."
                    ),
                    timestamp=timestamp,
                )
            ],
            timestamp=timestamp,
        ),
        ModelResponse(
            parts=[
                TextPart(
                    content=(
                        "Redis Streams are a good fit when the service already "
                        "uses Redis and needs consumer groups, replay, and "
                        "lightweight operational setup. A database queue can "
                        "simplify transactions with application data and may "
                        "be easier to operate when a database is already the "
                        "main dependency. For this small service, use Redis "
                        "Streams if Redis is already managed; otherwise, start "
                        "with the existing database and add a dedicated queue "
                        "only when its delivery or scaling needs are clear."
                    )
                )
            ],
            timestamp=timestamp,
        ),
    ]
    turn = {
        "messages": json.loads(ModelMessagesTypeAdapter.dump_json(messages)),
        "usage": {
            "input_tokens": 184,
            "output_tokens": 92,
            "requests": 1,
            "tool_calls": 0,
            "details": {"reasoning_tokens": 0},
        },
    }
    return json.dumps(turn, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )


def benchmark(
    name: str,
    compress: Callable[[bytes], bytes],
    decompress: Callable[[bytes], bytes],
    payload: bytes,
    iterations: int,
) -> tuple[str, int, float, float]:
    compressed = compress(payload)
    if decompress(compressed) != payload:
        raise RuntimeError(f"{name} failed its round-trip check")

    started = perf_counter()
    for _ in range(iterations):
        compress(payload)
    elapsed = perf_counter() - started

    ratio = len(compressed) / len(payload)
    return name, len(compressed), elapsed / iterations, ratio


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--iterations",
        type=int,
        default=1_000,
        help="number of compressions to time (default: 1000)",
    )
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be at least 1")

    payload = sample_turn()
    zstd_decompressor = zstandard.ZstdDecompressor()
    compressors: list[
        tuple[str, str, Callable[[bytes], bytes], Callable[[bytes], bytes]]
    ] = []

    # Use each codec's minimum, a middle setting, and maximum setting.
    zstd_levels = (("min", -131_072), ("medium", 3), ("max", 22))
    for label, level in zstd_levels:
        compressor = zstandard.ZstdCompressor(level=level)
        compressors.append(
            (
                "Zstandard",
                f"{label} ({level})",
                compressor.compress,
                zstd_decompressor.decompress,
            )
        )

    lz4_levels = (
        ("min", lz4_frame.COMPRESSIONLEVEL_MIN),
        ("medium", 8),
        ("max", lz4_frame.COMPRESSIONLEVEL_MAX),
    )
    for label, level in lz4_levels:
        compressors.append(
            (
                "LZ4",
                f"{label} ({level})",
                lambda data, level=level: lz4_frame.compress(
                    data, compression_level=level
                ),
                lz4_frame.decompress,
            )
        )

    brotli_levels = (("min", 0), ("medium", 5), ("max", 11))
    for label, quality in brotli_levels:
        compressors.append(
            (
                "Brotli",
                f"{label} ({quality})",
                lambda data, quality=quality: brotli.compress(
                    data, quality=quality
                ),
                brotli.decompress,
            )
        )

    # Snappy has no compression-level setting.
    compressors.append(("Snappy", "fixed", snappy.compress, snappy.decompress))

    print(f"Payload: {len(payload)} bytes (one turn, two messages + usage)")
    print(f"Iterations: {args.iterations:,}")
    print(
        f"{'Algorithm':<12} {'Level':<15} {'Bytes':>8} {'Ratio':>9} "
        f"{'Saved':>8} {'Time/op':>12}"
    )
    print("Ratio is compressed size divided by original size.")
    for algorithm_name, level_name, compress, decompress in compressors:
        name = f"{algorithm_name} {level_name}"
        result = benchmark(name, compress, decompress, payload, args.iterations)
        _, size, seconds_per_op, ratio = result
        saved = 1 - ratio
        print(
            f"{algorithm_name:<12} {level_name:<15} {size:>8} "
            f"{ratio:>8.1%} "
            f"{saved:>7.1%} {seconds_per_op * 1_000_000:>9.2f} µs"
        )


if __name__ == "__main__":
    main()
