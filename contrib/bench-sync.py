"""Copy fw into 100 namespaces and report the time for each sync."""

import os
from time import perf_counter

from redis import Redis

from fallingwater.archive import sync_streams


def main() -> None:
    redis_url = os.getenv("FW_REDIS_URL", "redis://localhost:6379/0")
    with Redis.from_url(redis_url) as redis:
        for i in range(1000):
            namespace = f"fw-copy-{i:002d}"
            started = perf_counter()
            count = sync_streams(redis, "fw", namespace)
            elapsed = perf_counter() - started
            print(f"{namespace}: {count} stream(s) in {elapsed:.3f}s")


if __name__ == "__main__":
    # Copy all the streams from the main namespace (fw:) to 1000 other
    # namespaces (fw-copy-xyz)
    # cleanup can be done with :
    # redis-cli --scan --pattern 'fw-copy-*:*' | xargs -n 100 redis-cli UNLINK

    main()
