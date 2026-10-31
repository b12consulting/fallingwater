"""A Redis Streams countdown shared by ten competing workers."""

import logging
import random
import threading
import time
from uuid import uuid4

from redis import Redis


STREAM = "fw:countdown"
GROUP = "countdown-workers"
INITIAL_VALUE = 100
WORKER_COUNT = 10
CLAIM_IDLE_MS = 3_000

logging.basicConfig(level=logging.INFO, format="%(threadName)s: %(message)s")
logger = logging.getLogger(__name__)


class Worker:
    """Consume countdown events and append the next value to the stream."""

    def __init__(self, redis: Redis, stream: str) -> None:
        self.redis = redis
        self.stream = stream
        self.consumer = str(uuid4())

    def _read(self) -> list[tuple[str, dict[str, str]]]:
        """Claim abandoned work first, then read one new event."""
        claimed = self.redis.xautoclaim(
            self.stream,
            GROUP,
            self.consumer,
            min_idle_time=CLAIM_IDLE_MS,
            start_id="0-0",
            count=1,
        )
        entries = claimed[1]
        if entries:
            return entries

        batches = self.redis.xreadgroup(
            GROUP,
            self.consumer,
            {self.stream: ">"},
            count=1,
            block=500,
        )
        return batches[0][1] if batches else []

    def run(self, stop: threading.Event) -> None:
        """Run until stopped or this worker simulates a crash."""
        while not stop.is_set():
            entries = self._read()
            if not entries:
                continue

            entry_id, fields = entries[0]
            value = int(fields["value"])
            delay = random.uniform(0, 2)
            time.sleep(delay)

            if stop.is_set():
                return

            if random.random() < 0.1:
                logger.warning(
                    "simulated worker death with value %s pending", value
                )
                return

            if value == 0:
                self.redis.xack(self.stream, GROUP, entry_id)
                logger.info("countdown reached 0")
                stop.set()
                return

            with self.redis.pipeline() as pipeline:
                pipeline.xadd(self.stream, {"value": value - 1})
                pipeline.xack(self.stream, GROUP, entry_id)
                next_id, _ = pipeline.execute()
            logger.info("%s -> %s (event %s)", value, value - 1, next_id)


def run_worker(redis: Redis, stream: str, stop: threading.Event) -> None:
    """Replace a simulated dead worker so the group keeps ten consumers."""
    while not stop.is_set():
        Worker(redis, stream).run(stop)
        if not stop.is_set():
            logger.info("restarting worker after simulated death")


def main() -> None:
    """Reset the demo stream, seed it, and start ten worker threads."""
    redis = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    stop = threading.Event()

    try:
        redis.delete(STREAM)
        redis.xadd(STREAM, {"value": INITIAL_VALUE})
        redis.xgroup_create(STREAM, GROUP, id="0")

        workers = [
            threading.Thread(
                target=run_worker,
                args=(redis, STREAM, stop),
                name=f"worker-{index + 1}",
            )
            for index in range(WORKER_COUNT)
        ]
        for worker in workers:
            worker.start()

        logger.info(
            "started %s workers on %s with value %s",
            WORKER_COUNT,
            STREAM,
            INITIAL_VALUE,
        )
        try:
            while not stop.is_set():
                time.sleep(0.25)
        except KeyboardInterrupt:
            logger.info("stopping workers")
            stop.set()
        finally:
            stop.set()
            for worker in workers:
                worker.join()
    finally:
        redis.close()


if __name__ == "__main__":
    main()
