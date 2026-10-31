"""
Start a pool of Redis-backed Pydantic AI workers and drain on shutdown.
"""

import signal
from queue import Queue
from threading import Thread, current_thread, main_thread

from redis import Redis

from .worker import Worker
from .utils import logger


class WorkerPool:
    """
    Own several workers and propagate process stop signals to each one.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = "fw",
        worker_count: int = 1,
        claim_idle_ms: int = 300_000,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        self.workers = [
            Worker(redis, namespace=namespace, claim_idle_ms=claim_idle_ms)
            for _ in range(worker_count)
        ]

    def stop(self) -> None:
        for worker in self.workers:
            worker.stop()

    def _run_worker(self, worker: Worker, failures: Queue[Exception]) -> None:
        try:
            worker.run()
        except Exception as error:
            logger.exception("Worker %s stopped with an error", worker.consumer)
            failures.put(error)
            self.stop()

    def _request_stop(self, signum: int, _frame: object) -> None:
        logger.info(
            "Worker pool received %s; stopping at the next saved step",
            signal.Signals(signum).name,
        )
        self.stop()

    def run(self) -> None:
        """
        Run until SIGINT or SIGTERM; active calls finish and checkpoint before exit.

        Signal handlers require this method to be called from the main thread.
        Embedded applications can call ``stop()`` from their own lifecycle code.
        """
        if current_thread() is not main_thread():
            raise RuntimeError("WorkerPool.run() must run in the main thread")

        failures: Queue[Exception] = Queue()
        previous_int = signal.signal(signal.SIGINT, self._request_stop)
        previous_term = signal.signal(signal.SIGTERM, self._request_stop)
        threads = [
            Thread(target=self._run_worker, args=(worker, failures))
            for worker in self.workers
        ]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            self.stop()
            for thread in threads:
                if thread.ident is not None:
                    thread.join()
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)

        if not failures.empty():
            raise failures.get()
