"""Start a pool of Redis-backed Pydantic AI proxies and drain on shutdown."""

import signal
from queue import Queue
from threading import Thread, current_thread, main_thread

from redis import Redis

from .proxy import Proxy
from .utils import logger


class Worker:
    """Own several proxies and propagate process stop signals to each one."""

    def __init__(
        self, redis: Redis, *, namespace: str = "fw", proxy_count: int = 1
    ) -> None:
        if proxy_count < 1:
            raise ValueError("proxy_count must be at least 1")
        self.proxies = [Proxy(redis, namespace=namespace) for _ in range(proxy_count)]

    def stop(self) -> None:
        for proxy in self.proxies:
            proxy.stop()

    def _run_proxy(self, proxy: Proxy, failures: Queue[Exception]) -> None:
        try:
            proxy.run()
        except Exception as error:
            logger.exception("Proxy %s stopped with an error", proxy.consumer)
            failures.put(error)
            self.stop()

    def _request_stop(self, signum: int, _frame: object) -> None:
        logger.info(
            "Worker received %s; stopping after active turns",
            signal.Signals(signum).name,
        )
        self.stop()

    def run(self) -> None:
        """Run until SIGINT or SIGTERM; active turns finish before exit.

        Signal handlers require this method to be called from the main thread.
        Embedded applications can call ``stop()`` from their own lifecycle code.
        """
        if current_thread() is not main_thread():
            raise RuntimeError("Worker.run() must run in the main thread")

        failures: Queue[Exception] = Queue()
        previous_int = signal.signal(signal.SIGINT, self._request_stop)
        previous_term = signal.signal(signal.SIGTERM, self._request_stop)
        threads = [
            Thread(target=self._run_proxy, args=(proxy, failures))
            for proxy in self.proxies
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
