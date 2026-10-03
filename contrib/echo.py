"""Small Redis Streams echo experiment.

With a local Redis server, run these in separate terminals:

    python contrib/echo.py worker
    python contrib/echo.py chat
    python contrib/echo.py info

Both request and reply events use one stream. The worker uses a consumer group
and acknowledges handled entries, but does not reclaim pending entries after a
crash.

Inspect the stream with `redis-cli` or `docker compose exec redis` :

    XLEN fw:echo
    XINFO STREAM fw:echo
    XINFO GROUPS fw:echo
    XPENDING fw:echo echo-proxy

Repeat SCAN with its returned cursor until the cursor is 0. Each reply has a
`request_id` matching its request's stream entry ID. In a second terminal,
watch new entries with:

    redis-cli XREAD BLOCK 0 STREAMS fw:echo '$'

Use `0-0` instead of `$` to read from the beginning.

To check for the demo stream and delete it, including its entries and consumer
group, run:

    redis-cli EXISTS fw:echo
    redis-cli DEL fw:echo

The next `worker` start creates the stream and consumer group again.
"""

import argparse
import signal
from threading import Event
from uuid import uuid4

from redis import Redis
from redis.exceptions import ResponseError

from fallingwater.utils import configure_cli_logging, logger


class EchoAgent:
    """Return each message unchanged, without an AI model."""

    def receive(self, message: str) -> str:
        return message


class Proxy:
    """Read requests from a stream and append the agent's replies."""

    def __init__(self, agent: EchoAgent, redis: Redis, stream: str) -> None:
        self.agent = agent
        self.redis = redis
        self.stream = stream
        self.group = "echo-proxy"
        self.consumer = str(uuid4())
        try:
            redis.xgroup_create(stream, self.group, id="0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                logger.exception("Could not create consumer group for %s", stream)
                raise
            logger.debug("Consumer group %s already exists on %s", self.group, stream)

    def run(self, stop: Event) -> None:
        while not stop.is_set():
            batches = self.redis.xreadgroup(
                self.group,
                self.consumer,
                {self.stream: ">"},
                count=1,
                block=500,
            )
            for _, entries in batches:
                for entry_id, fields in entries:
                    if fields.get("kind") == "request":
                        reply = self.agent.receive(fields["message"])
                        self.redis.xadd(
                            self.stream,
                            {"kind": "reply", "request_id": entry_id, "message": reply},
                        )
                    self.redis.xack(self.stream, self.group, entry_id)
                    if fields.get("kind") == "request":
                        print(".", end="", flush=True)


class CLI:
    """Submit messages and wait for their matching replies on a stream."""

    def __init__(self, stream: str, redis: Redis) -> None:
        self.stream = stream
        self.redis = redis

    def send(self, message: str) -> str:
        return self.redis.xadd(self.stream, {"kind": "request", "message": message})

    def receive(self, request_id: str) -> str:
        cursor = request_id
        while True:
            batches = self.redis.xread({self.stream: cursor}, block=1000)
            for _, entries in batches:
                for entry_id, fields in entries:
                    cursor = entry_id
                    if (
                        fields.get("kind") == "reply"
                        and fields.get("request_id") == request_id
                    ):
                        return fields["message"]

    def run(self) -> None:
        print("Echo chat. Press Ctrl-D or Ctrl-C to exit.")
        while True:
            try:
                message = input("> ")
                if message:
                    print(self.receive(self.send(message)))
            except (EOFError, KeyboardInterrupt):
                logger.debug("Echo chat closed from the terminal")
                print()
                return


def show_info(redis: Redis, stream: str) -> None:
    """Print stream and worker-group status without consuming entries."""
    if not redis.exists(stream):
        print(f"Stream {stream} does not exist.")
        return

    info = redis.xinfo_stream(stream)
    groups = redis.xinfo_groups(stream)
    group = next((item for item in groups if item["name"] == "echo-proxy"), None)

    print(f"Stream: {stream}")
    print(f"Entries: {info['length']}")
    print(f"First ID: {info['first-entry'][0] if info['first-entry'] else '-'}")
    print(f"Last ID: {info['last-entry'][0] if info['last-entry'] else '-'}")
    if group is None:
        print("Worker group: not created")
        return
    print(f"Worker group: {group['name']}")
    print(f"Last delivered ID: {group['last-delivered-id']}")
    print(f"Pending: {group['pending']}")
    print(f"Unread: {group['lag'] if group['lag'] is not None else 'unknown'}")


def main() -> None:
    configure_cli_logging()
    parser = argparse.ArgumentParser(description="Redis Streams echo experiment")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("worker", "chat", "info"):
        command = commands.add_parser(name)
        command.add_argument("--stream", default="fw:echo")
        command.add_argument("--redis-url", default="redis://localhost:6379/0")
    args = parser.parse_args()

    with Redis.from_url(args.redis_url, decode_responses=True) as redis:
        if args.command == "chat":
            CLI(args.stream, redis).run()
        elif args.command == "info":
            show_info(redis, args.stream)
        else:
            proxy = Proxy(EchoAgent(), redis, args.stream)
            stop = Event()

            def request_stop(_signum: int, _frame: object) -> None:
                stop.set()

            previous_int = signal.signal(signal.SIGINT, request_stop)
            previous_term = signal.signal(signal.SIGTERM, request_stop)
            try:
                print("Worker running. Press Ctrl-C to stop.", flush=True)
                proxy.run(stop)
            finally:
                signal.signal(signal.SIGINT, previous_int)
                signal.signal(signal.SIGTERM, previous_term)
                print()


if __name__ == "__main__":
    main()
