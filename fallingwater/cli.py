"""Command-line entry point for Fallingwater."""

import argparse
import os
from importlib.metadata import version
from collections.abc import Sequence

from redis import Redis

from .chat import Chat
from .names import generate_name
from .streams import StreamNames
from .utils import logger
from .worker import Worker


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser and its subcommands."""
    parser = argparse.ArgumentParser(prog="fw", description="Fallingwater agent tools.")
    parser.add_argument(
        "-V", "--version", action="version", version=version("fallingwater")
    )
    parser.add_argument(
        "-s", "--silent", action="store_true", default=False, help="Suppress logs."
    )
    parser.add_argument(
        "-r",
        "--redis-url",
        default=os.getenv("FW_REDIS_URL", "redis://localhost:6379/0"),
    )
    parser.add_argument("-n", "--namespace", default=os.getenv("FW_NAMESPACE", "fw"))

    subparsers = parser.add_subparsers(dest="command")
    version_parser = subparsers.add_parser(
        "version", help="Show the installed version."
    )
    version_parser.set_defaults(handler=show_version)

    worker_parser = subparsers.add_parser(
        "worker", help="Run Redis-backed agent workers."
    )
    worker_parser.add_argument("-p", "--proxies", type=int, default=1)
    worker_parser.set_defaults(handler=run_worker)

    chat_parser = subparsers.add_parser("chat", help="Start or resume a conversation.")
    chat_parser.add_argument(
        "conversation", nargs="?", help="Conversation ID to start or resume."
    )
    chat_parser.add_argument(
        "-a",
        "--agent",
        default=os.getenv("FW_AGENT", "pydantic_ai:Agent"),
        help="Agent for a new conversation (default: FW_AGENT or pydantic_ai:Agent).",
    )
    chat_parser.add_argument(
        "-m",
        "--model",
        default=os.getenv("FW_MODEL"),
        help="Model for a new conversation (default: FW_MODEL).",
    )
    chat_parser.add_argument(
        "--group", default="user", help="Conversation reader group (default: user)."
    )
    chat_parser.set_defaults(handler=run_chat)

    web_parser = subparsers.add_parser("web", help="Serve the Fallingwater API.")
    web_parser.add_argument("--host", default="127.0.0.1")
    web_parser.add_argument("--port", type=int, default=8000)
    web_parser.set_defaults(handler=run_web)

    reset_parser = subparsers.add_parser(
        "reset", help="Delete all Redis keys in a Fallingwater namespace."
    )
    reset_parser.add_argument(
        "-n",
        "--namespace",
        dest="reset_namespace",
        help="Namespace to reset (default: global --namespace).",
    )
    reset_parser.set_defaults(handler=run_reset)

    return parser


def show_version(_args: argparse.Namespace) -> int:
    """Print the installed version."""
    print(version("fallingwater"))
    return 0


def run_worker(args: argparse.Namespace) -> int:
    with Redis.from_url(args.redis_url, decode_responses=True) as redis:
        worker = Worker(redis, namespace=args.namespace, proxy_count=args.proxies)
        print(f"Worker running with {args.proxies} proxy(s). Press Ctrl-C to stop.")
        worker.run()
    return 0


def run_chat(args: argparse.Namespace) -> int:
    with Redis.from_url(args.redis_url, decode_responses=True) as redis:
        conversation_id = args.conversation
        if conversation_id is None:
            names = StreamNames(args.namespace)
            for _ in range(100):
                candidate = generate_name()
                if not redis.exists(names.conversation(candidate)):
                    conversation_id = candidate
                    break
            else:
                raise SystemExit("fw chat: could not find an unused conversation name")
        try:
            chat = Chat(
                redis,
                namespace=args.namespace,
                conversation_id=conversation_id,
                agent_path=args.agent,
                model=args.model,
                group=args.group,
            )
        except ValueError as error:
            logger.debug(
                "Could not open conversation %s", conversation_id, exc_info=True
            )
            raise SystemExit(f"fw chat: {error}") from None
        chat.run()
    return 0


def run_web(args: argparse.Namespace) -> int:
    """Run the optional FastAPI app with Uvicorn."""
    try:
        import uvicorn

        from .web import create_app
    except ModuleNotFoundError as error:
        logger.debug("Web dependency is missing", exc_info=True)
        raise SystemExit("fw web requires fallingwater[web]") from error

    app = create_app(redis_url=args.redis_url, namespace=args.namespace)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def run_reset(args: argparse.Namespace) -> int:
    """Delete keys belonging to the selected Fallingwater namespace."""
    namespace = args.namespace if args.reset_namespace is None else args.reset_namespace
    prefix = f"{StreamNames(namespace).namespace}:"
    deleted = 0
    with Redis.from_url(args.redis_url, decode_responses=True) as redis:
        batch: list[str] = []
        for key in redis.scan_iter():
            if key.startswith(prefix):
                batch.append(key)
            if len(batch) == 100:
                deleted += redis.delete(*batch)
                batch.clear()
        if batch:
            deleted += redis.delete(*batch)
    print(f"Deleted {deleted} key(s) from namespace {namespace!r}.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.silent:
        logger.setLevel("ERROR")
    if args.command is None:
        parser.print_help()
        return 0
    return args.handler(args)
