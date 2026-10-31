"""
Command-line entry point for Fallingwater.
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from importlib.metadata import version

from redis import Redis

from .chat import Chat
from .names import generate_name
from .stream_utils import (
    decode_conversation_entry,
    decode_dispatch_fields,
    decode_text,
    fmt_conversation_stream,
    fmt_dispatch_stream,
    validate_conversation_id,
)
from .utils import logger
from .worker_pool import WorkerPool
from fallingwater import archive


def build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser and its subcommands.
    """
    parser = argparse.ArgumentParser(
        prog="fw", description="Fallingwater agent tools."
    )
    parser.add_argument(
        "-V", "--version", action="version", version=version("fallingwater")
    )
    parser.add_argument(
        "-s",
        "--silent",
        action="store_true",
        default=False,
        help="Suppress logs.",
    )
    parser.add_argument(
        "-r",
        "--redis-url",
        default=os.getenv("FW_REDIS_URL", "redis://localhost:6379/0"),
    )
    parser.add_argument(
        "-n", "--namespace", default=os.getenv("FW_NAMESPACE", "fw")
    )

    subparsers = parser.add_subparsers(dest="command")
    version_parser = subparsers.add_parser(
        "version", help="Show the installed version."
    )
    version_parser.set_defaults(handler=show_version)

    worker_parser = subparsers.add_parser(
        "worker", help="Run Redis-backed agent workers."
    )
    worker_parser.add_argument("-w", "--workers", type=int, default=1)
    worker_parser.add_argument(
        "--claim-idle-ms",
        type=_nonnegative_int,
        default=300_000,
        help="Idle time before reclaiming pending dispatch entries (default: 300000).",
    )
    worker_parser.set_defaults(handler=run_worker)

    ls_parser = subparsers.add_parser("ls", help="List conversations.")
    ls_parser.set_defaults(handler=run_ls)

    archive_parser = subparsers.add_parser("archive", help="Archive conversation")
    archive_parser.add_argument("conversation", help="Conversation ID.")
    archive_parser.add_argument(
        "-s",
        "--store",
        help="Store URI.",
        default=".fw-store"
    )
    archive_parser.set_defaults(handler=run_archive)

    restore_parser = subparsers.add_parser("restore", help="Restore conversation")
    restore_parser.add_argument("conversation", help="Conversation ID.")
    restore_parser.add_argument(
        "-s",
        "--store",
        help="Store URI.",
        default=".fw-store"
    )
    restore_parser.set_defaults(handler=run_restore)

    tail_parser = subparsers.add_parser(
        "tail", help="Show the latest events for a conversation."
    )
    tail_parser.add_argument("conversation", help="Conversation ID.")
    tail_parser.add_argument(
        "-n",
        "--count",
        type=_positive_int,
        default=10,
        help="Number of latest events to show (default: 10).",
    )
    tail_parser.add_argument(
        "--full",
        action="store_true",
        help="Include message history and usage in completed events.",
    )
    tail_parser.set_defaults(handler=run_tail)

    dispatch_parser = subparsers.add_parser(
        "dispatch", help="Show the latest dispatch events."
    )
    dispatch_parser.add_argument(
        "-n",
        "--count",
        type=_positive_int,
        default=10,
        help="Number of latest events to show (default: 10).",
    )
    dispatch_parser.set_defaults(handler=run_dispatch)

    chat_parser = subparsers.add_parser(
        "chat", help="Start or resume a conversation."
    )
    chat_parser.add_argument(
        "conversation", nargs="?", help="Conversation ID to start or resume."
    )
    chat_parser.add_argument(
        "-p",
        "--param",
        dest="params",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Dependency for a new conversation (repeatable; KEY=VALUE).",
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
        "--group",
        default="user",
        help="Conversation reader group (default: user).",
    )
    chat_parser.add_argument(
        "--msg", help="Send one message, print the response, and exit."
    )
    chat_parser.set_defaults(handler=run_chat)

    web_parser = subparsers.add_parser(
        "web", help="Serve the Fallingwater API."
    )
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


def show_version(_args: argparse.Namespace) -> None:
    """
    Print the installed version.
    """
    print(version("fallingwater"))


def run_worker(args: argparse.Namespace) -> None:
    with Redis.from_url(args.redis_url) as redis:
        worker_pool = WorkerPool(
            redis,
            namespace=args.namespace,
            worker_count=args.workers,
            claim_idle_ms=args.claim_idle_ms,
        )
        print(
            f"Worker pool running with {args.workers} worker(s). Press Ctrl-C to stop."
        )
        worker_pool.run()


def run_ls(args: argparse.Namespace) -> None:
    """
    List conversation IDs found in the selected Redis namespace.
    """
    try:
        pattern = fmt_conversation_stream(args.namespace, "*")
    except ValueError as error:
        raise SystemExit(f"fw ls: {error}") from None
    prefix = f"{args.namespace}:conversation:"
    suffix = ":events"
    with Redis.from_url(args.redis_url) as redis:
        for raw_key in redis.scan_iter(match=pattern):
            key = decode_text(raw_key)
            conversation_id = key[len(prefix) : -len(suffix)]
            try:
                validate_conversation_id(conversation_id)
            except ValueError:
                continue
            print(conversation_id)


def run_archive(args: argparse.Namespace) -> None:
    """
    List conversation IDs found in the selected Redis namespace.
    """
    with Redis.from_url(args.redis_url) as redis:
        file_len = archive.dump_conversation_stream(
            redis,
            args.namespace,
            args.conversation,
            store=args.store,
        )
    logger.info("%s bytes written to %s", file_len, args.store)


def run_restore(args: argparse.Namespace) -> None:
    """
    Restore conversation ID
    """
    with Redis.from_url(args.redis_url) as redis:
        nb_msg = archive.load_conversation_stream(
            redis,
            args.namespace,
            args.conversation,
            store=args.store,
        )
    logger.info("%s messages restored", nb_msg)


def _print_event(
    entry_id: str, event_type: str, payload: Mapping[str, object]
) -> None:
    print(
        f"{entry_id} {event_type} "
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def run_tail(args: argparse.Namespace) -> None:
    """
    Print the latest decoded events from one conversation stream.
    """
    with Redis.from_url(args.redis_url) as redis:
        try:
            stream = fmt_conversation_stream(args.namespace, args.conversation)
        except ValueError as error:
            raise SystemExit(f"fw tail: {error}") from None
        if not redis.exists(stream):
            raise SystemExit(
                f"fw tail: conversation not found: {args.conversation}"
            )
        entries = redis.xrevrange(stream, count=args.count)
        for raw_entry in reversed(entries):
            entry_id, event = decode_conversation_entry(raw_entry)
            payload = event.payload.fmt(full=args.full)
            _print_event(entry_id, event.type.value, payload)


def run_dispatch(args: argparse.Namespace) -> None:
    """
    Print the latest decoded dispatch stream entries.
    """
    with Redis.from_url(args.redis_url) as redis:
        try:
            stream = fmt_dispatch_stream(args.namespace)
        except ValueError as error:
            raise SystemExit(f"fw dispatch: {error}") from None
        entries = redis.xrevrange(stream, count=args.count)
        for raw_entry_id, raw_fields in reversed(entries):
            entry_id = decode_text(raw_entry_id)
            event = decode_dispatch_fields(raw_fields)
            event_type = event.get("type", "unknown")
            payload = {
                key: value for key, value in event.items() if key != "type"
            }
            _print_event(entry_id, event_type, payload)


def run_chat(args: argparse.Namespace) -> None:
    conversation_id = args.conversation
    try:
        deps = _parse_chat_params(args.params)
    except ValueError as error:
        raise SystemExit(f"fw chat: {error}") from None

    with Redis.from_url(args.redis_url) as redis:
        if conversation_id is None:
            for _ in range(100):
                candidate = generate_name()
                stream = fmt_conversation_stream(args.namespace, candidate)
                if not redis.exists(stream):
                    conversation_id = candidate
                    break
            else:
                raise SystemExit(
                    "fw chat: could not find an unused conversation name"
                )
        try:
            chat = Chat(
                redis,
                namespace=args.namespace,
                conversation_id=conversation_id,
                agent_path=args.agent,
                model=args.model,
                group=args.group,
                deps=deps,
            )
        except ValueError as error:
            logger.debug(
                "Could not open conversation %s", conversation_id, exc_info=True
            )
            raise SystemExit(f"fw chat: {error}") from None
        if args.msg is None:
            chat.run()
        else:
            if args.conversation is None:
                print(f"Conversation: {chat.conversation_id}", file=sys.stderr)
            chat.run_once(args.msg)


def _parse_chat_params(params: Sequence[str]) -> dict[str, str]:
    """
    Parse CLI dependency values supplied as ``key=value`` arguments.
    """
    deps: dict[str, str] = {}
    for param in params:
        key, separator, value = param.partition("=")
        if not separator or not key or not value:
            raise ValueError(f"invalid param {param!r}")
        deps[key] = value
    return deps


def _positive_int(value: str) -> int:
    """
    Parse a positive count used by event inspection commands.
    """
    try:
        count = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "must be a positive integer"
        ) from error
    if count < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return count


def _nonnegative_int(value: str) -> int:
    """Parse a nonnegative duration in milliseconds."""
    try:
        duration = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "must be a nonnegative integer"
        ) from error
    if duration < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return duration


def run_web(args: argparse.Namespace) -> None:
    """
    Run the optional FastAPI app with Uvicorn.
    """
    try:
        import uvicorn

        from .web.apirouter import create_app
    except ModuleNotFoundError as error:
        logger.debug("Web dependency is missing", exc_info=True)
        raise SystemExit("fw web requires fallingwater[web]") from error

    with Redis.from_url(args.redis_url) as redis:
        app = create_app(namespace=args.namespace, redis=redis)
        # SSE connections stay open, so give them a short window to finish on exit.
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            timeout_graceful_shutdown=3,
        )


def run_reset(args: argparse.Namespace) -> None:
    """
    Delete keys belonging to the selected Fallingwater namespace.
    """
    namespace = (
        args.namespace if args.reset_namespace is None else args.reset_namespace
    )
    prefix = fmt_dispatch_stream(namespace).removesuffix("dispatch")
    deleted = 0
    with Redis.from_url(args.redis_url) as redis:
        batch: list[str] = []
        for key in redis.scan_iter():
            key = decode_text(key)
            if key.startswith(prefix):
                batch.append(key)
            if len(batch) == 100:
                deleted += redis.delete(*batch)
                batch.clear()
        if batch:
            deleted += redis.delete(*batch)
    print(f"Deleted {deleted} key(s) from namespace {namespace!r}.")


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the command-line interface.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.silent:
        logger.setLevel("ERROR")
    if args.command is None:
        parser.print_help()
        return 0
    args.handler(args)
    return 0
