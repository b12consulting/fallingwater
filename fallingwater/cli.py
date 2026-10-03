"""Command-line entry point for Fallingwater."""

import argparse
from importlib.metadata import version
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser and its subcommands."""
    parser = argparse.ArgumentParser(prog="fw", description="Fallingwater agent tools.")
    parser.add_argument("--version", action="version", version=version("fallingwater"))

    subparsers = parser.add_subparsers(dest="command")
    version_parser = subparsers.add_parser(
        "version", help="Show the installed version."
    )
    version_parser.set_defaults(handler=show_version)

    return parser


def show_version(_args: argparse.Namespace) -> int:
    """Print the installed version."""
    print(version("fallingwater"))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    return args.handler(args)
