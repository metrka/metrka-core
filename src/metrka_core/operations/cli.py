"""Stable command-line routing for Metrka operator workflows."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

OperationHandler = Callable[[Sequence[str]], int]

_COMMAND_HELP = {
    "metadata": "Manage the PostgreSQL metadata schema.",
    "engine-releases": "Inspect and decide Silver engine releases.",
    "publication-candidates": "Inspect, decide, and publish Silver candidates.",
    "reconcile-publications": "Repair publication projections and audit artifacts.",
}


def _metadata(arguments: Sequence[str]) -> int:
    from metrka_core.metadata.migrations.__main__ import main

    return main(arguments, prog="metrka operations metadata")


def _engine_releases(arguments: Sequence[str]) -> int:
    from metrka_core.pipeline.silver.manage_engine_releases import main

    return main(arguments, prog="metrka operations engine-releases")


def _publication_candidates(arguments: Sequence[str]) -> int:
    from metrka_core.pipeline.silver.manage_publication_candidates import main

    return main(arguments, prog="metrka operations publication-candidates")


def _reconcile_publications(arguments: Sequence[str]) -> int:
    from metrka_core.pipeline.silver.reconcile_publications import main

    return main(arguments, prog="metrka operations reconcile-publications")


_COMMAND_HANDLERS: dict[str, OperationHandler] = {
    "metadata": _metadata,
    "engine-releases": _engine_releases,
    "publication-candidates": _publication_candidates,
    "reconcile-publications": _reconcile_publications,
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metrka operations",
        description="Administer Metrka metadata, governance decisions, and publications.",
    )
    commands = parser.add_subparsers(dest="operation", required=True)

    for command, help_text in _COMMAND_HELP.items():
        commands.add_parser(command, help=help_text, add_help=False)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Route one installed operator command to its existing implementation."""

    arguments = list(argv) if argv is not None else sys.argv[1:]

    if arguments:
        handler = _COMMAND_HANDLERS.get(arguments[0])

        if handler is not None:
            return handler(arguments[1:])

    _build_parser().parse_args(arguments)
    raise RuntimeError("Operation parser returned without selecting a command")
