"""Administrative CLI for Silver engine releases."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from metrka_core.metadata.postgres import PostgresSession
from metrka_core.operations.database_config import resolve_operations_conninfo
from metrka_core.pipeline.runtime_services import Clock, SystemClock
from metrka_core.pipeline.silver.engine_store import (
    DEFAULT_ENGINE_RELEASE_LIST_LIMIT,
    require_engine_release_list_limit,
)
from metrka_core.pipeline.silver.postgres_engine_store import PostgresSilverEngineReleaseStore


def _release_list_limit(value: str) -> int:
    try:
        limit = int(value)
        return require_engine_release_list_limit(limit)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _build_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog, description="Inspect and approve Silver engine releases."
    )

    commands = parser.add_subparsers(dest="command", required=True)

    list_releases = commands.add_parser("list")
    list_releases.add_argument(
        "--limit",
        type=_release_list_limit,
        default=DEFAULT_ENGINE_RELEASE_LIST_LIMIT,
        help=f"maximum releases to show (default: {DEFAULT_ENGINE_RELEASE_LIST_LIMIT})",
    )

    approve = commands.add_parser("approve")
    approve.add_argument("engine_release_id")
    approve.add_argument("--approved-by", required=True)

    reject = commands.add_parser("reject")
    reject.add_argument("engine_release_id")
    reject.add_argument("--rejected-by", required=True)
    reject.add_argument("--reason", required=True)

    return parser


def main(
    argv: Sequence[str] | None = None, *, clock: Clock | None = None, prog: str | None = None
) -> int:
    """Run the Silver engine-release administration command.

    ``argv`` supplies command arguments without the executable name. ``clock``
    supports deterministic approval and rejection timestamps, and ``prog``
    overrides the name shown in help. Successful commands return zero; parser,
    configuration, database, and governance failures propagate as exceptions or
    ``argparse`` exits.
    """

    args = _build_parser(prog=prog).parse_args(argv)

    resolved_clock = clock if clock is not None else SystemClock()

    with PostgresSession(resolve_operations_conninfo()) as session:
        store = PostgresSilverEngineReleaseStore(session)

        if args.command == "list":
            for release in store.list_releases(limit=args.limit):
                print(
                    release.engine_release_id,
                    release.status.value,
                    release.identity.engine_hash[:12],
                    release.identity.runtime_hash[:12],
                    release.core_commit_sha[:12],
                    release.detected_at.isoformat(),
                )

            return 0

        if args.command == "approve":
            release = store.approve(
                engine_release_id=args.engine_release_id,
                approved_by=args.approved_by,
                approved_at=resolved_clock.now_utc(),
            )

            print("Approved:", release.engine_release_id)

            return 0

        release = store.reject(
            engine_release_id=args.engine_release_id,
            rejected_by=args.rejected_by,
            rejection_reason=args.reason,
            rejected_at=resolved_clock.now_utc(),
        )

        print("Rejected:", release.engine_release_id)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
