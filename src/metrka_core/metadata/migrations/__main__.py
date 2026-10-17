"""Command-line interface for Metrka metadata migrations."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from alembic import command
from alembic.config import Config

from metrka_core.metadata.migrations.config import resolve_migration_conninfo
from metrka_core.metadata.migrations.runner import build_alembic_config
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.metadata.schema_compatibility import inspect_metadata_schema


def _database_config() -> Config:
    conninfo = resolve_migration_conninfo()

    return build_alembic_config(conninfo=conninfo)


def main(argv: Sequence[str] | None = None, *, prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog=prog, description="Manage the Metrka metadata database schema."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    upgrade_parser = subparsers.add_parser("upgrade", help="Apply missing metadata migrations.")
    upgrade_parser.add_argument("revision", nargs="?", default="head")

    subparsers.add_parser("current", help="Show the database revision.")

    subparsers.add_parser("heads", help="Show the revisions required by this codebase.")

    subparsers.add_parser("history", help="Show metadata migration history.")

    subparsers.add_parser(
        "check", help=("Verify that the database is on the required metadata revision.")
    )

    stamp_parser = subparsers.add_parser(
        "stamp", help=("Record an existing schema revision without executing its migration.")
    )
    stamp_parser.add_argument("revision")

    args = parser.parse_args(argv)

    if args.command == "heads":
        command.heads(build_alembic_config(), verbose=True)
        return 0

    if args.command == "history":
        command.history(build_alembic_config(), verbose=True)
        return 0

    config = _database_config()

    if args.command == "check":
        conninfo = resolve_migration_conninfo()

        with PostgresSession(conninfo=conninfo) as session:
            status = inspect_metadata_schema(session)

        current = ", ".join(sorted(status.current_heads)) or "<not initialized>"
        required = ", ".join(sorted(status.required_heads)) or "<no migration head>"

        print(f"Current metadata revision: {current}")
        print(f"Required metadata revision: {required}")

        if not status.is_current:
            return 1

        print("Metadata database schema is current.")
        return 0

    if args.command == "upgrade":
        command.upgrade(config, args.revision)
        return 0

    if args.command == "current":
        command.current(config, verbose=True)
        return 0

    if args.command == "stamp":
        command.stamp(config, args.revision)
        return 0

    raise RuntimeError(f"Unsupported migration command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
