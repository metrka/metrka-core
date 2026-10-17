"""Verify metadata database compatibility with metrka-core."""

from __future__ import annotations

from dataclasses import dataclass

from alembic.script import ScriptDirectory

from metrka_core.metadata.migrations.runner import build_alembic_config
from metrka_core.metadata.postgres import PostgresSession


class MetadataSchemaMismatchError(RuntimeError):
    """Raised when PostgreSQL is not on the required revision."""


@dataclass(frozen=True)
class MetadataSchemaStatus:
    """Current and required metadata revision heads."""

    current_heads: frozenset[str]
    required_heads: frozenset[str]

    @property
    def is_current(self) -> bool:
        return self.current_heads == self.required_heads


def required_metadata_schema_heads() -> frozenset[str]:
    """Return migration heads required by this core package."""

    script = ScriptDirectory.from_config(build_alembic_config())

    return frozenset(script.get_heads())


def inspect_metadata_schema(session: PostgresSession) -> MetadataSchemaStatus:
    """Read current database revisions without changing schema."""

    required_heads = required_metadata_schema_heads()

    with session.cursor() as cursor:
        cursor.execute(
            """
            SELECT to_regclass(
                'public.alembic_version'
            ) AS version_table
            """
        )

        row = cursor.fetchone()

        if row is None or row["version_table"] is None:
            return MetadataSchemaStatus(current_heads=frozenset(), required_heads=required_heads)

        cursor.execute(
            """
            SELECT version_num
            FROM public.alembic_version
            """
        )

        current_heads = frozenset(str(item["version_num"]) for item in cursor.fetchall())

    return MetadataSchemaStatus(current_heads=current_heads, required_heads=required_heads)


def require_metadata_schema_current(session: PostgresSession) -> None:
    """Stop execution when code and database schema differ."""

    status = inspect_metadata_schema(session)

    if status.is_current:
        return

    current = ", ".join(sorted(status.current_heads)) or "<not initialized>"
    required = ", ".join(sorted(status.required_heads)) or "<no migration head>"

    raise MetadataSchemaMismatchError(
        "Metadata database schema is incompatible. "
        f"Current revision(s): {current}. "
        f"Required revision(s): {required}. "
        "Run `python -m "
        "metrka_core.metadata.migrations upgrade` "
        "using migration credentials before starting "
        "the pipeline."
    )
