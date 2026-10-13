"""Resolve privileged metadata migration configuration."""

from __future__ import annotations

import os
import re

_ROLE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DEFAULT_OWNER_ROLE = "metrka_owner"


def resolve_migration_conninfo(*, conninfo: str | None = None) -> str:
    """
    Resolve a privileged PostgreSQL connection for migrations.

    Runtime ETL credentials should not normally be used for DDL.
    """

    if conninfo is not None:
        normalized = conninfo.strip()

        if not normalized:
            raise ValueError("Migration conninfo must not be empty")

        return normalized

    environment_conninfo = os.environ.get("METRKA_MIGRATION_DSN")

    if environment_conninfo:
        return environment_conninfo.strip()

    raise RuntimeError(
        "PostgreSQL migration configuration is missing. "
        "Set METRKA_MIGRATION_DSN using a database-owner "
        "or migration-role connection."
    )


def resolve_migration_owner_role(*, role: str | None = None) -> str:
    """Resolve the non-login owner role assumed by privileged operations."""

    configured_role = (
        role
        if role is not None
        else os.environ.get("METRKA_MIGRATION_OWNER_ROLE", _DEFAULT_OWNER_ROLE)
    )

    if not isinstance(configured_role, str):
        raise TypeError("Migration owner role must be a string")

    normalized_role = configured_role.strip()

    if _ROLE_PATTERN.fullmatch(normalized_role) is None:
        raise ValueError("METRKA_MIGRATION_OWNER_ROLE must be a PostgreSQL identifier")

    return normalized_role
