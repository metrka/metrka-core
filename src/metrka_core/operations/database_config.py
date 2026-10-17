"""Resolve the dedicated PostgreSQL connection for operator workflows."""

from __future__ import annotations

import os

OPERATIONS_DSN_ENVIRONMENT_VARIABLE = "METRKA_OPERATIONS_DSN"


def resolve_operations_conninfo(*, conninfo: str | None = None) -> str:
    """Resolve the least-privilege connection used by ``metrka operations``."""

    if conninfo is not None:
        normalized = conninfo.strip()

        if not normalized:
            raise ValueError("Operations conninfo must not be empty")

        return normalized

    environment_conninfo = os.environ.get(OPERATIONS_DSN_ENVIRONMENT_VARIABLE)

    if environment_conninfo is not None:
        normalized = environment_conninfo.strip()

        if normalized:
            return normalized

    raise RuntimeError(
        "PostgreSQL operator configuration is missing. "
        f"Set {OPERATIONS_DSN_ENVIRONMENT_VARIABLE} using the "
        "dedicated metrka_operator connection."
    )
