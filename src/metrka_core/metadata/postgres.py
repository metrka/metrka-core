"""Shared PostgreSQL connection and transaction lifecycle."""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


def to_jsonb(value: Any) -> Any:
    """Adapt a Python value to PostgreSQL JSONB."""

    if value is None:
        return None

    return Jsonb(value)


class PostgresSession:
    """
    Own one PostgreSQL connection for a pipeline execution.

    Domain stores share this session but do not own its lifecycle.
    """

    def __init__(self, conninfo: str, *, assume_role: str | None = None) -> None:
        if not conninfo.strip():
            raise ValueError("PostgreSQL conninfo must not be empty")

        self.conninfo = conninfo.strip()

        if assume_role is not None and not isinstance(assume_role, str):
            raise TypeError("assume_role must be a string")

        normalized_role = assume_role.strip() if assume_role is not None else None

        if normalized_role == "":
            raise ValueError("assume_role must not be empty")

        logger.debug("PostgreSQL metadata session connecting.")

        self._connection = psycopg.connect(self.conninfo, autocommit=True, row_factory=dict_row)

        with self._connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")

            if normalized_role is not None:
                cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(normalized_role)))

    def cursor(self) -> Any:
        """Return a cursor context manager."""

        return self._connection.cursor()

    def transaction(self) -> Any:
        """Return a transaction context manager."""

        return self._connection.transaction()

    def close(self) -> None:
        """Close the owned PostgreSQL connection."""

        if self._connection:
            self._connection.close()
            logger.debug("PostgreSQL metadata session closed.")

    def __enter__(self) -> PostgresSession:
        return self

    def __exit__(self, exc_type: object, exc_value: object, exc_tb: object) -> None:
        self.close()
