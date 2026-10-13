from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

import metrka_core.metadata.postgres as postgres_module
from metrka_core.metadata.postgres import PostgresSession


def test_postgres_session_does_not_expose_raw_connection() -> None:
    assert not hasattr(PostgresSession, "connection")


def test_postgres_session_assumes_requested_role(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False

    connection = MagicMock()
    connection.cursor.return_value = cursor_context
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(postgres_module.psycopg, "connect", connect)

    role_identifier = object()
    role_statement = object()
    sql_template = MagicMock()
    sql_template.format.return_value = role_statement
    sql_factory = MagicMock(return_value=sql_template)
    identifier_factory = MagicMock(return_value=role_identifier)
    monkeypatch.setattr(postgres_module.sql, "SQL", sql_factory)
    monkeypatch.setattr(postgres_module.sql, "Identifier", identifier_factory)

    PostgresSession("postgresql://example", assume_role="metrka_owner")

    connect.assert_called_once_with(
        "postgresql://example", autocommit=True, row_factory=postgres_module.dict_row
    )
    sql_factory.assert_called_once_with("SET ROLE {}")
    identifier_factory.assert_called_once_with("metrka_owner")
    sql_template.format.assert_called_once_with(role_identifier)
    cursor.execute.assert_has_calls([call("SET TIME ZONE 'UTC'"), call(role_statement)])


def test_postgres_session_rejects_empty_assumed_role() -> None:
    with pytest.raises(ValueError, match="assume_role must not be empty"):
        PostgresSession("postgresql://example", assume_role="  ")
