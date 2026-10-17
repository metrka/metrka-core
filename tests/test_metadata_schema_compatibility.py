"""Tests for metadata schema compatibility checks."""

from __future__ import annotations

from types import TracebackType

import pytest

from metrka_core.metadata import schema_compatibility
from metrka_core.metadata.schema_compatibility import (
    MetadataSchemaMismatchError,
    inspect_metadata_schema,
    require_metadata_schema_current,
)


class FakeCursor:
    """Return configured Alembic revision information."""

    def __init__(self, *, version_table_exists: bool, current_heads: frozenset[str]) -> None:
        self.version_table_exists = version_table_exists
        self.current_heads = current_heads
        self.executed_queries: list[str] = []
        self.last_query = ""

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def execute(self, query: str, parameters: object | None = None) -> None:
        del parameters

        self.last_query = query
        self.executed_queries.append(query)

    def fetchone(self) -> dict[str, object]:
        if "to_regclass" not in self.last_query:
            raise AssertionError("Unexpected fetchone query")

        return {"version_table": ("alembic_version" if self.version_table_exists else None)}

    def fetchall(self) -> list[dict[str, object]]:
        if "FROM public.alembic_version" not in self.last_query:
            raise AssertionError("Unexpected fetchall query")

        return [{"version_num": revision} for revision in sorted(self.current_heads)]


class FakePostgresSession:
    """Expose only the cursor operation used by the check."""

    def __init__(self, *, version_table_exists: bool, current_heads: frozenset[str]) -> None:
        self.fake_cursor = FakeCursor(
            version_table_exists=version_table_exists, current_heads=current_heads
        )

    def cursor(self) -> FakeCursor:
        return self.fake_cursor


def _require_heads(monkeypatch: pytest.MonkeyPatch, *heads: str) -> None:
    monkeypatch.setattr(
        schema_compatibility, "required_metadata_schema_heads", lambda: frozenset(heads)
    )


def test_schema_is_current_when_heads_match(monkeypatch: pytest.MonkeyPatch) -> None:
    _require_heads(monkeypatch, "0001_initial")

    session = FakePostgresSession(
        version_table_exists=True, current_heads=frozenset({"0001_initial"})
    )

    require_metadata_schema_current(
        session  # type: ignore[arg-type]
    )

    assert all(
        query.lstrip().startswith("SELECT") for query in session.fake_cursor.executed_queries
    )


def test_schema_check_rejects_older_database(monkeypatch: pytest.MonkeyPatch) -> None:
    _require_heads(monkeypatch, "0001_initial")

    session = FakePostgresSession(
        version_table_exists=True, current_heads=frozenset({"outdated_revision"})
    )

    with pytest.raises(
        MetadataSchemaMismatchError, match=("Current revision\\(s\\): outdated_revision")
    ):
        require_metadata_schema_current(
            session  # type: ignore[arg-type]
        )


def test_schema_check_rejects_uninitialized_database(monkeypatch: pytest.MonkeyPatch) -> None:
    _require_heads(monkeypatch, "0001_initial")

    session = FakePostgresSession(version_table_exists=False, current_heads=frozenset())

    status = inspect_metadata_schema(
        session  # type: ignore[arg-type]
    )

    assert not status.is_current
    assert status.current_heads == frozenset()

    with pytest.raises(MetadataSchemaMismatchError, match="<not initialized>"):
        require_metadata_schema_current(
            session  # type: ignore[arg-type]
        )
