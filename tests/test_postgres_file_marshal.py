from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from metrka_core.metadata.file_marshal_models import BronzeArtifactDigest, SilverCandidateFile
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore


class FakeCursor:
    def __init__(
        self, *, rows: list[dict[str, object]] | None = None, row: dict[str, object] | None = None
    ) -> None:
        self._rows = rows or []
        self._row = row
        self.query: str | None = None
        self.params: object = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.query = query
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, object] | None:
        return self._row


class FakeSession:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor


def _store(cursor: FakeCursor) -> PostgresFileMarshalStore:
    session = cast(PostgresSession, FakeSession(cursor))
    return PostgresFileMarshalStore(session)


def _candidate_row() -> dict[str, object]:
    return {
        "dataset_file_id": "file-1",
        "dataset_id": "example.dataset",
        "bronze_run_id": "bronze-1",
    }


def _marshal_row() -> dict[str, object]:
    return {
        "dataset_file_id": "file-1",
        "dataset_id": "example.dataset",
        "source_url": "https://example.test/source.csv",
        "source_file_name": "stored.csv",
        "original_source_file_name": "source.csv",
        "artifact_role": "data",
        "source_hash": "a" * 64,
        "file_size": 10,
        "ingestion_timestamp": datetime(2026, 8, 16, tzinfo=UTC),
        "source_last_modified": None,
        "row_count_raw": 0,
        "column_count_raw": 1,
        "bronze_run_id": "bronze-1",
        "bronze_artifacts": [{"relative_path": "table.csv", "sha256": "b" * 64, "size_bytes": 8}],
        "silver_run_id": None,
        "landing_path": "landing/source.csv",
        "manifest_path": None,
        "partition_key": None,
        "partition_value": None,
        "is_promoted": False,
        "version_period": None,
        "promoted_at": None,
        "superseded_by_file_id": None,
    }


def test_candidate_query_returns_typed_immutable_records() -> None:
    cursor = FakeCursor(rows=[_candidate_row()])

    candidates = _store(cursor).get_silver_candidate_files(dataset_id="example.dataset")

    assert candidates == (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="example.dataset", bronze_run_id="bronze-1"
        ),
    )
    assert cursor.params == ["example.dataset"]
    assert cursor.query is not None
    assert "jsonb_array_length(f.bronze_artifacts) > 0" in cursor.query


@pytest.mark.parametrize("field_name", ["dataset_file_id", "dataset_id", "bronze_run_id"])
def test_candidate_query_rejects_null_identity_fields(field_name: str) -> None:
    row = _candidate_row()
    row[field_name] = None

    with pytest.raises(ValueError, match=field_name):
        _store(FakeCursor(rows=[row])).get_silver_candidate_files()


def test_candidate_query_rejects_missing_columns() -> None:
    row = _candidate_row()
    del row["bronze_run_id"]

    with pytest.raises(ValueError, match="bronze_run_id"):
        _store(FakeCursor(rows=[row])).get_silver_candidate_files()


def test_marshaled_file_restores_typed_bronze_artifact_manifest() -> None:
    entry = _store(FakeCursor(row=_marshal_row())).get_marshaled_file("file-1")

    assert entry is not None
    assert entry.bronze_artifacts == (
        BronzeArtifactDigest(relative_path="table.csv", sha256="b" * 64, size_bytes=8),
    )


def test_marshaled_file_does_not_default_a_missing_bronze_manifest() -> None:
    row = _marshal_row()
    del row["bronze_artifacts"]

    with pytest.raises(ValueError, match="bronze_artifacts"):
        _store(FakeCursor(row=row)).get_marshaled_file("file-1")


def test_marshaled_file_does_not_default_a_missing_artifact_role() -> None:
    row = _marshal_row()
    del row["artifact_role"]

    with pytest.raises(ValueError, match="artifact_role"):
        _store(FakeCursor(row=row)).get_marshaled_file("file-1")


def test_marshaled_file_rejects_unknown_artifact_role() -> None:
    row = _marshal_row()
    row["artifact_role"] = "unknown"

    with pytest.raises(ValueError, match="Invalid artifact_role"):
        _store(FakeCursor(row=row)).get_marshaled_file("file-1")


def test_promoted_fingerprint_requires_decoded_jsonb() -> None:
    cursor = FakeCursor(row={"fingerprint": '{"source.csv": {}}'})

    with pytest.raises(TypeError, match="decoded object"):
        _store(cursor).get_promoted_fingerprint("example.dataset")


def test_promoted_fingerprint_returns_valid_jsonb_object() -> None:
    fingerprint: dict[str, Any] = {
        "source.csv": {"name": "source.csv", "sha256": "a" * 64, "size": 10}
    }

    actual = _store(FakeCursor(row={"fingerprint": fingerprint})).get_promoted_fingerprint(
        "example.dataset"
    )

    assert actual == fingerprint
