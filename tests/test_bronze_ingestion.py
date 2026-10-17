from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.file_marshal_errors import DuplicateSourceFileError
from metrka_core.metadata.file_marshal_models import MarshaledFile, MarshalEntry, MarshalEvent
from metrka_core.pipeline.bronze.bronze_ingestion import ingest_to_bronze
from metrka_core.pipeline.runtime_services import (
    UuidBronzeRunIdGenerator,
    UuidDatasetFileIdGenerator,
)
from metrka_core.storage.bronze_store import LocalBronzeArtifactStore

FROZEN_TIME = datetime(2026, 8, 14, 12, 30, tzinfo=UTC)


class FrozenClock:
    """Return one deterministic UTC timestamp."""

    def now_utc(self) -> datetime:
        return FROZEN_TIME


class MarshalStore:
    def __init__(self) -> None:
        self.entries: dict[str, MarshalEntry] = {}
        self.events: list[MarshalEvent] = []

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def upsert_marshaled_file(self, entry: MarshalEntry) -> None:
        self.entries[entry.file.dataset_file_id] = entry

    def insert_marshal_event(self, event: MarshalEvent) -> None:
        self.events.append(event)

    def check_hash_exists(self, dataset_id: str, source_hash: str) -> bool:
        return self.get_marshaled_file_by_hash(dataset_id, source_hash) is not None

    def get_marshaled_file_by_hash(self, dataset_id: str, source_hash: str) -> MarshalEntry | None:
        return next(
            (
                entry
                for entry in self.entries.values()
                if entry.file.dataset_id == dataset_id and entry.file.source_hash == source_hash
            ),
            None,
        )

    def get_marshaled_file(self, dataset_file_id: str) -> MarshalEntry | None:
        return self.entries.get(dataset_file_id)

    def get_promoted_fingerprint(self, dataset_id: str) -> dict[str, object] | None:
        return None


def _bronze_store(tmp_path: Path) -> LocalBronzeArtifactStore:
    return LocalBronzeArtifactStore(
        workspace_root=tmp_path,
        bronze_root=tmp_path / "data" / "files" / "bronze",
        current_root=tmp_path / "data" / "current",
    )


def _ingest(
    *,
    source: Path,
    bronze_store: LocalBronzeArtifactStore,
    marshal_store: MarshalStore,
    execution_store: MagicMock,
    quality_store: MagicMock | None = None,
):
    resolved_quality_store = quality_store if quality_store is not None else MagicMock()

    return ingest_to_bronze(
        clock=FrozenClock(),
        dataset_file_ids=UuidDatasetFileIdGenerator(),
        bronze_run_ids=UuidBronzeRunIdGenerator(),
        dataset_name="adult-lead",
        bronze_store=bronze_store,
        marshal=FileMarshal(marshal_store, clock=FrozenClock()),  # type: ignore[arg-type]
        landed_file=source,
        dataset_id="dataset-1",
        source_url="https://example.test/source.csv",
        execution_log_store=execution_store,
        quality_store=resolved_quality_store,
        file_marshal_store=marshal_store,  # type: ignore[arg-type]
        pipeline_run_id="pipeline-1",
    )


def test_flat_file_quality_evidence_uses_workspace_relative_paths(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id,name\n1,Alice\n", encoding="utf-8")

    bronze_store = _bronze_store(tmp_path)
    quality_store = MagicMock()

    result = _ingest(
        source=source,
        bronze_store=bronze_store,
        marshal_store=MarshalStore(),
        execution_store=MagicMock(),
        quality_store=quality_store,
    )

    assert result is not None
    assert result.bronze_run_id is not None

    quality_records = [
        call.args[0] for call in quality_store.insert_quality_check_run.call_args_list
    ]
    output_record = next(
        record
        for record in quality_records
        if record["check_id"].endswith(".post_bronze.output_files_created")
    )

    bronze_file = bronze_store.run_dir(run_id=result.bronze_run_id) / "source.csv"
    expected_path = bronze_store.relative_path(bronze_file)

    assert output_record["actual"]["output_files"] == [expected_path]
    assert str(tmp_path) not in str(output_record["actual"])


def test_missing_landed_file_returns_none(tmp_path: Path) -> None:
    result = _ingest(
        source=tmp_path / "missing.csv",
        bronze_store=_bronze_store(tmp_path),
        marshal_store=MarshalStore(),
        execution_store=MagicMock(),
    )

    assert result is None


def test_flat_file_is_registered_copied_and_pointed_to(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id,name\n1,Alice\n", encoding="utf-8")
    store = MarshalStore()
    bronze_store = _bronze_store(tmp_path)

    result = _ingest(
        source=source, bronze_store=bronze_store, marshal_store=store, execution_store=MagicMock()
    )

    assert result is not None and result.is_new
    assert result.bronze_run_id is not None
    assert (bronze_store.run_dir(run_id=result.bronze_run_id) / "source.csv").is_file()
    assert (
        tmp_path / "data" / "current" / "latest" / "bronze" / "dataset--dataset-1.json"
    ).is_file()
    assert len(store.entries) == 1

    marshal_entry = next(iter(store.entries.values()))
    marshaled_file = marshal_entry.file

    assert marshaled_file.ingestion_timestamp == FROZEN_TIME
    assert len(marshal_entry.bronze_artifacts) == 1

    bronze_artifact = marshal_entry.bronze_artifacts[0]
    bronze_file = bronze_store.run_dir(run_id=result.bronze_run_id) / "source.csv"

    assert bronze_artifact.relative_path == "source.csv"
    assert bronze_artifact.size_bytes == bronze_file.stat().st_size
    assert bronze_artifact.sha256 == hashlib.sha256(bronze_file.read_bytes()).hexdigest()
    assert bronze_artifact.sha256 == result.source_hash
    assert store.events[-1].reason == "record_bronze_artifacts"

    pointer_path = tmp_path / "data" / "current" / "latest" / "bronze" / "dataset--dataset-1.json"

    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))

    assert pointer["updated_at_utc"] == (FROZEN_TIME.isoformat())


def test_duplicate_content_reuses_file_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id,name\n1,Alice\n", encoding="utf-8")
    store = MarshalStore()
    bronze_store = _bronze_store(tmp_path)
    execution_store = MagicMock()

    first = _ingest(
        source=source,
        bronze_store=bronze_store,
        marshal_store=store,
        execution_store=execution_store,
    )
    second = _ingest(
        source=source,
        bronze_store=bronze_store,
        marshal_store=store,
        execution_store=execution_store,
    )

    assert first is not None and second is not None
    assert not second.is_new
    assert second.dataset_file_id == first.dataset_file_id
    assert len(store.entries) == 1


def test_duplicate_branch_depends_on_error_type_not_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id,name\n1,Alice\n", encoding="utf-8")
    store = MarshalStore()
    bronze_store = _bronze_store(tmp_path)
    execution_store = MagicMock()
    first = _ingest(
        source=source,
        bronze_store=bronze_store,
        marshal_store=store,
        execution_store=execution_store,
    )

    def raise_renamed_duplicate(
        _marshal: FileMarshal, file: MarshaledFile, meta: dict[str, Any] | None = None
    ) -> None:
        _ = meta
        error = DuplicateSourceFileError(dataset_id=file.dataset_id, source_hash=file.source_hash)
        error.args = ("Source was already registered",)
        raise error

    monkeypatch.setattr(FileMarshal, "register", raise_renamed_duplicate)

    second = _ingest(
        source=source,
        bronze_store=bronze_store,
        marshal_store=store,
        execution_store=execution_store,
    )

    assert first is not None and second is not None
    assert not second.is_new
    assert second.dataset_file_id == first.dataset_file_id
