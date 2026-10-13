from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from metrka_core.metadata.bronze_artifact_integrity import (
    BronzeArtifactIntegrityError,
    capture_bronze_artifacts,
    require_bronze_artifacts_match_source,
    verify_bronze_artifacts,
)
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.file_marshal_models import (
    BronzeArtifactDigest,
    MarshaledFile,
    MarshalEntry,
)
from metrka_core.observability.execution_events import ExecutionEvent, StepFinishedEvent
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.silver.candidate_dataset_preparation import PreparedSilverDataset
from metrka_core.pipeline.silver.candidate_processing import (
    SilverCandidatePreparationDeps,
    SilverCandidatePreparationRequest,
    SilverCandidatePreparationStatus,
    prepare_silver_candidate,
)
from metrka_core.pipeline.silver.version_period import VersionPeriod


class RecordingExecutionLogStore:
    def __init__(self) -> None:
        self.records: list[ExecutionEvent] = []

    def insert_execution_log(self, event: ExecutionEvent) -> None:
        self.records.append(event)


class FrozenClock:
    def now_utc(self) -> datetime:
        return datetime(2026, 8, 16, 12, 0, tzinfo=UTC)


class FixedBronzeStore:
    def __init__(self, run_dir: Path) -> None:
        self._run_dir = run_dir

    def run_dir(self, *, run_id: str) -> Path:
        assert run_id == "bronze-1"
        return self._run_dir


class FixedMarshal:
    def __init__(self, entry: MarshalEntry) -> None:
        self._entry = entry

    def get(self, file_id: str) -> MarshalEntry | None:
        assert file_id == self._entry.file.dataset_file_id
        return self._entry


def _marshaled_file() -> MarshaledFile:
    return MarshaledFile(
        dataset_file_id="file-1",
        dataset_id="example.dataset",
        source_url="https://example.test/source.csv",
        source_file_name="stored-source.csv",
        original_source_file_name="source.csv",
        source_hash="b" * 64,
        file_size=12,
        ingestion_timestamp=datetime(2026, 8, 16, 10, 0, tzinfo=UTC),
        source_last_modified=None,
        row_count_raw=0,
        column_count_raw=1,
    )


def _version_period(*_args: object) -> VersionPeriod:
    return VersionPeriod(value=date(2026, 1, 1), grain="year", source="test")


def test_capture_and_verify_complete_bronze_manifest(tmp_path: Path) -> None:
    run_dir = tmp_path / "bronze-1"
    nested = run_dir / "nested"
    nested.mkdir(parents=True)
    first = run_dir / "table.csv"
    second = nested / "lookup.csv"
    first.write_text("id\n1\n", encoding="utf-8")
    second.write_text("code\na\n", encoding="utf-8")

    manifest = capture_bronze_artifacts(bronze_run_dir=run_dir, output_paths=[second, first])
    verification = verify_bronze_artifacts(bronze_run_dir=run_dir, expected=manifest)

    assert [artifact.relative_path for artifact in manifest] == ["nested/lookup.csv", "table.csv"]
    assert verification.artifact_count == 2
    assert verification.total_bytes == first.stat().st_size + second.stat().st_size


def test_verification_rejects_same_size_content_replacement(tmp_path: Path) -> None:
    run_dir = tmp_path / "bronze-1"
    run_dir.mkdir()
    artifact_path = run_dir / "table.csv"
    artifact_path.write_bytes(b"original")
    manifest = capture_bronze_artifacts(bronze_run_dir=run_dir, output_paths=[artifact_path])
    artifact_path.write_bytes(b"replaced")

    with pytest.raises(BronzeArtifactIntegrityError) as captured:
        verify_bronze_artifacts(bronze_run_dir=run_dir, expected=manifest)

    assert captured.value.details["failure_code"] == "BRONZE_ARTIFACT_INTEGRITY_MISMATCH"
    assert captured.value.details["hash_mismatches"] == [
        {
            "relative_path": "table.csv",
            "expected_sha256": manifest[0].sha256,
            "actual_sha256": captured.value.details["hash_mismatches"][0]["actual_sha256"],
        }
    ]


def test_verification_rejects_unrecorded_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "bronze-1"
    run_dir.mkdir()
    artifact_path = run_dir / "table.csv"
    artifact_path.write_text("id\n1\n", encoding="utf-8")
    manifest = capture_bronze_artifacts(bronze_run_dir=run_dir, output_paths=[artifact_path])
    (run_dir / "injected.csv").write_text("id\n2\n", encoding="utf-8")

    with pytest.raises(BronzeArtifactIntegrityError) as captured:
        verify_bronze_artifacts(bronze_run_dir=run_dir, expected=manifest)

    assert captured.value.details["unexpected_paths"] == ["injected.csv"]


def test_capture_must_match_the_hash_recorded_from_the_source() -> None:
    captured = (BronzeArtifactDigest(relative_path="table.csv", sha256="b" * 64, size_bytes=10),)
    expected = (BronzeArtifactDigest(relative_path="table.csv", sha256="a" * 64, size_bytes=10),)

    with pytest.raises(BronzeArtifactIntegrityError) as error:
        require_bronze_artifacts_match_source(captured=captured, expected_from_source=expected)

    assert error.value.details["failure_code"] == "BRONZE_ARTIFACT_SOURCE_MISMATCH"


def test_candidate_integrity_failure_happens_before_silver_build_start(tmp_path: Path) -> None:
    run_dir = tmp_path / "bronze-1"
    run_dir.mkdir()
    artifact_path = run_dir / "table.csv"
    artifact_path.write_bytes(b"changed")
    entry = MarshalEntry(
        file=_marshaled_file(),
        bronze_run_id="bronze-1",
        bronze_artifacts=(
            BronzeArtifactDigest(
                relative_path="table.csv", sha256="a" * 64, size_bytes=artifact_path.stat().st_size
            ),
        ),
    )
    build_ids = MagicMock()
    build_store = MagicMock()
    execution_logs = RecordingExecutionLogStore()
    deps = SilverCandidatePreparationDeps(
        clock=FrozenClock(),
        build_ids=build_ids,
        bronze_store=cast(Any, FixedBronzeStore(run_dir)),
        marshal=cast(FileMarshal, FixedMarshal(entry)),
        silver_build_store=build_store,
        execution_log_store=cast(ExecutionLogStore, execution_logs),
    )
    request = SilverCandidatePreparationRequest(
        pipeline_run_id="pipeline-1",
        silver_run_id="silver-1",
        dataset_file_id="file-1",
        dataset_id="example.dataset",
        bronze_run_id="bronze-1",
        dataset=PreparedSilverDataset(
            dataset_id="example.dataset",
            contract_path=tmp_path / "contract.yaml",
            contract_meta={"contract_hash": "c" * 64},
            contract_snapshot_path=tmp_path / "snapshot.yaml",
            configured_tables={"table": {}},
        ),
        partition_key="version_period",
        version_period_discovery_func=_version_period,
        input_kwargs={},
        engine_release_id="engine-1",
        processing_config_hash="p" * 64,
        quality_config_hash="q" * 64,
        build_signature="s" * 64,
        matching_successful_build=None,
        force_rebuild=True,
    )

    result = prepare_silver_candidate(deps=deps, request=request)

    assert result.status is SilverCandidatePreparationStatus.FAILED
    assert result.error_code == "BRONZE_ARTIFACT_INTEGRITY_MISMATCH"
    build_ids.new_silver_build_id.assert_not_called()
    build_store.insert_started.assert_not_called()

    finished = [event for event in execution_logs.records if isinstance(event, StepFinishedEvent)]

    assert finished[-1].step == "verify_bronze_artifact_integrity"
    assert finished[-1].status == "failed"
