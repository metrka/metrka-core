"""Tests for immutable transformation-detail evidence."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from metrka_core.lineage.transformation.models import TransformationImpact
from metrka_core.storage.checksums import format_sha256_checksum
from metrka_core.storage.file_integrity import (
    FileIntegrityExpectation,
    FileIntegrityFailureCode,
    Sha256WorkspaceFileIntegrityVerifier,
)


def _impact(**overrides: object) -> TransformationImpact:
    values: dict[str, object] = {
        "pipeline_run_id": "pipeline-1",
        "dataset_id": "example.records",
        "dataset_file_id": "file-1",
        "bronze_run_id": "bronze-1",
        "silver_run_id": "silver-1",
        "silver_build_id": "build-1",
        "table_key": "records",
        "operation": "cast",
        "column_name": "amount",
        "before_value": "1.50",
        "after_value": "1.50",
        "affected_row_count": 1,
        "transformation_impact_id": "impact-1",
        "recorded_at": datetime(2026, 8, 17, tzinfo=UTC),
        "partition_key": "version_period",
        "partition_value": "2026",
        "version_period": date(2026, 1, 1),
        "contract_hash": "c" * 64,
        "details_path": "data/files/silver/transformation_impacts/impact-1.parquet",
        "details_hash": "1" * 64,
        "details_row_count": 1,
    }
    values.update(overrides)
    return TransformationImpact(**values)  # type: ignore[arg-type]


def test_transformation_details_fields_must_be_set_together() -> None:
    with pytest.raises(ValueError, match="must be set together"):
        _impact(details_hash=None)


def test_transformation_details_hash_must_be_sha256() -> None:
    with pytest.raises(ValueError, match="64-character"):
        _impact(details_hash="not-a-hash")


def test_changed_transformation_details_file_fails_verification(tmp_path: Path) -> None:
    impact = _impact()
    assert impact.details_path is not None
    assert impact.details_hash is not None
    path = tmp_path / impact.details_path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"changed evidence")

    result = Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path).inspect(
        FileIntegrityExpectation(
            artifact_kind="transformation_details",
            owner_id=impact.transformation_impact_id,
            file_path=impact.details_path,
            expected_checksum=format_sha256_checksum(impact.details_hash),
        )
    )

    assert result.failed
    assert result.failure_codes == (FileIntegrityFailureCode.CHECKSUM_MISMATCH,)
    assert result.actual_checksum == f"sha256:{hashlib.sha256(b'changed evidence').hexdigest()}"
