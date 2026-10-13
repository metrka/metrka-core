from __future__ import annotations

from datetime import UTC, datetime

import pytest

from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityFailureCode,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)

FIXED_NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)


def _passed_result(*, file_path: str = "data.parquet") -> AssetIntegrityResult:
    return AssetIntegrityResult(
        file_path=file_path,
        status=AssetIntegrityStatus.PASSED,
        expected_size_bytes=5,
        actual_size_bytes=5,
        expected_checksum="sha256:expected",
        actual_checksum="sha256:expected",
    )


def test_batch_derives_passed_status_from_its_results() -> None:
    batch = AssetIntegrityBatch(checked_at=FIXED_NOW, results=(_passed_result(),))

    assert batch.passed
    assert batch.failed_results == ()


def test_batch_rejects_duplicate_file_paths() -> None:
    result = _passed_result()

    with pytest.raises(ValueError, match="cannot repeat"):
        AssetIntegrityBatch(checked_at=FIXED_NOW, results=(result, result))


def test_failed_result_requires_a_structured_failure_code() -> None:
    with pytest.raises(ValueError, match="at least one failure code"):
        AssetIntegrityResult(
            file_path="data.parquet",
            status=AssetIntegrityStatus.FAILED,
            expected_size_bytes=5,
            actual_size_bytes=5,
            expected_checksum="sha256:expected",
            actual_checksum="sha256:actual",
        )

    result = AssetIntegrityResult(
        file_path="data.parquet",
        status=AssetIntegrityStatus.FAILED,
        expected_size_bytes=5,
        actual_size_bytes=5,
        expected_checksum="sha256:expected",
        actual_checksum="sha256:actual",
        failure_codes=(AssetIntegrityFailureCode.CHECKSUM_MISMATCH,),
    )

    assert not AssetIntegrityBatch(checked_at=FIXED_NOW, results=(result,)).passed
