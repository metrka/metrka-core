from __future__ import annotations

import pytest

from metrka_core.pipeline.silver.process_models import (
    SilverCandidateOutcome,
    SilverCandidateOutcomeStatus,
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
    SilverProcessResult,
)


def test_silver_process_result_exposes_structured_counts() -> None:
    result = SilverProcessResult(
        finalized_dataset_ids=("example.county", "example.state"),
        skipped_dataset_ids=("example.archive",),
        warnings=("staging cleanup failed",),
    )

    assert result.finalized_count == 2
    assert result.skipped_count == 1
    assert result.warning_count == 1


def test_silver_process_result_aggregates_candidate_outcomes() -> None:
    result = SilverProcessResult.from_outcomes(
        (
            SilverCandidateOutcome(
                dataset_id="example.county",
                status=SilverCandidateOutcomeStatus.FINALIZED,
                warnings=("cleanup failed",),
            ),
            SilverCandidateOutcome(
                dataset_id="example.archive", status=SilverCandidateOutcomeStatus.SKIPPED
            ),
        )
    )

    assert result.finalized_dataset_ids == ("example.county",)
    assert result.skipped_dataset_ids == ("example.archive",)
    assert result.warnings == ("cleanup failed",)


def test_silver_processing_error_retains_failure_context() -> None:
    failure = SilverDatasetFailure(
        dataset_id="example.county",
        stage=SilverFailureStage.FINALIZATION,
        error_code="SILVER_FINALIZATION_FAILED",
        message="publication decision could not be committed",
        silver_build_id="silver-build-1",
    )

    error = SilverProcessingError(failure)

    assert error.failure is failure
    assert "dataset_id=example.county" in str(error)
    assert "stage=finalization" in str(error)
    assert "error_code=SILVER_FINALIZATION_FAILED" in str(error)
    assert "silver_build_id=silver-build-1" in str(error)


def test_silver_dataset_failure_rejects_empty_identity() -> None:
    with pytest.raises(ValueError, match="dataset_id"):
        SilverDatasetFailure(
            dataset_id="",
            stage=SilverFailureStage.PREPARATION,
            error_code="SILVER_PREPARATION_FAILED",
            message="candidate could not be prepared",
        )
