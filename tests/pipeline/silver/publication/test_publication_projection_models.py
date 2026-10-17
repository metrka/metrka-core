from __future__ import annotations

from datetime import UTC, datetime

import pytest

from metrka_core.catalog.publication_projection_models import (
    DatasetPublicationProjectionState,
    PublicationProjectionKind,
    PublicationProjectionStatus,
)

CHECKED_AT = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)


def test_synchronized_projection_requires_expected_publication() -> None:
    with pytest.raises(ValueError, match="expected publication"):
        DatasetPublicationProjectionState(
            dataset_id="example.dataset",
            projection_kind=PublicationProjectionKind.CURRENT,
            expected_publication_id="publication-2",
            projected_publication_id="publication-1",
            status=PublicationProjectionStatus.SYNCHRONIZED,
            status_changed_at=CHECKED_AT,
            last_attempted_at=CHECKED_AT,
            last_synchronized_at=CHECKED_AT,
        )


def test_stale_projection_requires_structured_error() -> None:
    with pytest.raises(ValueError, match="structured error"):
        DatasetPublicationProjectionState(
            dataset_id="example.dataset",
            projection_kind=PublicationProjectionKind.HISTORY,
            expected_publication_id="publication-2",
            projected_publication_id="publication-1",
            status=PublicationProjectionStatus.STALE,
            status_changed_at=CHECKED_AT,
            last_attempted_at=CHECKED_AT,
        )


def test_pending_projection_can_retain_last_successful_publication() -> None:
    state = DatasetPublicationProjectionState(
        dataset_id="example.dataset",
        projection_kind=PublicationProjectionKind.CURRENT,
        expected_publication_id="publication-2",
        projected_publication_id="publication-1",
        status=PublicationProjectionStatus.PENDING,
        status_changed_at=CHECKED_AT,
        last_synchronized_at=CHECKED_AT,
    )

    assert state.expected_publication_id == "publication-2"
    assert state.projected_publication_id == "publication-1"
