"""Best-effort projection refresh tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_projection_models import (
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.pipeline.silver.publication_indexes import SilverPublicationIndexResult
from metrka_core.pipeline.silver.publication_projection import (
    refresh_current_publication_projection,
    refresh_history_publication_projection,
)

from .fakes import (
    DATASET_ID,
    FIXED_NOW,
    FakePublicationIndexService,
    InMemoryPublicationProjectionStateStore,
    make_publication,
)


def _index_result(publication: DatasetPublication, root: Path) -> SilverPublicationIndexResult:
    return SilverPublicationIndexResult(
        current_publication=publication,
        pointer_path=root / "current.json",
        view_paths=(root / "latest.sql",),
    )


def _pending_states(publication: DatasetPublication) -> InMemoryPublicationProjectionStateStore:
    projection_states = InMemoryPublicationProjectionStateStore()
    projection_states.mark_pending(
        dataset_id=DATASET_ID,
        current_publication_id=publication.publication_id,
        history_publication_id=publication.publication_id,
        changed_at=FIXED_NOW,
    )
    return projection_states


def test_refresh_updates_current_and_history_projections(tmp_path: Path) -> None:
    publication = make_publication()
    indexes = FakePublicationIndexService(
        current_result=_index_result(publication, tmp_path),
        history_paths=(tmp_path / "history.sql",),
    )
    projection_states = _pending_states(publication)

    refresh_current_publication_projection(
        dataset_id=DATASET_ID,
        publication=publication,
        checked_at=FIXED_NOW,
        publication_indexes=indexes,
        projection_states=projection_states,
    )
    refresh_history_publication_projection(
        dataset_id=DATASET_ID,
        expected_publication_id=publication.publication_id,
        checked_at=FIXED_NOW,
        publication_indexes=indexes,
        projection_states=projection_states,
    )

    assert indexes.current_calls == 1
    assert indexes.history_calls == 1
    for projection_kind in PublicationProjectionKind:
        state = projection_states.get(dataset_id=DATASET_ID, projection_kind=projection_kind)
        assert state is not None
        assert state.status is PublicationProjectionStatus.SYNCHRONIZED


def test_failed_current_refresh_marks_only_current_projection_stale(tmp_path: Path) -> None:
    publication = make_publication()
    indexes = FakePublicationIndexService(
        current_result=_index_result(publication, tmp_path),
        history_paths=(tmp_path / "history.sql",),
        current_error=RuntimeError("pointer write failed"),
    )
    projection_states = _pending_states(publication)

    with pytest.raises(RuntimeError, match="pointer write failed"):
        refresh_current_publication_projection(
            dataset_id=DATASET_ID,
            publication=publication,
            checked_at=FIXED_NOW,
            publication_indexes=indexes,
            projection_states=projection_states,
        )
    refresh_history_publication_projection(
        dataset_id=DATASET_ID,
        expected_publication_id=publication.publication_id,
        checked_at=FIXED_NOW,
        publication_indexes=indexes,
        projection_states=projection_states,
    )

    current_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.CURRENT
    )
    history_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.HISTORY
    )
    assert current_state is not None
    assert current_state.status is PublicationProjectionStatus.STALE
    assert current_state.error == {"type": "RuntimeError", "message": "pointer write failed"}
    assert history_state is not None
    assert history_state.status is PublicationProjectionStatus.SYNCHRONIZED


def test_failed_history_refresh_marks_history_projection_stale(tmp_path: Path) -> None:
    publication = make_publication()
    indexes = FakePublicationIndexService(
        current_result=_index_result(publication, tmp_path),
        history_error=RuntimeError("history failed"),
    )
    projection_states = _pending_states(publication)

    with pytest.raises(RuntimeError, match="history failed"):
        refresh_history_publication_projection(
            dataset_id=DATASET_ID,
            expected_publication_id=publication.publication_id,
            checked_at=FIXED_NOW,
            publication_indexes=indexes,
            projection_states=projection_states,
        )

    state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.HISTORY
    )
    assert state is not None
    assert state.status is PublicationProjectionStatus.STALE


def test_superseded_refresh_cannot_mark_newer_publication_synchronized(tmp_path: Path) -> None:
    attempted_publication = make_publication(publication_id="publication-1")
    projection_states = InMemoryPublicationProjectionStateStore()
    projection_states.mark_pending(
        dataset_id=DATASET_ID,
        current_publication_id="publication-2",
        history_publication_id="publication-2",
        changed_at=FIXED_NOW,
    )
    indexes = FakePublicationIndexService(
        current_result=_index_result(attempted_publication, tmp_path)
    )

    with pytest.raises(RuntimeError, match="superseded"):
        refresh_current_publication_projection(
            dataset_id=DATASET_ID,
            publication=attempted_publication,
            checked_at=FIXED_NOW,
            publication_indexes=indexes,
            projection_states=projection_states,
        )

    state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.CURRENT
    )
    assert state is not None
    assert state.expected_publication_id == "publication-2"
    assert state.status is PublicationProjectionStatus.PENDING
