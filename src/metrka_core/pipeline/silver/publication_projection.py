"""Best-effort refresh of derived Silver projections."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_projection_models import (
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.catalog.publication_projection_store import DatasetPublicationProjectionStateStore
from metrka_core.pipeline.silver.publication_indexes import (
    SilverPublicationIndexResult,
    SilverPublicationIndexService,
)

logger = logging.getLogger(__name__)


def _projection_error(error: BaseException) -> dict[str, str]:
    return {"type": type(error).__name__, "message": str(error)}


def _record_stale_safely(
    *,
    projection_states: DatasetPublicationProjectionStateStore,
    dataset_id: str,
    projection_kind: PublicationProjectionKind,
    publication_id: str,
    checked_at: datetime,
    error: BaseException,
) -> None:
    """Keep the original projection error authoritative if state persistence also fails."""

    try:
        projection_states.mark_stale(
            dataset_id=dataset_id,
            projection_kind=projection_kind,
            expected_publication_id=publication_id,
            checked_at=checked_at,
            error=_projection_error(error),
        )
    except Exception:
        logger.exception(
            "Could not persist stale %s projection state for dataset_id=%s. "
            "The existing pending state remains the conservative health signal.",
            projection_kind.value,
            dataset_id,
        )


def refresh_current_publication_projection(
    *,
    dataset_id: str,
    publication: DatasetPublication,
    checked_at: datetime,
    publication_indexes: SilverPublicationIndexService,
    projection_states: DatasetPublicationProjectionStateStore,
) -> SilverPublicationIndexResult:
    """Refresh the current projection and persist its resulting health."""

    try:
        result = publication_indexes.refresh_current(dataset_id=dataset_id)

        if result.current_publication.publication_id != publication.publication_id:
            raise RuntimeError(
                "Current projection resolved an unexpected publication: "
                f"expected={publication.publication_id} "
                f"actual={result.current_publication.publication_id}"
            )

        state = projection_states.mark_synchronized(
            dataset_id=dataset_id,
            projection_kind=PublicationProjectionKind.CURRENT,
            publication_id=publication.publication_id,
            checked_at=checked_at,
        )

        if (
            state.status is not PublicationProjectionStatus.SYNCHRONIZED
            or state.expected_publication_id != publication.publication_id
        ):
            raise RuntimeError(
                "Current projection refresh was superseded by publication "
                f"{state.expected_publication_id}"
            )

        return result

    except BaseException as error:
        _record_stale_safely(
            projection_states=projection_states,
            dataset_id=dataset_id,
            projection_kind=PublicationProjectionKind.CURRENT,
            publication_id=publication.publication_id,
            checked_at=checked_at,
            error=error,
        )
        raise


def refresh_history_publication_projection(
    *,
    dataset_id: str,
    expected_publication_id: str,
    checked_at: datetime,
    publication_indexes: SilverPublicationIndexService,
    projection_states: DatasetPublicationProjectionStateStore,
) -> tuple[Path, ...]:
    """Refresh the history projection and persist its resulting health."""

    try:
        paths = publication_indexes.rebuild_history(dataset_id=dataset_id)

        state = projection_states.mark_synchronized(
            dataset_id=dataset_id,
            projection_kind=PublicationProjectionKind.HISTORY,
            publication_id=expected_publication_id,
            checked_at=checked_at,
        )

        if (
            state.status is not PublicationProjectionStatus.SYNCHRONIZED
            or state.expected_publication_id != expected_publication_id
        ):
            raise RuntimeError(
                "History projection refresh was superseded by publication "
                f"{state.expected_publication_id}"
            )

        return paths

    except BaseException as error:
        _record_stale_safely(
            projection_states=projection_states,
            dataset_id=dataset_id,
            projection_kind=PublicationProjectionKind.HISTORY,
            publication_id=expected_publication_id,
            checked_at=checked_at,
            error=error,
        )
        raise
