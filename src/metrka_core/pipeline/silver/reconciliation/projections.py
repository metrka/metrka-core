"""Repair consumer-facing Silver publication projections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_projection_models import PublicationProjectionKind
from metrka_core.catalog.publication_projection_store import DatasetPublicationProjectionStateStore
from metrka_core.pipeline.silver.publication_indexes import SilverPublicationIndexService
from metrka_core.pipeline.silver.publication_projection import (
    refresh_current_publication_projection,
    refresh_history_publication_projection,
)
from metrka_core.pipeline.silver.reconciliation.models import (
    ProjectionReconciliationResult,
    ProjectionReconciliationStatus,
    PublicationProjectionReconciliation,
)


@dataclass(frozen=True, slots=True)
class PublicationProjectionReconciler:
    """Repair current and history projections independently."""

    publication_indexes: SilverPublicationIndexService
    projection_states: DatasetPublicationProjectionStateStore

    def reconcile(
        self,
        *,
        dataset_id: str,
        current_publication: DatasetPublication | None,
        all_publications: tuple[DatasetPublication, ...],
        checked_at: datetime,
    ) -> PublicationProjectionReconciliation:
        """Repair both projections while retaining either partial failure."""

        current = self._skipped(PublicationProjectionKind.CURRENT)
        history = self._skipped(PublicationProjectionKind.HISTORY)

        if current_publication is not None:
            current = self._reconcile_current(
                dataset_id=dataset_id,
                current_publication=current_publication,
                checked_at=checked_at,
            )

            latest_publication = max(
                all_publications,
                key=lambda publication: (publication.published_at, publication.publication_id),
            )
            history = self._reconcile_history(
                dataset_id=dataset_id,
                expected_publication_id=self._history_expected_publication_id(
                    dataset_id=dataset_id, fallback_publication_id=latest_publication.publication_id
                ),
                checked_at=checked_at,
            )

        return PublicationProjectionReconciliation(current=current, history=history)

    def _reconcile_current(
        self, *, dataset_id: str, current_publication: DatasetPublication, checked_at: datetime
    ) -> ProjectionReconciliationResult:
        try:
            result = refresh_current_publication_projection(
                dataset_id=dataset_id,
                publication=current_publication,
                checked_at=checked_at,
                publication_indexes=self.publication_indexes,
                projection_states=self.projection_states,
            )
        except Exception as error:
            return self._failed(projection_kind=PublicationProjectionKind.CURRENT, error=error)

        return ProjectionReconciliationResult(
            projection_kind=PublicationProjectionKind.CURRENT,
            status=ProjectionReconciliationStatus.REPAIRED,
            paths=(result.pointer_path, *result.view_paths),
        )

    def _reconcile_history(
        self, *, dataset_id: str, expected_publication_id: str, checked_at: datetime
    ) -> ProjectionReconciliationResult:
        try:
            paths = refresh_history_publication_projection(
                dataset_id=dataset_id,
                expected_publication_id=expected_publication_id,
                checked_at=checked_at,
                publication_indexes=self.publication_indexes,
                projection_states=self.projection_states,
            )
        except Exception as error:
            return self._failed(projection_kind=PublicationProjectionKind.HISTORY, error=error)

        return ProjectionReconciliationResult(
            projection_kind=PublicationProjectionKind.HISTORY,
            status=ProjectionReconciliationStatus.REPAIRED,
            paths=paths,
        )

    def _history_expected_publication_id(
        self, *, dataset_id: str, fallback_publication_id: str
    ) -> str:
        state = self.projection_states.get(
            dataset_id=dataset_id, projection_kind=PublicationProjectionKind.HISTORY
        )

        return fallback_publication_id if state is None else state.expected_publication_id

    @staticmethod
    def _skipped(projection_kind: PublicationProjectionKind) -> ProjectionReconciliationResult:
        return ProjectionReconciliationResult(
            projection_kind=projection_kind, status=ProjectionReconciliationStatus.SKIPPED
        )

    @staticmethod
    def _failed(
        *, projection_kind: PublicationProjectionKind, error: Exception
    ) -> ProjectionReconciliationResult:
        return ProjectionReconciliationResult(
            projection_kind=projection_kind,
            status=ProjectionReconciliationStatus.FAILED,
            error_type=type(error).__name__,
            error_message=str(error),
        )
