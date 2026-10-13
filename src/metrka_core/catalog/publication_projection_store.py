"""Persistence port for derived publication projection health."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol

from metrka_core.catalog.publication_projection_models import (
    DatasetPublicationProjectionState,
    PublicationProjectionKind,
)


class DatasetPublicationProjectionStateStore(Protocol):
    """Persist queryable synchronization state for recoverable projections."""

    def mark_pending(
        self,
        *,
        dataset_id: str,
        current_publication_id: str,
        history_publication_id: str,
        changed_at: datetime,
    ) -> tuple[DatasetPublicationProjectionState, ...]: ...

    def mark_synchronized(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        publication_id: str,
        checked_at: datetime,
    ) -> DatasetPublicationProjectionState: ...

    def mark_stale(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        expected_publication_id: str,
        checked_at: datetime,
        error: Mapping[str, Any],
    ) -> DatasetPublicationProjectionState: ...

    def get(
        self, *, dataset_id: str, projection_kind: PublicationProjectionKind
    ) -> DatasetPublicationProjectionState | None: ...
