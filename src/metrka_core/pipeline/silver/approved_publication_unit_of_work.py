"""Command and result for publishing an approved candidate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from metrka_core.catalog.publication_asset_models import DatasetPublicationAsset
from metrka_core.catalog.publication_candidate_models import DatasetPublicationCandidate
from metrka_core.catalog.publication_models import DatasetPublication


@dataclass(frozen=True)
class ApprovedPublicationCommand:
    """Request publication of one approved candidate."""

    candidate_id: str
    published_at: datetime

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValueError("candidate_id must not be empty")

        if self.published_at.utcoffset() is None:
            raise ValueError("published_at must be timezone-aware")


@dataclass(frozen=True)
class ApprovedPublicationResult:
    """Records committed while publishing a candidate."""

    candidate: DatasetPublicationCandidate
    publication: DatasetPublication
    current_publication: DatasetPublication
    publication_assets: tuple[DatasetPublicationAsset, ...]
