"""Publication relationships to normalized file-integrity evidence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from metrka_core.quality.asset_integrity_models import AssetIntegrityBatch


class PublicationIntegrityTrigger(StrEnum):
    """Reason why publication files were inspected."""

    PUBLICATION_COMMIT = "publication_commit"
    RECONCILIATION = "reconciliation"


@dataclass(frozen=True, slots=True)
class PublicationIntegrityCheck:
    """A publication-scoped use of one new integrity batch."""

    publication_id: str
    trigger: PublicationIntegrityTrigger
    batch: AssetIntegrityBatch

    def __post_init__(self) -> None:
        if not self.publication_id.strip():
            raise ValueError("publication_id must not be empty")


@dataclass(frozen=True, slots=True)
class PublicationIntegrityBatchLink:
    """Link an already persisted integrity batch to a publication."""

    publication_id: str
    trigger: PublicationIntegrityTrigger
    integrity_batch_id: int

    def __post_init__(self) -> None:
        if not self.publication_id.strip():
            raise ValueError("publication_id must not be empty")

        if self.integrity_batch_id <= 0:
            raise ValueError("integrity_batch_id must be positive")
