"""Persistence contract for publication assets."""

from __future__ import annotations

from typing import Protocol

from metrka_core.catalog.publication_asset_models import (
    DatasetPublicationAsset,
    DatasetPublicationAssetRequest,
)


class DatasetPublicationAssetStore(Protocol):
    """Persist and query files belonging to publications."""

    def register(
        self, *, publication_id: str, assets: tuple[DatasetPublicationAssetRequest, ...]
    ) -> tuple[DatasetPublicationAsset, ...]:
        """Idempotently register publication assets."""
        ...

    def list_for_publication(self, *, publication_id: str) -> tuple[DatasetPublicationAsset, ...]:
        """Return all files of one publication."""
        ...

    def list_active(self, *, dataset_id: str) -> tuple[DatasetPublicationAsset, ...]:
        """Return files of all active dataset revisions."""
        ...
