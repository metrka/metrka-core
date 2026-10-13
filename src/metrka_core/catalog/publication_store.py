"""Persistence contract for dataset publications."""

from __future__ import annotations

from typing import Protocol

from metrka_core.catalog.publication_models import DatasetPublication, DatasetPublicationRequest


class DatasetPublicationStore(Protocol):
    """Persist and query public dataset releases."""

    def publish(self, request: DatasetPublicationRequest) -> DatasetPublication: ...

    def get_by_id(self, publication_id: str) -> DatasetPublication | None: ...

    def find_current(self, dataset_id: str) -> DatasetPublication | None: ...

    def find_active(self, *, dataset_id: str, partition_value: str) -> DatasetPublication | None:
        """Return the active revision of one dataset version."""
        ...

    def list_active(self, *, dataset_id: str) -> list[DatasetPublication]: ...

    def list_all(self, *, dataset_id: str) -> list[DatasetPublication]:
        """Return all publication revisions of a dataset."""
        ...
