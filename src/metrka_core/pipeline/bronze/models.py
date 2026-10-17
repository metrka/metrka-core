"""Result models produced by Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeIngestResult:
    """Result of ingesting one source asset."""

    dataset_file_id: str
    dataset_id: str
    source_hash: str
    bronze_run_id: str | None
    is_new: bool


@dataclass(frozen=True)
class BronzeBatchResult:
    """Results from ingesting a collection of landed assets."""

    by_stream: dict[str, BronzeIngestResult]
    new_count: int
    duplicate_count: int

    @property
    def total_count(self) -> int:
        return len(self.by_stream)
