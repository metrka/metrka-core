"""Domain value objects for source-file lifecycle metadata."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import Any, Literal

from metrka_core.metadata.artifact import ArtifactRole
from metrka_core.storage.checksums import parse_sha256_hex


def _require_utc_datetime(value: datetime, field_name: str) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")

    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field_name} must be in UTC")


@dataclass(frozen=True, slots=True)
class BronzeArtifactDigest:
    """Immutable identity of one file written into a Bronze run directory."""

    relative_path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        path = PurePosixPath(self.relative_path)

        if (
            not self.relative_path.strip()
            or self.relative_path == "."
            or "\\" in self.relative_path
            or path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != self.relative_path
        ):
            raise ValueError("Bronze artifact relative_path must be a normalized relative path")

        try:
            parse_sha256_hex(self.sha256)
        except ValueError as error:
            raise ValueError(
                "Bronze artifact sha256 must be a lowercase SHA-256 hex digest"
            ) from error

        if self.size_bytes < 0:
            raise ValueError("Bronze artifact size_bytes must not be negative")


@dataclass(frozen=True)
class MarshaledFile:
    """Raw file metadata captured at ingestion time."""

    dataset_file_id: str
    dataset_id: str
    source_url: str
    source_file_name: str
    original_source_file_name: str
    source_hash: str
    file_size: int
    ingestion_timestamp: datetime
    source_last_modified: datetime | None
    row_count_raw: int
    column_count_raw: int
    artifact_role: ArtifactRole = "data"

    def __post_init__(self) -> None:
        """Validate basic file invariants."""

        if not isinstance(self.dataset_file_id, str) or not self.dataset_file_id.strip():
            raise ValueError("dataset_file_id must not be blank")

        if self.file_size <= 0:
            raise ValueError("file_size must be > 0")

        if self.row_count_raw < 0:
            raise ValueError("row_count_raw must be >= 0")

        if self.column_count_raw < 1:
            raise ValueError("column_count_raw must be >= 1")

        _require_utc_datetime(self.ingestion_timestamp, "ingestion_timestamp")

        if self.source_last_modified is not None:
            _require_utc_datetime(self.source_last_modified, "source_last_modified")

            if self.source_last_modified > self.ingestion_timestamp:
                raise ValueError("source_last_modified cannot be after ingestion_timestamp")

        if self.artifact_role not in {"data", "source_schema", "documentation"}:
            raise ValueError(f"Invalid artifact_role: {self.artifact_role}")


@dataclass(frozen=True, slots=True)
class SilverCandidateFile:
    """Bronze file identity eligible for Silver evaluation."""

    dataset_file_id: str
    dataset_id: str
    bronze_run_id: str

    def __post_init__(self) -> None:
        for field_name in ("dataset_file_id", "dataset_id", "bronze_run_id"):
            value = getattr(self, field_name)

            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")


@dataclass(frozen=True)
class MarshalEntry:
    """Lifecycle state for one ingested file."""

    file: MarshaledFile
    bronze_run_id: str | None = None
    bronze_artifacts: tuple[BronzeArtifactDigest, ...] = ()
    silver_run_id: str | None = None
    landing_path: str | None = None
    manifest_path: str | None = None
    partition_key: str | None = None
    partition_value: str | None = None
    is_promoted: bool = False
    superseded_by_file_id: str | None = None
    promoted_at: datetime | None = None
    version_period: date | None = None

    def with_bronze_artifacts(self, artifacts: tuple[BronzeArtifactDigest, ...]) -> MarshalEntry:
        """Attach the immutable file manifest produced by this Bronze run."""

        if self.bronze_run_id is None:
            raise ValueError("Cannot record Bronze artifacts without bronze_run_id")

        if not artifacts:
            raise ValueError("Bronze artifact manifest must not be empty")

        ordered = tuple(sorted(artifacts, key=lambda artifact: artifact.relative_path))
        paths = [artifact.relative_path for artifact in ordered]

        if len(paths) != len(set(paths)):
            raise ValueError("Bronze artifact manifest contains duplicate relative paths")

        if self.bronze_artifacts and self.bronze_artifacts != ordered:
            raise ValueError("Bronze artifact manifest is immutable once recorded")

        return replace(self, bronze_artifacts=ordered)

    def demote(self) -> MarshalEntry:
        """Clear the promoted flag."""

        return replace(self, is_promoted=False, promoted_at=None)

    def as_promoted(
        self,
        at: datetime,
        version_period: date,
        silver_run_id: str | None = None,
        manifest_path: str | None = None,
        partition_key: str | None = None,
        partition_value: str | None = None,
    ) -> MarshalEntry:
        """Mark as promoted for a dataset period."""

        if self.superseded_by_file_id is not None:
            raise ValueError("superseded file cannot be promoted")

        _require_utc_datetime(at, "promoted_at")

        return replace(
            self,
            is_promoted=True,
            promoted_at=at,
            version_period=version_period,
            silver_run_id=(silver_run_id or self.silver_run_id),
            manifest_path=(manifest_path or self.manifest_path),
            partition_key=(partition_key or self.partition_key),
            partition_value=(partition_value or self.partition_value),
        )

    def superseded_by(self, new_id: str) -> MarshalEntry:
        """Mark this file as superseded."""

        if new_id == self.file.dataset_file_id:
            raise ValueError("file cannot supersede itself")

        return replace(self, is_promoted=False, promoted_at=None, superseded_by_file_id=new_id)


@dataclass(frozen=True)
class MarshalEvent:
    """Audit record for a marshal mutation."""

    event_ts: datetime
    event_type: Literal["entry_created", "entry_replaced"]
    file_id: str
    reason: str
    old: dict[str, Any] | None
    new: dict[str, Any]
    meta: dict[str, Any]
