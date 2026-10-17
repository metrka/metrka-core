"""Models for governed dataset-publication candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class SilverPublicationChangeKind(StrEnum):
    """Kind of logical change detected by the publication gate."""

    NONE = "none"
    INITIAL_PUBLICATION = "initial_publication"
    FINGERPRINT_VERSION_CHANGED = "fingerprint_version_changed"
    FINGERPRINT_ALGORITHM_CHANGED = "fingerprint_algorithm_changed"
    LOGICAL_DATA_CHANGED = "logical_data_changed"
    SCHEMA_CHANGED = "schema_changed"
    LOGICAL_DATA_AND_SCHEMA_CHANGED = "logical_data_and_schema_changed"


class DatasetPublicationCandidateStatus(StrEnum):
    """Lifecycle status of a proposed dataset publication."""

    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    PUBLISHED = "published"


def _require_non_empty(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_sha256(field_name: str, value: str) -> None:
    if len(value) != 64:
        raise ValueError(f"{field_name} must be a SHA-256 hash")


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True)
class DatasetPublicationCandidateRequest:
    """Request human approval for one changed Silver output."""

    candidate_id: str
    dataset_id: str
    version_period: date
    partition_key: str
    partition_value: str
    silver_build_id: str
    baseline_publication_id: str | None
    change_kind: SilverPublicationChangeKind
    fingerprint_version: int
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    logical_data_hash: str
    schema_hash: str
    requested_at: datetime

    def __post_init__(self) -> None:
        for field_name, value in {
            "candidate_id": self.candidate_id,
            "dataset_id": self.dataset_id,
            "partition_key": self.partition_key,
            "partition_value": self.partition_value,
            "silver_build_id": self.silver_build_id,
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "schema_hash_algorithm": self.schema_hash_algorithm,
        }.items():
            _require_non_empty(field_name, value)

        if self.change_kind is SilverPublicationChangeKind.NONE:
            raise ValueError("A publication candidate must describe what changed")

        if self.change_kind is SilverPublicationChangeKind.INITIAL_PUBLICATION:
            if self.baseline_publication_id is not None:
                raise ValueError("Initial publication cannot reference a baseline publication")
        elif self.baseline_publication_id is None:
            raise ValueError("Changed publication candidate requires a baseline publication")

        if self.fingerprint_version < 1:
            raise ValueError("fingerprint_version must be positive")

        _require_sha256("logical_data_hash", self.logical_data_hash)
        _require_sha256("schema_hash", self.schema_hash)
        _require_aware_datetime("requested_at", self.requested_at)


@dataclass(frozen=True)
class DatasetPublicationCandidate:
    """Persisted proposal for a new public revision."""

    candidate_id: str
    dataset_id: str
    version_period: date
    partition_key: str
    partition_value: str
    silver_build_id: str
    baseline_publication_id: str | None
    change_kind: SilverPublicationChangeKind
    status: DatasetPublicationCandidateStatus
    fingerprint_version: int
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    logical_data_hash: str
    schema_hash: str
    requested_at: datetime
    approved_at: datetime | None = None
    approved_by: str | None = None
    rejected_at: datetime | None = None
    rejected_by: str | None = None
    rejection_reason: str | None = None
    publication_id: str | None = None
