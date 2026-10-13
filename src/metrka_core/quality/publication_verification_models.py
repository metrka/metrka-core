"""Models for public-dataset reproducibility verification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


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
class SilverPublicationVerificationRequest:
    """Record that a build reproduced a public dataset."""

    publication_id: str
    engine_hash: str
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    silver_build_id: str
    quality_config_hash: str
    verified_at: datetime

    def __post_init__(self) -> None:
        for field_name, value in {
            "publication_id": self.publication_id,
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "schema_hash_algorithm": self.schema_hash_algorithm,
            "silver_build_id": self.silver_build_id,
        }.items():
            _require_non_empty(field_name, value)

        _require_sha256("engine_hash", self.engine_hash)
        _require_sha256("quality_config_hash", self.quality_config_hash)
        _require_aware_datetime("verified_at", self.verified_at)


@dataclass(frozen=True)
class SilverPublicationVerification:
    """Aggregated reproducibility evidence for one publication."""

    publication_id: str
    engine_hash: str
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    latest_silver_build_id: str
    quality_config_hash: str
    verification_count: int
    first_verified_at: datetime
    last_verified_at: datetime

    def __post_init__(self) -> None:
        if self.verification_count < 1:
            raise ValueError("verification_count must be positive")

        _require_aware_datetime("first_verified_at", self.first_verified_at)
        _require_aware_datetime("last_verified_at", self.last_verified_at)

        if self.last_verified_at < self.first_verified_at:
            raise ValueError("last_verified_at cannot precede first_verified_at")
