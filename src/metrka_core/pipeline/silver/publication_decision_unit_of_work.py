"""Atomic persistence boundary for Silver publication decisions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from metrka_core.catalog.publication_candidate_models import DatasetPublicationCandidate
from metrka_core.pipeline.silver.build_models import SilverBuild
from metrka_core.pipeline.silver.fingerprints import SilverDatasetFingerprint
from metrka_core.pipeline.silver.publication_decision import SilverPublicationDecision
from metrka_core.quality.publication_verification_models import SilverPublicationVerification


@dataclass(frozen=True)
class SilverPublicationDecisionCommand:
    """Data required to finalize one successful Silver build."""

    dataset_id: str
    bronze_file_id: str
    silver_build_id: str

    engine_hash: str
    quality_config_hash: str

    version_period: date
    partition_key: str
    partition_value: str

    manifest_path: str
    output_hash: str
    output_file_count: int
    output_byte_count: int

    completed_at: datetime
    fingerprint: SilverDatasetFingerprint
    marshal_meta: Mapping[str, Any]

    def __post_init__(self) -> None:
        required_strings = {
            "dataset_id": self.dataset_id,
            "bronze_file_id": self.bronze_file_id,
            "silver_build_id": self.silver_build_id,
            "partition_key": self.partition_key,
            "partition_value": self.partition_value,
            "manifest_path": self.manifest_path,
        }

        for field_name, value in required_strings.items():
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

        for field_name, value in {
            "engine_hash": self.engine_hash,
            "quality_config_hash": (self.quality_config_hash),
            "output_hash": self.output_hash,
        }.items():
            if len(value) != 64:
                raise ValueError(f"{field_name} must be a SHA-256 hash")

        if self.output_file_count < 1:
            raise ValueError("output_file_count must be positive")

        if self.output_byte_count < 0:
            raise ValueError("output_byte_count must not be negative")

        if self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")


@dataclass(frozen=True)
class SilverPublicationDecisionResult:
    """Atomic result of finalizing one Silver build."""

    completed_build: SilverBuild
    decision: SilverPublicationDecision
    verification: SilverPublicationVerification | None = None
    candidate: DatasetPublicationCandidate | None = None

    def __post_init__(self) -> None:
        if self.decision.verified_equivalent:
            if self.verification is None:
                raise ValueError("Equivalent build requires verification")

            if self.candidate is not None:
                raise ValueError("Equivalent build cannot create a candidate")

        if self.decision.requires_approval:
            if self.candidate is None:
                raise ValueError("Changed build requires a candidate")

            if self.verification is not None:
                raise ValueError("Changed build cannot create verification")
