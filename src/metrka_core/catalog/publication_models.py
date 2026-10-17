"""Domain models for published dataset versions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class DatasetPublicationRequest:
    """Request to publish one successful Silver build."""

    publication_id: str
    pipeline_run_id: str
    dataset_id: str
    version_period: date
    partition_key: str
    partition_value: str
    silver_build_id: str
    engine_release_id: str
    processing_config_hash: str
    quality_config_hash: str
    fingerprint_version: int
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    logical_data_hash: str
    schema_hash: str
    manifest_path: str
    published_at: datetime

    def __post_init__(self) -> None:
        required_strings = {
            "publication_id": self.publication_id,
            "pipeline_run_id": self.pipeline_run_id,
            "dataset_id": self.dataset_id,
            "partition_key": self.partition_key,
            "partition_value": self.partition_value,
            "silver_build_id": self.silver_build_id,
            "engine_release_id": self.engine_release_id,
            "processing_config_hash": (self.processing_config_hash),
            "quality_config_hash": (self.quality_config_hash),
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "schema_hash_algorithm": self.schema_hash_algorithm,
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
            "manifest_path": self.manifest_path,
        }

        for field_name, value in required_strings.items():
            if not value.strip():
                raise ValueError(f"DatasetPublicationRequest.{field_name} must not be empty")

        for field_name, value in {
            "processing_config_hash": (self.processing_config_hash),
            "quality_config_hash": (self.quality_config_hash),
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
        }.items():
            if len(value) != 64:
                raise ValueError(f"DatasetPublicationRequest.{field_name} must be a SHA-256 hash")

        if self.fingerprint_version < 1:
            raise ValueError("DatasetPublicationRequest.fingerprint_version must be positive")

        if self.published_at.utcoffset() is None:
            raise ValueError("DatasetPublicationRequest.published_at must be timezone-aware")


@dataclass(frozen=True)
class DatasetPublication:
    """One immutable public release of a Silver build."""

    publication_id: str
    pipeline_run_id: str
    dataset_id: str
    version_period: date
    partition_key: str
    partition_value: str
    revision: int
    silver_build_id: str
    engine_release_id: str
    processing_config_hash: str
    quality_config_hash: str
    fingerprint_version: int
    logical_hash_algorithm: str
    schema_hash_algorithm: str
    logical_data_hash: str
    schema_hash: str
    manifest_path: str
    published_at: datetime
    is_active_revision: bool
    is_current: bool
    supersedes_publication_id: str | None = None

    def __post_init__(self) -> None:
        if not self.logical_hash_algorithm.strip():
            raise ValueError("DatasetPublication.logical_hash_algorithm must not be empty")

        if not self.schema_hash_algorithm.strip():
            raise ValueError("DatasetPublication.schema_hash_algorithm must not be empty")

        if self.revision < 1:
            raise ValueError("DatasetPublication.revision must be positive")

        if self.published_at.utcoffset() is None:
            raise ValueError("DatasetPublication.published_at must be timezone-aware")

        if self.is_current and not self.is_active_revision:
            raise ValueError("Current publication must also be the active revision")
