"""Models describing one immutable Silver materialization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from metrka_core.storage.checksums import parse_sha256_hex


class SilverBuildStatus(StrEnum):
    """Lifecycle status of one Silver build."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RebuildMode(StrEnum):
    """Whether the rebuild decision was automatic or manual."""

    AUTOMATIC = "automatic"
    MANUAL = "manual"


class RebuildReason(StrEnum):
    """Reason why a new Silver build was required."""

    INITIAL_BUILD = "initial_build"
    PREVIOUS_BUILD_FAILED = "previous_build_failed"
    BRONZE_FILE_CHANGED = "bronze_file_changed"
    CONTRACT_CHANGED = "contract_changed"
    SILVER_ENGINE_CHANGED = "silver_engine_changed"
    PROCESSING_CONFIG_CHANGED = "processing_config_changed"
    QUALITY_CONFIG_CHANGED = "quality_config_changed"
    MANUAL_FORCE = "manual_force"


@dataclass(frozen=True)
class RebuildDecision:
    """Decision made before processing one Bronze asset into Silver."""

    required: bool
    mode: RebuildMode
    build_signature: str
    reasons: tuple[RebuildReason, ...] = ()
    matching_silver_build_id: str | None = None

    def __post_init__(self) -> None:

        if not self.build_signature.strip():
            raise ValueError("RebuildDecision.build_signature must not be empty")

        if self.required and not self.reasons:
            raise ValueError("A required Silver rebuild must include at least one reason")

        if not self.required and self.matching_silver_build_id is None:
            raise ValueError("A skipped Silver rebuild must reference the matching build")


@dataclass(frozen=True)
class SilverBuild:
    """One immutable attempt to materialize a Bronze asset into Silver."""

    silver_build_id: str
    pipeline_run_id: str
    silver_run_id: str
    dataset_file_id: str
    dataset_id: str

    contract_hash: str
    engine_release_id: str
    processing_config_hash: str
    quality_config_hash: str
    build_signature: str
    fingerprint_version: int
    logical_hash_algorithm: str
    schema_hash_algorithm: str

    status: SilverBuildStatus
    rebuild_mode: RebuildMode
    rebuild_reasons: tuple[RebuildReason, ...]

    started_at: datetime

    version_period: date | None = None
    partition_key: str | None = None
    partition_value: str | None = None

    logical_data_hash: str | None = None
    schema_hash: str | None = None

    manifest_path: str | None = None
    output_hash: str | None = None
    output_file_count: int | None = None
    output_byte_count: int | None = None

    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        required_strings = {
            "silver_build_id": self.silver_build_id,
            "pipeline_run_id": self.pipeline_run_id,
            "silver_run_id": self.silver_run_id,
            "dataset_file_id": self.dataset_file_id,
            "dataset_id": self.dataset_id,
            "contract_hash": self.contract_hash,
            "engine_release_id": self.engine_release_id,
            "processing_config_hash": (self.processing_config_hash),
            "quality_config_hash": (self.quality_config_hash),
            "build_signature": self.build_signature,
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "schema_hash_algorithm": self.schema_hash_algorithm,
        }

        for field_name, value in required_strings.items():
            if not value.strip():
                raise ValueError(f"SilverBuild.{field_name} must not be empty")

        for field_name, value in {
            "processing_config_hash": (self.processing_config_hash),
            "quality_config_hash": (self.quality_config_hash),
            "build_signature": self.build_signature,
        }.items():
            if len(value) != 64:
                raise ValueError(f"SilverBuild.{field_name} must be a SHA-256 hash")

        if self.fingerprint_version < 1:
            raise ValueError("SilverBuild.fingerprint_version must be positive")

        for field_name, optional_hash in {
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
            "output_hash": self.output_hash,
        }.items():
            if optional_hash is None:
                continue

            try:
                parse_sha256_hex(optional_hash)
            except ValueError as error:
                raise ValueError(f"SilverBuild.{field_name} must be a SHA-256 hash") from error

        if self.completed_at is not None and self.completed_at.utcoffset() is None:
            raise ValueError("SilverBuild.completed_at must be timezone-aware")

        if self.status is SilverBuildStatus.SUCCEEDED:
            if self.logical_data_hash is None:
                raise ValueError("Successful Silver build requires logical_data_hash")

            if self.schema_hash is None:
                raise ValueError("Successful Silver build requires schema_hash")
