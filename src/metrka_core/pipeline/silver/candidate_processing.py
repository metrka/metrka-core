"""Prepare one Bronze candidate for Silver processing."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from metrka_core.metadata.bronze_artifact_integrity import (
    BronzeArtifactIntegrityError,
    verify_bronze_artifacts,
)
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.file_marshal_models import MarshaledFile, MarshalEntry
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.runtime_services import Clock, SilverBuildIdGenerator
from metrka_core.pipeline.silver.build_models import RebuildDecision, SilverBuild, SilverBuildStatus
from metrka_core.pipeline.silver.candidate_dataset_preparation import PreparedSilverDataset
from metrka_core.pipeline.silver.dataset_links import match_bronze_tables, unmatched_bronze_message
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
    SILVER_FINGERPRINT_VERSION,
)
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.rebuild_decision import decide_silver_rebuild
from metrka_core.pipeline.silver.version_period import VersionPeriod, VersionPeriodDiscovery
from metrka_core.storage.bronze_store import BronzeArtifactStore

logger = logging.getLogger(__name__)


class SilverCandidatePreparationStatus(StrEnum):
    """Outcome of evaluating one Bronze candidate."""

    READY = "ready"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class SilverCandidatePreparationDeps:
    """Dependencies required to prepare one Silver candidate."""

    clock: Clock
    build_ids: SilverBuildIdGenerator
    bronze_store: BronzeArtifactStore
    marshal: FileMarshal
    silver_build_store: PostgresSilverBuildStore
    execution_log_store: ExecutionLogStore


@dataclass(frozen=True)
class SilverCandidatePreparationRequest:
    """Inputs required to evaluate one Bronze candidate."""

    pipeline_run_id: str
    silver_run_id: str

    dataset_file_id: str
    dataset_id: str
    bronze_run_id: str

    dataset: PreparedSilverDataset
    partition_key: str
    version_period_discovery_func: VersionPeriodDiscovery
    input_kwargs: dict[str, Any]

    engine_release_id: str
    processing_config_hash: str
    quality_config_hash: str
    build_signature: str
    matching_successful_build: SilverBuild | None

    force_rebuild: bool


@dataclass(frozen=True)
class PreparedSilverCandidate:
    """Validated Bronze candidate ready for table building."""

    dataset_id: str
    bronze_file_id: str
    bronze_run_id: str
    bronze_dir: Path
    marshaled_file: MarshaledFile

    contract_path: Path
    contract_meta: dict[str, str]
    contract_snapshot_path: Path
    configured_tables: dict[str, Any]

    version_period: VersionPeriod
    partition_value: str
    rebuild_decision: RebuildDecision
    silver_build: SilverBuild


@dataclass(frozen=True)
class SilverCandidatePreparationResult:
    """Structured result of candidate preparation."""

    status: SilverCandidatePreparationStatus
    message: str
    candidate: PreparedSilverCandidate | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        if self.status is SilverCandidatePreparationStatus.READY and self.candidate is None:
            raise ValueError("A ready candidate result must contain PreparedSilverCandidate")

        if self.status is not SilverCandidatePreparationStatus.READY and self.candidate is not None:
            raise ValueError(
                "A non-ready candidate result must not contain PreparedSilverCandidate"
            )

        if self.status is SilverCandidatePreparationStatus.FAILED:
            if self.error_code is None or not self.error_code.strip():
                raise ValueError("A failed candidate result must contain error_code")
        elif self.error_code is not None:
            raise ValueError("A non-failed candidate result must not contain error_code")

    def require_candidate(self) -> PreparedSilverCandidate:
        """Return the prepared candidate or fail loudly."""

        if self.candidate is None:
            raise RuntimeError(f"Silver candidate is not ready: {self.message}")

        return self.candidate


def _format_partition_value(version_period: VersionPeriod) -> str:
    if version_period.grain == "year":
        return version_period.value.strftime("%Y")

    if version_period.grain == "month":
        return version_period.value.strftime("%Y-%m")

    return version_period.value.isoformat()


def _discover_version_period(
    *,
    bronze_dir: Path,
    configured_tables: dict[str, Any],
    request: SilverCandidatePreparationRequest,
    marshaled_file: MarshaledFile,
) -> tuple[VersionPeriod, str]:
    """
    Discover the logical dataset period before table building.

    The first Bronze file that feeds a contract table is used.
    """

    matches = match_bronze_tables(bronze_dir, configured_tables)

    if not matches:
        raise RuntimeError(
            unmatched_bronze_message(
                dataset_id=request.dataset_id, bronze_dir=bronze_dir, table_keys=configured_tables
            )
        )

    _, file_path = matches[0]
    version_period = request.version_period_discovery_func(
        file_path, request.input_kwargs, marshaled_file
    )

    return (version_period, _format_partition_value(version_period))


def _failed(
    message: str, *, error_code: str = "SILVER_PREPARATION_FAILED"
) -> SilverCandidatePreparationResult:
    logger.error(message)

    return SilverCandidatePreparationResult(
        status=SilverCandidatePreparationStatus.FAILED, message=message, error_code=error_code
    )


def _verify_candidate_bronze_artifacts(
    *,
    deps: SilverCandidatePreparationDeps,
    request: SilverCandidatePreparationRequest,
    bronze_dir: Path,
    marshal_entry: MarshalEntry,
) -> SilverCandidatePreparationResult | None:
    start_meta = ExecutionStepMeta(
        dataset_id=request.dataset_id,
        dataset_file_id=request.dataset_file_id,
        bronze_run_id=request.bronze_run_id,
        silver_run_id=request.silver_run_id,
        input_file_count=len(marshal_entry.bronze_artifacts),
        input_byte_count=sum(artifact.size_bytes for artifact in marshal_entry.bronze_artifacts),
        extra={"integrity_algorithm": "sha256", "integrity_scope": "bronze_run"},
    )

    try:
        with run_step(
            dataset=request.dataset_id,
            step="verify_bronze_artifact_integrity",
            layer="silver",
            run_id=request.silver_run_id,
            execution_log_store=deps.execution_log_store,
            clock=deps.clock,
            start_meta=start_meta,
        ) as context:
            try:
                verification = verify_bronze_artifacts(
                    bronze_run_dir=bronze_dir, expected=marshal_entry.bronze_artifacts
                )
            except BronzeArtifactIntegrityError as error:
                context.count_failed(1)
                context.set_finish_meta(
                    ExecutionStepMeta(extra={"integrity_status": "failed", **error.details})
                )
                raise

            context.count_success(verification.artifact_count)
            context.set_finish_meta(
                ExecutionStepMeta(
                    input_file_count=verification.artifact_count,
                    input_byte_count=verification.total_bytes,
                    extra={"integrity_status": "passed"},
                )
            )

    except BronzeArtifactIntegrityError as error:
        failure_code = error.details.get("failure_code", "BRONZE_ARTIFACT_INTEGRITY_FAILED")
        return _failed(
            "Bronze artifact integrity verification failed for "
            f"dataset_id={request.dataset_id} dataset_file_id={request.dataset_file_id} "
            f"error_code={failure_code}: {error}",
            error_code=str(failure_code),
        )

    return None


def prepare_silver_candidate(
    *, deps: SilverCandidatePreparationDeps, request: SilverCandidatePreparationRequest
) -> SilverCandidatePreparationResult:
    """
    Validate one Bronze candidate and start a Silver build.

    Returns READY when table building should proceed, SKIPPED
    when an identical successful build already exists, and
    FAILED for an expected candidate-level failure.
    """

    if request.dataset.dataset_id != request.dataset_id:
        raise ValueError("Prepared Silver dataset does not match the candidate dataset_id")

    if request.matching_successful_build is not None and not request.force_rebuild:
        message = (
            "Skipping Silver candidate for "
            f"dataset_id={request.dataset_id} "
            f"dataset_file_id={request.dataset_file_id}; matching successful "
            f"build already exists: {request.matching_successful_build.silver_build_id}"
        )
        logger.info(message)
        return SilverCandidatePreparationResult(
            status=SilverCandidatePreparationStatus.SKIPPED, message=message
        )

    bronze_dir = deps.bronze_store.run_dir(run_id=request.bronze_run_id)

    marshal_entry = deps.marshal.get(request.dataset_file_id)

    if marshal_entry is None:
        return _failed(f"Bronze file {request.dataset_file_id} was not found in FileMarshal")

    marshaled_file = marshal_entry.file

    if marshal_entry.bronze_run_id != request.bronze_run_id:
        return _failed(
            "FileMarshal bronze_run_id does not match the Silver candidate for "
            f"dataset_id={request.dataset_id} dataset_file_id={request.dataset_file_id}"
        )

    integrity_failure = _verify_candidate_bronze_artifacts(
        deps=deps, request=request, bronze_dir=bronze_dir, marshal_entry=marshal_entry
    )

    if integrity_failure is not None:
        return integrity_failure

    try:
        version_period, partition_value = _discover_version_period(
            bronze_dir=bronze_dir,
            configured_tables=request.dataset.configured_tables,
            request=request,
            marshaled_file=marshaled_file,
        )

    except Exception as error:
        message = (
            f"Could not discover Silver version period for dataset_id={request.dataset_id}: {error}"
        )

        logger.error(message)

        return _failed(message)

    logger.info("Discovered Silver partition: %s=%s", request.partition_key, partition_value)

    latest_successful_build = deps.silver_build_store.find_latest_successful_for_version(
        dataset_id=request.dataset_id, partition_value=partition_value
    )

    latest_build_attempt = deps.silver_build_store.find_latest_attempt_for_version(
        dataset_id=request.dataset_id, partition_value=partition_value
    )

    rebuild_decision = decide_silver_rebuild(
        dataset_file_id=request.dataset_file_id,
        contract_hash=request.dataset.contract_meta["contract_hash"],
        engine_release_id=request.engine_release_id,
        processing_config_hash=request.processing_config_hash,
        quality_config_hash=request.quality_config_hash,
        matching_successful_build=request.matching_successful_build,
        latest_successful_build=latest_successful_build,
        latest_build_attempt=latest_build_attempt,
        force_rebuild=request.force_rebuild,
    )

    if rebuild_decision.build_signature != request.build_signature:
        raise RuntimeError("Precomputed Silver build signature does not match rebuild decision")

    silver_build_id = deps.build_ids.new_silver_build_id()

    silver_build = SilverBuild(
        silver_build_id=silver_build_id,
        pipeline_run_id=request.pipeline_run_id,
        silver_run_id=request.silver_run_id,
        dataset_file_id=request.dataset_file_id,
        dataset_id=request.dataset_id,
        version_period=version_period.value,
        partition_key=request.partition_key,
        partition_value=partition_value,
        contract_hash=request.dataset.contract_meta["contract_hash"],
        engine_release_id=request.engine_release_id,
        processing_config_hash=request.processing_config_hash,
        quality_config_hash=request.quality_config_hash,
        fingerprint_version=SILVER_FINGERPRINT_VERSION,
        logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
        schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
        build_signature=request.build_signature,
        status=SilverBuildStatus.RUNNING,
        rebuild_mode=rebuild_decision.mode,
        rebuild_reasons=rebuild_decision.reasons,
        started_at=deps.clock.now_utc(),
    )

    deps.silver_build_store.insert_started(silver_build)

    logger.info(
        "Started Silver build %s for %s version %s. Reasons: %s",
        silver_build_id,
        request.dataset_id,
        partition_value,
        ", ".join(reason.value for reason in rebuild_decision.reasons),
    )

    return SilverCandidatePreparationResult(
        status=SilverCandidatePreparationStatus.READY,
        message="Silver candidate is ready for table building",
        candidate=PreparedSilverCandidate(
            dataset_id=request.dataset_id,
            bronze_file_id=request.dataset_file_id,
            bronze_run_id=request.bronze_run_id,
            bronze_dir=bronze_dir,
            marshaled_file=marshaled_file,
            contract_path=request.dataset.contract_path,
            contract_meta=request.dataset.contract_meta,
            contract_snapshot_path=request.dataset.contract_snapshot_path,
            configured_tables=request.dataset.configured_tables,
            version_period=version_period,
            partition_value=partition_value,
            rebuild_decision=rebuild_decision,
            silver_build=silver_build,
        ),
    )
