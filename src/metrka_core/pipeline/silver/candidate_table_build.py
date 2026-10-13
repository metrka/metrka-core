"""Build every configured physical table for one prepared Silver candidate."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.runtime_services import Clock, TransformationImpactIdGenerator
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.candidate_processing import PreparedSilverCandidate
from metrka_core.pipeline.silver.dataset_links import match_bronze_tables
from metrka_core.pipeline.silver.fingerprints import (
    SilverDatasetFingerprint,
    SilverTableFingerprint,
    combine_silver_table_fingerprints,
)
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.process_models import (
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
)
from metrka_core.pipeline.silver.silver_builder import build_silver_table
from metrka_core.pipeline.silver.task_models import SilverTaskConfig
from metrka_core.quality.config import QualityConfig
from metrka_core.quality.store import QualityCheckStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SilverCandidateTableBuildDeps:
    """Dependencies required to build the tables of one candidate."""

    clock: Clock
    silver_store: SilverArtifactStore
    silver_build_store: PostgresSilverBuildStore
    execution_log_store: ExecutionLogStore
    quality_store: QualityCheckStore
    transformation_impact_store: PostgresTransformationImpactStore
    transformation_impact_ids: TransformationImpactIdGenerator
    quality_config: QualityConfig


@dataclass(frozen=True)
class SilverCandidateTableBuildResult:
    """Staged files and combined logical fingerprint of one candidate."""

    staged_files: tuple[Path, ...]
    fingerprint: SilverDatasetFingerprint


def _mark_failed(
    *,
    deps: SilverCandidateTableBuildDeps,
    silver_build_id: str,
    error_code: str,
    error_message: str,
) -> None:
    deps.silver_build_store.mark_failed(
        silver_build_id=silver_build_id,
        completed_at=deps.clock.now_utc(),
        error_code=error_code,
        error_message=error_message,
    )


def build_candidate_tables(
    *,
    runtime: ActionRuntime,
    deps: SilverCandidateTableBuildDeps,
    task: SilverTaskConfig,
    candidate: PreparedSilverCandidate,
    silver_run_id: str,
) -> SilverCandidateTableBuildResult:
    """Build all contract-selected Bronze files into Silver staging."""

    staged_files: list[Path] = []
    table_fingerprints: list[SilverTableFingerprint] = []
    silver_build_id = candidate.silver_build.silver_build_id

    for table_key, file_path in match_bronze_tables(
        candidate.bronze_dir, candidate.configured_tables
    ):
        try:
            table_build = build_silver_table(
                dataset_name=runtime.dataset_name,
                silver_store=deps.silver_store,
                dataset_id=candidate.dataset_id,
                bronze_file_id=candidate.bronze_file_id,
                bronze_run_id=candidate.bronze_run_id,
                silver_build_id=silver_build_id,
                version_period=candidate.version_period,
                partition_key=task.partition_key,
                partition_value=candidate.partition_value,
                source_file_name=file_path.name,
                bronze_ingested_at=candidate.marshaled_file.ingestion_timestamp,
                silver_processed_at=deps.clock.now_utc(),
                input_file_path=file_path,
                cfg_path=candidate.contract_path,
                contract_meta=candidate.contract_meta,
                table_key=table_key,
                run_id=silver_run_id,
                pipeline_run_id=runtime.pipeline_run_id,
                quality_config=deps.quality_config,
                execution_log_store=deps.execution_log_store,
                quality_store=deps.quality_store,
                transformation_impact_store=deps.transformation_impact_store,
                transformation_impact_ids=deps.transformation_impact_ids,
                input_format=task.input_format,
                input_kwargs=task.input_kwargs,
                output_formats=task.output_formats,
            )
        except Exception as error:
            error_message = (
                f"Silver build failed for dataset_id={candidate.dataset_id} "
                f"table_key={table_key} input_file={file_path.name}: {error}"
            )
            logger.error(error_message)

            _mark_failed(
                deps=deps,
                silver_build_id=silver_build_id,
                error_code="SILVER_TABLE_BUILD_FAILED",
                error_message=error_message,
            )

            raise SilverProcessingError(
                SilverDatasetFailure(
                    dataset_id=candidate.dataset_id,
                    stage=SilverFailureStage.TABLE_BUILD,
                    error_code="SILVER_TABLE_BUILD_FAILED",
                    message=error_message,
                    silver_build_id=silver_build_id,
                    table_key=table_key,
                )
            ) from error

        staged_files.extend(table_build.staged_paths)
        table_fingerprints.append(table_build.fingerprint)

    if not staged_files:
        error_message = (
            f"Silver build produced no output files for dataset_id={candidate.dataset_id}"
        )
        logger.error(error_message)

        _mark_failed(
            deps=deps,
            silver_build_id=silver_build_id,
            error_code="NO_SILVER_OUTPUT",
            error_message=error_message,
        )

        raise SilverProcessingError(
            SilverDatasetFailure(
                dataset_id=candidate.dataset_id,
                stage=SilverFailureStage.EMPTY_OUTPUT,
                error_code="NO_SILVER_OUTPUT",
                message=error_message,
                silver_build_id=silver_build_id,
            )
        )

    return SilverCandidateTableBuildResult(
        staged_files=tuple(staged_files),
        fingerprint=combine_silver_table_fingerprints(table_fingerprints),
    )
