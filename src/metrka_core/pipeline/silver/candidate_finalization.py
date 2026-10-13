"""Finalize one built Silver candidate and interpret its publication decision."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.runtime_services import Clock
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.candidate_processing import PreparedSilverCandidate
from metrka_core.pipeline.silver.candidate_table_build import SilverCandidateTableBuildResult
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.process_models import (
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
)
from metrka_core.pipeline.silver.publication_decision_unit_of_work import (
    SilverPublicationDecisionResult,
)
from metrka_core.pipeline.silver.silver_build_finalization import (
    SilverBuildFinalizationDeps,
    SilverBuildFinalizationError,
    SilverBuildFinalizationRequest,
    finalize_silver_build,
)
from metrka_core.pipeline.silver.task_models import SilverTaskConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SilverCandidateFinalizationDeps:
    """Dependencies required to finalize one built candidate."""

    clock: Clock
    silver_build_store: PostgresSilverBuildStore
    finalization: SilverBuildFinalizationDeps
    engine_hash: str


@dataclass(frozen=True)
class SilverCandidateFinalizationResult:
    """Publication decision and non-fatal observability warnings."""

    decision_result: SilverPublicationDecisionResult
    warnings: tuple[str, ...] = ()


def _mark_running_build_failed(
    *, deps: SilverCandidateFinalizationDeps, silver_build_id: str, error_message: str
) -> None:
    current_build = deps.silver_build_store.get_by_id(silver_build_id)

    if current_build is None or current_build.status is not SilverBuildStatus.RUNNING:
        return

    deps.silver_build_store.mark_failed(
        silver_build_id=silver_build_id,
        completed_at=deps.clock.now_utc(),
        error_code="SILVER_FINALIZATION_FAILED",
        error_message=error_message,
    )


def _log_publication_decision(
    *, dataset_id: str, silver_build_id: str, decision_result: SilverPublicationDecisionResult
) -> None:
    if decision_result.decision.verified_equivalent:
        verification = decision_result.verification

        if verification is None:
            raise RuntimeError("Equivalent Silver decision contains no verification record")

        logger.info(
            "Silver build %s reproduced publication %s. Verification count is now %d. "
            "No public revision was created.",
            silver_build_id,
            verification.publication_id,
            verification.verification_count,
        )
        return

    publication_candidate = decision_result.candidate

    if publication_candidate is None:
        raise RuntimeError("Changed Silver decision contains no publication candidate")

    logger.info(
        "Silver build %s created publication candidate %s with change kind %s. "
        "Approval is required; public data remains unchanged.",
        silver_build_id,
        publication_candidate.candidate_id,
        publication_candidate.change_kind.value,
    )

    logger.debug(
        "Recorded publication decision for dataset_id=%s silver_build_id=%s",
        dataset_id,
        silver_build_id,
    )


def finalize_candidate_build(
    *,
    runtime: ActionRuntime,
    deps: SilverCandidateFinalizationDeps,
    task: SilverTaskConfig,
    candidate: PreparedSilverCandidate,
    table_build: SilverCandidateTableBuildResult,
    silver_run_id: str,
) -> SilverCandidateFinalizationResult:
    """Finalize one candidate or raise a structured finalization failure."""

    silver_build_id = candidate.silver_build.silver_build_id

    try:
        finalization_result = finalize_silver_build(
            deps=deps.finalization,
            request=SilverBuildFinalizationRequest(
                dataset_name=runtime.dataset_name,
                dataset_id=candidate.dataset_id,
                pipeline_run_id=runtime.pipeline_run_id,
                silver_run_id=silver_run_id,
                bronze_file_id=candidate.bronze_file_id,
                bronze_run_id=candidate.bronze_run_id,
                source_file_name=candidate.marshaled_file.source_file_name,
                silver_build_id=silver_build_id,
                engine_release_id=candidate.silver_build.engine_release_id,
                engine_hash=deps.engine_hash,
                processing_config_hash=candidate.silver_build.processing_config_hash,
                quality_config_hash=candidate.silver_build.quality_config_hash,
                version_period=candidate.version_period,
                partition_key=task.partition_key,
                partition_value=candidate.partition_value,
                contract_path=candidate.contract_path,
                contract_snapshot_path=candidate.contract_snapshot_path,
                contract_meta=candidate.contract_meta,
                staged_files=table_build.staged_files,
                catalog_highlight_specs=tuple(dict(spec) for spec in task.catalog_highlights),
                temporal_coverage_spec=task.temporal_coverage,
                rebuild_decision=candidate.rebuild_decision,
                code_provenance=runtime.code_provenance,
                fingerprint=table_build.fingerprint,
            ),
        )
    except SilverBuildFinalizationError as error:
        error_message = str(error)
        logger.error(error_message)

        _mark_running_build_failed(
            deps=deps, silver_build_id=silver_build_id, error_message=error_message
        )

        raise SilverProcessingError(
            SilverDatasetFailure(
                dataset_id=candidate.dataset_id,
                stage=SilverFailureStage.FINALIZATION,
                error_code="SILVER_FINALIZATION_FAILED",
                message=error_message,
                silver_build_id=silver_build_id,
            )
        ) from error

    warnings: list[str] = []

    if finalization_result.observability_warning is not None:
        warning_message = str(finalization_result.observability_warning)
        warnings.append(warning_message)

        logger.warning(
            "Silver build %s for dataset_id=%s was finalized successfully, but "
            "finalization observability did not complete: %s",
            silver_build_id,
            candidate.dataset_id,
            warning_message,
        )

    _log_publication_decision(
        dataset_id=candidate.dataset_id,
        silver_build_id=silver_build_id,
        decision_result=finalization_result.decision_result,
    )

    logger.info(
        "Bronze file %s completed Silver processing for version %s.",
        candidate.bronze_file_id[:8],
        candidate.version_period.value,
    )

    return SilverCandidateFinalizationResult(
        decision_result=finalization_result.decision_result, warnings=tuple(warnings)
    )
