"""Execute the complete Silver workflow for one Bronze candidate."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.silver.build_models import SilverBuild
from metrka_core.pipeline.silver.candidate_dataset_preparation import (
    PreparedSilverDataset,
    SilverDatasetPreparationDeps,
)
from metrka_core.pipeline.silver.candidate_finalization import (
    SilverCandidateFinalizationDeps,
    finalize_candidate_build,
)
from metrka_core.pipeline.silver.candidate_processing import (
    SilverCandidatePreparationDeps,
    SilverCandidatePreparationRequest,
    SilverCandidatePreparationStatus,
    prepare_silver_candidate,
)
from metrka_core.pipeline.silver.candidate_selection import SilverCandidateSelectionDeps
from metrka_core.pipeline.silver.candidate_table_build import (
    SilverCandidateTableBuildDeps,
    build_candidate_tables,
)
from metrka_core.pipeline.silver.config_fingerprints import calculate_quality_config_hash
from metrka_core.pipeline.silver.dependencies import SilverProcessDeps
from metrka_core.pipeline.silver.process_models import (
    SilverCandidateOutcome,
    SilverCandidateOutcomeStatus,
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
)
from metrka_core.pipeline.silver.silver_build_finalization import SilverBuildFinalizationDeps
from metrka_core.pipeline.silver.staging_cleanup import (
    SilverStagingCleanupDeps,
    SilverStagingCleanupRequest,
    cleanup_finalized_staging,
)
from metrka_core.pipeline.silver.task_models import SilverTaskConfig


@dataclass(frozen=True)
class SilverCandidateExecutionDeps:
    """Narrow stage dependencies used by one candidate execution."""

    selection: SilverCandidateSelectionDeps
    dataset_preparation: SilverDatasetPreparationDeps
    preparation: SilverCandidatePreparationDeps
    table_build: SilverCandidateTableBuildDeps
    finalization: SilverCandidateFinalizationDeps
    cleanup: SilverStagingCleanupDeps
    engine_release_id: str
    quality_config_hash: str


@dataclass(frozen=True)
class SilverCandidateExecutionRequest:
    """Candidate identity and task configuration supplied by the batch loop."""

    dataset_file_id: str
    dataset_id: str
    bronze_run_id: str
    silver_run_id: str
    task: SilverTaskConfig
    dataset: PreparedSilverDataset
    build_signature: str
    matching_successful_build: SilverBuild | None
    force_rebuild: bool


def build_silver_candidate_execution_deps(deps: SilverProcessDeps) -> SilverCandidateExecutionDeps:
    """Project the complete Silver dependency group into stage-specific groups."""

    selection = SilverCandidateSelectionDeps(
        config_store=deps.inputs.config_store,
        contract_store=deps.contracts.contract_store,
        silver_build_store=deps.outputs.silver_build_store,
    )

    dataset_preparation = SilverDatasetPreparationDeps(
        contract_store=deps.contracts.contract_store,
        contract_metadata_store=deps.contracts.contract_metadata_store,
        dataset_catalog_store=deps.contracts.dataset_catalog_store,
        execution_log_store=deps.evidence.execution_log_store,
    )

    preparation = SilverCandidatePreparationDeps(
        clock=deps.clock,
        build_ids=deps.build_ids,
        bronze_store=deps.inputs.bronze_store,
        marshal=deps.inputs.marshal,
        silver_build_store=deps.outputs.silver_build_store,
        execution_log_store=deps.evidence.execution_log_store,
    )

    table_build = SilverCandidateTableBuildDeps(
        clock=deps.clock,
        silver_store=deps.outputs.silver_store,
        silver_build_store=deps.outputs.silver_build_store,
        execution_log_store=deps.evidence.execution_log_store,
        quality_store=deps.evidence.quality_store,
        transformation_impact_store=deps.evidence.transformation_impact_store,
        transformation_impact_ids=deps.evidence.transformation_impact_ids,
        quality_config=deps.quality_config,
    )

    finalization_boundary = SilverBuildFinalizationDeps(
        clock=deps.clock,
        silver_store=deps.outputs.silver_store,
        decision_uow=deps.outputs.publication_decision_uow,
        execution_log_store=deps.evidence.execution_log_store,
    )

    finalization = SilverCandidateFinalizationDeps(
        clock=deps.clock,
        silver_build_store=deps.outputs.silver_build_store,
        finalization=finalization_boundary,
        engine_hash=deps.engine.runtime.identity.engine_hash,
    )

    cleanup = SilverStagingCleanupDeps(
        silver_store=deps.outputs.silver_store,
        execution_log_store=deps.evidence.execution_log_store,
    )

    return SilverCandidateExecutionDeps(
        selection=selection,
        dataset_preparation=dataset_preparation,
        preparation=preparation,
        table_build=table_build,
        finalization=finalization,
        cleanup=cleanup,
        engine_release_id=deps.engine.runtime.release.engine_release_id,
        quality_config_hash=calculate_quality_config_hash(deps.quality_config),
    )


def process_one_candidate(
    *,
    runtime: ActionRuntime,
    deps: SilverCandidateExecutionDeps,
    request: SilverCandidateExecutionRequest,
) -> SilverCandidateOutcome:
    """Prepare, build, finalize and clean up one Bronze candidate."""

    preparation_result = prepare_silver_candidate(
        deps=deps.preparation,
        request=SilverCandidatePreparationRequest(
            pipeline_run_id=runtime.pipeline_run_id,
            silver_run_id=request.silver_run_id,
            dataset_file_id=request.dataset_file_id,
            dataset_id=request.dataset_id,
            bronze_run_id=request.bronze_run_id,
            dataset=request.dataset,
            partition_key=request.task.partition_key,
            version_period_discovery_func=request.task.version_period_discovery_func,
            input_kwargs=dict(request.task.input_kwargs),
            engine_release_id=deps.engine_release_id,
            processing_config_hash=request.task.processing_config_hash,
            quality_config_hash=deps.quality_config_hash,
            build_signature=request.build_signature,
            matching_successful_build=request.matching_successful_build,
            force_rebuild=request.force_rebuild,
        ),
    )

    if preparation_result.status is SilverCandidatePreparationStatus.SKIPPED:
        return SilverCandidateOutcome(
            dataset_id=request.dataset_id, status=SilverCandidateOutcomeStatus.SKIPPED
        )

    if preparation_result.status is SilverCandidatePreparationStatus.FAILED:
        error_code = preparation_result.error_code or "SILVER_PREPARATION_FAILED"
        raise SilverProcessingError(
            SilverDatasetFailure(
                dataset_id=request.dataset_id,
                stage=SilverFailureStage.PREPARATION,
                error_code=error_code,
                message=preparation_result.message,
            )
        )

    candidate = preparation_result.require_candidate()

    table_build = build_candidate_tables(
        runtime=runtime,
        deps=deps.table_build,
        task=request.task,
        candidate=candidate,
        silver_run_id=request.silver_run_id,
    )

    finalization = finalize_candidate_build(
        runtime=runtime,
        deps=deps.finalization,
        task=request.task,
        candidate=candidate,
        table_build=table_build,
        silver_run_id=request.silver_run_id,
    )

    warnings = list(finalization.warnings)

    cleanup_warning = cleanup_finalized_staging(
        deps=deps.cleanup,
        request=SilverStagingCleanupRequest(
            dataset_name=runtime.dataset_name,
            dataset_id=candidate.dataset_id,
            silver_run_id=request.silver_run_id,
            silver_build_id=candidate.silver_build.silver_build_id,
            decision_result=finalization.decision_result,
        ),
    )

    if cleanup_warning is not None:
        warnings.append(cleanup_warning)

    return SilverCandidateOutcome(
        dataset_id=candidate.dataset_id,
        status=SilverCandidateOutcomeStatus.FINALIZED,
        warnings=tuple(warnings),
    )
