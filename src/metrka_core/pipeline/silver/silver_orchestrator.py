"""Batch orchestration for configured Silver candidates."""

from __future__ import annotations

import logging

from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.silver.candidate_dataset_preparation import (
    PreparedSilverDataset,
    prepare_silver_dataset,
)
from metrka_core.pipeline.silver.candidate_execution import (
    SilverCandidateExecutionRequest,
    build_silver_candidate_execution_deps,
    process_one_candidate,
)
from metrka_core.pipeline.silver.candidate_selection import (
    SelectedSilverCandidate,
    select_silver_candidates,
)
from metrka_core.pipeline.silver.dependencies import SilverProcessDeps
from metrka_core.pipeline.silver.process_models import (
    SilverCandidateOutcome,
    SilverCandidateOutcomeStatus,
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
    SilverProcessResult,
)
from metrka_core.pipeline.silver.task_models import SilverTaskConfig

logger = logging.getLogger(__name__)

__all__ = ["SilverTaskConfig", "process_silver_queue"]


def _missing_task_failure(dataset_id: str) -> SilverProcessingError:
    message = f"No Silver task is configured for dataset_id={dataset_id}"

    return SilverProcessingError(
        SilverDatasetFailure(
            dataset_id=dataset_id,
            stage=SilverFailureStage.PREPARATION,
            error_code="SILVER_TASK_NOT_CONFIGURED",
            message=message,
        )
    )


def _dataset_preparation_failure(dataset_id: str, error: Exception) -> SilverProcessingError:
    return SilverProcessingError(
        SilverDatasetFailure(
            dataset_id=dataset_id,
            stage=SilverFailureStage.PREPARATION,
            error_code="SILVER_DATASET_PREPARATION_FAILED",
            message=str(error),
        )
    )


def _candidate_request(
    *,
    candidate: SelectedSilverCandidate,
    dataset: PreparedSilverDataset,
    silver_run_id: str,
    force_rebuild: bool,
) -> SilverCandidateExecutionRequest:
    """Translate a selected candidate into an execution request."""

    return SilverCandidateExecutionRequest(
        dataset_file_id=candidate.dataset_file_id,
        dataset_id=candidate.dataset_id,
        bronze_run_id=candidate.bronze_run_id,
        silver_run_id=silver_run_id,
        task=candidate.task,
        dataset=dataset,
        build_signature=candidate.build_signature,
        matching_successful_build=candidate.matching_successful_build,
        force_rebuild=force_rebuild,
    )


def process_silver_queue(
    *,
    runtime: ActionRuntime,
    deps: SilverProcessDeps,
    tasks: list[SilverTaskConfig],
    target_dataset_id: str | None = None,
    force_rebuild: bool = False,
) -> SilverProcessResult:
    """Evaluate available Bronze candidates using fail-fast batch semantics."""

    task_map = {task.dataset_id: task for task in tasks}

    if target_dataset_id is not None and target_dataset_id not in task_map:
        raise _missing_task_failure(target_dataset_id)

    # The metadata database is shared by every workspace, so keep only the
    # datasets this workspace configures.
    candidate_files = tuple(
        record
        for record in deps.inputs.file_marshal_store.get_silver_candidate_files(
            dataset_id=target_dataset_id
        )
        if record.dataset_id in task_map
    )

    if not candidate_files:
        logger.info("No active Bronze data assets are available for Silver processing.")
        return SilverProcessResult()

    logger.info("Found %d active Bronze candidates for Silver evaluation.", len(candidate_files))

    candidate_deps = build_silver_candidate_execution_deps(deps)
    result = SilverProcessResult()

    with run_step(
        dataset=runtime.dataset_name,
        step="process_silver_queue",
        layer="silver",
        start_meta=ExecutionStepMeta(extra={"candidate_files_count": len(candidate_files)}),
        execution_log_store=deps.evidence.execution_log_store,
    ) as batch_context:
        silver_run_id = batch_context.execution.run_id

        if silver_run_id is None:
            raise ValueError("Missing batch run_id in Silver orchestrator context")

        outcomes: list[SilverCandidateOutcome] = []

        try:
            selection = select_silver_candidates(
                deps=candidate_deps.selection,
                records=candidate_files,
                task_map=task_map,
                engine_release_id=candidate_deps.engine_release_id,
                quality_config_hash=candidate_deps.quality_config_hash,
                force_rebuild=force_rebuild,
            )
        except KeyError as error:
            dataset_id = str(error.args[0])
            batch_context.count_failed(1)
            raise _missing_task_failure(dataset_id) from error

        for candidate in selection.skipped:
            matching_build = candidate.matching_successful_build
            if matching_build is None:
                raise RuntimeError("Skipped Silver candidate has no matching successful build")

            logger.info(
                "Skipping Silver candidate dataset_id=%s dataset_file_id=%s; "
                "matching successful build=%s",
                candidate.dataset_id,
                candidate.dataset_file_id,
                matching_build.silver_build_id,
            )
            batch_context.count_skipped(1)
            outcomes.append(
                SilverCandidateOutcome(
                    dataset_id=candidate.dataset_id, status=SilverCandidateOutcomeStatus.SKIPPED
                )
            )

        prepared_datasets: dict[str, PreparedSilverDataset] = {}

        for candidate in selection.pending:
            dataset = prepared_datasets.get(candidate.dataset_id)
            if dataset is None:
                try:
                    dataset = prepare_silver_dataset(
                        deps=candidate_deps.dataset_preparation,
                        dataset_name=runtime.dataset_name,
                        dataset_id=candidate.dataset_id,
                        silver_run_id=silver_run_id,
                        identity=candidate.contract_identity,
                    )
                except Exception as error:
                    batch_context.count_failed(1)
                    raise _dataset_preparation_failure(candidate.dataset_id, error) from error

                prepared_datasets[candidate.dataset_id] = dataset

            try:
                outcome = process_one_candidate(
                    runtime=runtime,
                    deps=candidate_deps,
                    request=_candidate_request(
                        candidate=candidate,
                        dataset=dataset,
                        silver_run_id=silver_run_id,
                        force_rebuild=force_rebuild,
                    ),
                )
            except SilverProcessingError:
                batch_context.count_failed(1)
                raise

            if outcome.status is SilverCandidateOutcomeStatus.SKIPPED:
                batch_context.count_skipped(1)
            else:
                batch_context.count_success(1)

            outcomes.append(outcome)

        result = SilverProcessResult.from_outcomes(outcomes)

        logger.info(
            "Silver Run Complete. Finalized %d datasets with %d post-build warnings.",
            result.finalized_count,
            result.warning_count,
        )

    return result
