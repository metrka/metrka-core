"""Best-effort cleanup of finalized Silver staging files."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.publication_decision_unit_of_work import (
    SilverPublicationDecisionResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SilverStagingCleanupDeps:
    """Dependencies required to remove one finalized staging directory."""

    silver_store: SilverArtifactStore
    execution_log_store: ExecutionLogStore


@dataclass(frozen=True)
class SilverStagingCleanupRequest:
    """Identity and decision metadata for one cleanup operation."""

    dataset_name: str
    dataset_id: str
    silver_run_id: str
    silver_build_id: str
    decision_result: SilverPublicationDecisionResult


def cleanup_finalized_staging(
    *, deps: SilverStagingCleanupDeps, request: SilverStagingCleanupRequest
) -> str | None:
    """Remove staging and return a warning instead of invalidating finalization."""

    decision_result = request.decision_result

    try:
        with run_step(
            dataset=request.dataset_name,
            step="cleanup_silver_staging",
            layer="silver",
            run_id=request.silver_run_id,
            start_meta=ExecutionStepMeta(
                dataset_id=request.dataset_id,
                silver_build_id=request.silver_build_id,
                silver_run_id=request.silver_run_id,
                extra={
                    "publication_decision_status": decision_result.decision.status.value,
                    "publication_candidate_id": (
                        decision_result.candidate.candidate_id
                        if decision_result.candidate is not None
                        else None
                    ),
                    "verified_publication_id": (
                        decision_result.verification.publication_id
                        if decision_result.verification is not None
                        else None
                    ),
                },
            ),
            execution_log_store=deps.execution_log_store,
        ) as cleanup_context:
            deps.silver_store.cleanup_staging(
                run_id=request.silver_run_id, dataset_id=request.dataset_id
            )

            cleanup_context.count_success(1)
            cleanup_context.set_finish_meta(
                ExecutionStepMeta(extra={"staging_cleanup_status": "succeeded"})
            )

        return None
    except Exception:
        warning_message = (
            f"Staging cleanup failed for dataset_id={request.dataset_id} "
            f"silver_build_id={request.silver_build_id}"
        )

        logger.warning(
            "Silver build %s for dataset_id=%s was finalized successfully, but staging "
            "cleanup failed. The persisted publication decision remains valid.",
            request.silver_build_id,
            request.dataset_id,
            exc_info=True,
        )

        return warning_message
