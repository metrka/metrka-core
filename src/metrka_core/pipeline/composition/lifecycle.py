"""Record the lifecycle of one pipeline execution."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from metrka_core.datasets.dataset_identity import RunName
from metrka_core.observability.stores import PipelineRunStore
from metrka_core.pipeline.composition.runtime import RuntimeComposition
from metrka_core.pipeline.context import PipelineContext
from metrka_core.pipeline.runtime_services import Clock

logger = logging.getLogger(__name__)


@contextmanager
def pipeline_run(
    *,
    context: PipelineContext,
    runtime: RuntimeComposition,
    pipeline_runs: PipelineRunStore,
    clock: Clock,
    run_name: RunName,
    config_name: str,
) -> Iterator[PipelineContext]:
    """Start, finish and report one pipeline execution."""

    pipeline_runs.start_pipeline_run(
        pipeline_run_id=runtime.pipeline_run_id,
        run_name=run_name,
        config_name=config_name,
        code_provenance=runtime.code_provenance,
        started_at=clock.now_utc(),
    )

    try:
        yield context

    except BaseException as exc:
        error: dict[str, object] = {"type": type(exc).__name__, "message": str(exc)}

        try:
            pipeline_runs.finish_pipeline_run(
                pipeline_run_id=runtime.pipeline_run_id,
                status="failed",
                finished_at=clock.now_utc(),
                error=error,
            )
        except Exception:
            logger.exception("Failed to record failed pipeline run %s", runtime.pipeline_run_id)

        logger.error("Pipeline run %s failed: %s", runtime.pipeline_run_id, exc)

        raise

    else:
        pipeline_runs.finish_pipeline_run(
            pipeline_run_id=runtime.pipeline_run_id, status="success", finished_at=clock.now_utc()
        )

        logger.info("Pipeline run %s finished successfully", runtime.pipeline_run_id)
