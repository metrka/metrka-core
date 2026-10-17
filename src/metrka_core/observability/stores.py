"""Persistence interfaces for pipeline observability."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from metrka_core.datasets.dataset_identity import RunName
from metrka_core.observability.execution_events import ExecutionEvent
from metrka_core.pipeline.provenance import CodeProvenance


class PipelineRunStore(Protocol):
    """Persist the lifecycle of one pipeline run."""

    def start_pipeline_run(
        self,
        *,
        pipeline_run_id: str,
        run_name: RunName,
        config_name: str,
        code_provenance: CodeProvenance,
        started_at: datetime,
    ) -> None: ...

    def finish_pipeline_run(
        self,
        *,
        pipeline_run_id: str,
        status: str,
        finished_at: datetime,
        error: dict[str, object] | None = None,
    ) -> None: ...


class ExecutionLogStore(Protocol):
    """Persist structured pipeline execution events."""

    def insert_execution_log(self, event: ExecutionEvent) -> None: ...
