"""
Context manager for one execution step.

Logs step start and step finish events.
Tracks aggregate step counters.
Gathers failure details if the step raises.

It receives the dataset identity directly and delegates DB writes to ExecutionLog.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from metrka_core.observability.execution_events import (
    ExecutionCounts,
    ExecutionError,
    Layer,
    StepStatus,
)
from metrka_core.observability.execution_log import ExecutionLog
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.runtime_services import (
    Clock,
    ExecutionIdGenerator,
    MonotonicClock,
    SystemClock,
    SystemMonotonicClock,
    UuidExecutionIdGenerator,
)

logger = logging.getLogger(__name__)


def _require_non_negative_int(n: int) -> None:
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        raise ValueError("count increment must be a non-negative integer")


@dataclass
class StepContext:
    """One execution scope for one dataset step."""

    execution: ExecutionLog

    success: int = 0
    failed: int = 0
    skipped: int = 0
    blocked: int = 0

    finish_meta: ExecutionStepMeta | None = None

    def set_finish_meta(self, meta: ExecutionStepMeta) -> None:
        """Set metadata emitted on the step_finished event."""
        if not isinstance(meta, ExecutionStepMeta):
            raise TypeError("finish meta must be ExecutionStepMeta")
        self.finish_meta = meta

    def count_success(self, n: int = 1) -> None:
        """Increment success count."""
        _require_non_negative_int(n)
        self.success += n

    def count_failed(self, n: int = 1) -> None:
        """Increment failed count."""
        _require_non_negative_int(n)
        self.failed += n

    def count_skipped(self, n: int = 1) -> None:
        """Increment skipped count."""
        _require_non_negative_int(n)
        self.skipped += n

    def count_blocked(self, n: int = 1) -> None:
        """Increment blocked count."""
        _require_non_negative_int(n)
        self.blocked += n

    def execution_counts(self) -> ExecutionCounts:
        """Return aggregate step counters in execution-log format."""
        return ExecutionCounts(
            success=self.success, failed=self.failed, skipped=self.skipped, blocked=self.blocked
        )


@contextmanager
def run_step(
    *,
    dataset: str,
    step: str,
    layer: Layer,
    execution_log_store: ExecutionLogStore,
    run_id: str | None = None,
    execution_ids: ExecutionIdGenerator | None = None,
    clock: Clock | None = None,
    monotonic_clock: MonotonicClock | None = None,
    start_meta: ExecutionStepMeta | Callable[[ExecutionLog], ExecutionStepMeta] | None = None,
) -> Iterator[StepContext]:
    """Record one pipeline step and preserve its original failure."""

    resolved_execution_ids = (
        execution_ids if execution_ids is not None else UuidExecutionIdGenerator()
    )
    resolved_clock = clock if clock is not None else SystemClock()
    resolved_monotonic_clock = (
        monotonic_clock if monotonic_clock is not None else SystemMonotonicClock()
    )

    execution = ExecutionLog(
        dataset=dataset,
        step=step,
        layer=layer,
        run_id=run_id,
        store=execution_log_store,
        ids=resolved_execution_ids,
        clock=resolved_clock,
        monotonic_clock=resolved_monotonic_clock,
    )
    timer = execution.timer()
    ctx = StepContext(execution=execution)

    status: StepStatus = "success"
    error_obj: ExecutionError | None = None
    primary_error: BaseException | None = None

    resolved_start_meta = start_meta(execution) if callable(start_meta) else start_meta
    execution.step_started(meta=resolved_start_meta)

    try:
        yield ctx

    except BaseException as exc:
        primary_error = exc
        status = "failed" if isinstance(exc, Exception) else "interrupted"
        error_obj = ExecutionError(error_type=type(exc).__name__, message=str(exc))
        raise

    finally:
        finish_meta = (resolved_start_meta or ExecutionStepMeta()).merged_with(ctx.finish_meta)

        try:
            execution.step_finished(
                status=status,
                duration_ms=timer.ms(),
                counts=ctx.execution_counts(),
                error=error_obj,
                meta=finish_meta if finish_meta.to_dict() else None,
            )
        except Exception:
            if primary_error is None:
                raise

            logger.exception("Could not record step_finished for dataset=%s step=%s", dataset, step)
