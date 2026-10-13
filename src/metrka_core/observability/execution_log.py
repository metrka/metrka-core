"""
Helpers for writing execution receipts.

Used to log when a step starts, when it finishes,
how long it took and whether it succeeded or chose a more dramatic ending.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from metrka_core.observability.execution_events import (
    ExecutionCounts,
    ExecutionError,
    Layer,
    StepFinishedEvent,
    StepStartedEvent,
    StepStatus,
)
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


# ================================================================================
# Timer
# ===============================================================================
@dataclass
class ExecutionTimer:
    """Measure elapsed execution time."""

    clock: MonotonicClock
    started_at: float = field(init=False)

    def __post_init__(self) -> None:
        self.started_at = self.clock.monotonic()

    def ms(self) -> int:
        """Return non-negative elapsed milliseconds."""

        elapsed_seconds = self.clock.monotonic() - self.started_at

        return int(max(0.0, elapsed_seconds) * 1000)


# ================================================================================
# ExecutionLog
# ===============================================================================
@dataclass(frozen=True)
class ExecutionLog:
    """
    Write execution summary receipts only:

    - step started
    - step finished
    """

    dataset: str
    step: str
    store: ExecutionLogStore = field(compare=False)
    clock: Clock = field(default_factory=SystemClock, compare=False)
    monotonic_clock: MonotonicClock = field(default_factory=SystemMonotonicClock, compare=False)
    ids: ExecutionIdGenerator = field(default_factory=UuidExecutionIdGenerator, compare=False)
    layer: Layer = "none"
    schema_version: int = 1
    run_id: str | None = None
    step_id: str | None = None

    # ---------------------------------------------------------------------------------------------
    # Init / properties
    # ---------------------------------------------------------------------------------------------
    def __post_init__(self) -> None:
        if self.store is None:
            raise ValueError("execution log store is required")

        if self.ids is None:
            raise ValueError("execution ID generator is required")

        if self.clock is None:
            raise ValueError("execution UTC clock is required")

        if self.monotonic_clock is None:
            raise ValueError("execution monotonic clock is required")

        if not isinstance(self.dataset, str) or not self.dataset.strip():
            raise ValueError("dataset must not be blank")

        object.__setattr__(self, "dataset", self.dataset.strip())

        if not isinstance(self.step, str) or not self.step.strip():
            raise ValueError("step must not be blank")

        object.__setattr__(self, "step", self.step.strip())

        if self.run_id is None:
            prefix = self.layer if self.layer != "none" else "run"

            object.__setattr__(self, "run_id", self.ids.new_run_id(prefix))

        if self.step_id is None:
            object.__setattr__(self, "step_id", self.ids.new_step_id())

    def timer(self) -> ExecutionTimer:
        """Return a timer using the injected clock."""

        return ExecutionTimer(clock=self.monotonic_clock)

    # ---------------------------------------------------------------------------------------------
    # Step events
    # ---------------------------------------------------------------------------------------------
    def step_started(self, *, meta: ExecutionStepMeta | None = None) -> StepStartedEvent:
        """Write step_started execution receipt."""

        if self.run_id is None or self.step_id is None:
            raise RuntimeError("Execution log identifiers were not initialized")

        event = StepStartedEvent(
            ts=self.clock.now_utc(),
            schema_version=self.schema_version,
            dataset=self.dataset,
            layer=self.layer,
            step=self.step,
            run_id=self.run_id,
            step_id=self.step_id,
            meta=meta,
        )

        self.store.insert_execution_log(event)
        return event

    def step_finished(
        self,
        *,
        status: StepStatus,
        duration_ms: int,
        counts: ExecutionCounts,
        error: ExecutionError | None = None,
        meta: ExecutionStepMeta | None = None,
    ) -> StepFinishedEvent:
        """Write step_finished execution receipt."""

        if self.run_id is None or self.step_id is None:
            raise RuntimeError("Execution log identifiers were not initialized")

        event = StepFinishedEvent(
            ts=self.clock.now_utc(),
            schema_version=self.schema_version,
            dataset=self.dataset,
            layer=self.layer,
            step=self.step,
            run_id=self.run_id,
            step_id=self.step_id,
            status=status,
            duration_ms=duration_ms,
            counts=counts,
            error=error,
            meta=meta,
        )

        self.store.insert_execution_log(event)
        return event
