"""Typed execution events persisted by observability adapters"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from metrka_core.observability.execution_step_meta import ExecutionStepMeta

# ================================================================================
# Types
# ================================================================================
type Layer = Literal["none", "landing", "bronze", "silver"]
type StepStatus = Literal["success", "failed", "skipped", "blocked", "interrupted"]

_VALID_LAYERS = frozenset({"none", "landing", "bronze", "silver"})
_VALID_STEP_STATUSES = frozenset({"success", "failed", "skipped", "blocked", "interrupted"})


def _require_non_negative_integer(value: object, *, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _require_non_empty_string(value: object, *, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _validate_common_event_fields(
    *,
    ts: datetime,
    schema_version: int,
    dataset: str,
    layer: Layer,
    step: str,
    run_id: str,
    step_id: str,
) -> None:

    if not isinstance(ts, datetime):
        raise TypeError("ts must be a datetime")

    if ts.tzinfo is None or ts.utcoffset() != UTC.utcoffset(ts):
        raise ValueError("execution event timestamp must be in UTC")

    _require_non_negative_integer(schema_version, field_name="schema_version")

    if schema_version == 0:
        raise ValueError("schema_version must be greater than zero")

    _require_non_empty_string(dataset, field_name="dataset")
    _require_non_empty_string(step, field_name="step")
    _require_non_empty_string(run_id, field_name="run_id")
    _require_non_empty_string(step_id, field_name="step_id")

    if layer not in _VALID_LAYERS:
        raise ValueError(f"layer must be one of: {sorted(_VALID_LAYERS)}")


@dataclass(frozen=True, slots=True)
class ExecutionCounts:
    """Aggregate outcomes recorded for one finished execution step."""

    success: int
    failed: int
    skipped: int
    blocked: int

    def __post_init__(self) -> None:
        _require_non_negative_integer(self.success, field_name="success")
        _require_non_negative_integer(self.failed, field_name="failed")
        _require_non_negative_integer(self.skipped, field_name="skipped")
        _require_non_negative_integer(self.blocked, field_name="blocked")


@dataclass(frozen=True, slots=True)
class ExecutionError:
    """Structured failure information recorded for a finished step."""

    error_type: str
    message: str

    def __post_init__(self) -> None:
        _require_non_empty_string(self.error_type, field_name="error_type")

        if not isinstance(self.message, str):
            raise TypeError("message must be a string")


@dataclass(frozen=True, slots=True)
class StepStartedEvent:
    """A receipt proving that one execution step started."""

    ts: datetime
    schema_version: int
    dataset: str
    layer: Layer
    step: str
    run_id: str
    step_id: str
    meta: ExecutionStepMeta | None = None
    event_type: Literal["step_started"] = field(init=False, default="step_started")

    def __post_init__(self) -> None:
        _validate_common_event_fields(
            ts=self.ts,
            schema_version=self.schema_version,
            dataset=self.dataset,
            layer=self.layer,
            step=self.step,
            run_id=self.run_id,
            step_id=self.step_id,
        )

        if self.meta is not None and not isinstance(self.meta, ExecutionStepMeta):
            raise TypeError("meta must be ExecutionStepMeta or None")


@dataclass(frozen=True, slots=True)
class StepFinishedEvent:
    """A receipt proving that one execution step finished."""

    ts: datetime
    schema_version: int
    dataset: str
    layer: Layer
    step: str
    run_id: str
    step_id: str
    status: StepStatus
    duration_ms: int
    counts: ExecutionCounts
    error: ExecutionError | None = None
    meta: ExecutionStepMeta | None = None
    event_type: Literal["step_finished"] = field(init=False, default="step_finished")

    def __post_init__(self) -> None:
        _validate_common_event_fields(
            ts=self.ts,
            schema_version=self.schema_version,
            dataset=self.dataset,
            layer=self.layer,
            step=self.step,
            run_id=self.run_id,
            step_id=self.step_id,
        )

        if self.meta is not None and not isinstance(self.meta, ExecutionStepMeta):
            raise TypeError("meta must be ExecutionStepMeta or None")

        if self.status not in _VALID_STEP_STATUSES:
            raise ValueError(f"status must be one of: {sorted(_VALID_STEP_STATUSES)}")

        _require_non_negative_integer(self.duration_ms, field_name="duration_ms")

        if not isinstance(self.counts, ExecutionCounts):
            raise TypeError("counts must be ExecutionCounts")

        if self.error is not None and not isinstance(self.error, ExecutionError):
            raise TypeError("error must be ExecutionError or None")


type ExecutionEvent = StepStartedEvent | StepFinishedEvent
