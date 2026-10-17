"""Tests for typed execution events."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from metrka_core.observability.execution_events import (
    ExecutionCounts,
    ExecutionError,
    StepFinishedEvent,
    StepStartedEvent,
    StepStatus,
)
from metrka_core.observability.execution_step_meta import ExecutionStepMeta

FIXED_TIME = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


def _started_event() -> StepStartedEvent:
    return StepStartedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="bronze",
        step="ingest",
        run_id="bronze-1",
        step_id="step-1",
    )


def _finished_event() -> StepFinishedEvent:
    return StepFinishedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="bronze",
        step="ingest",
        run_id="bronze-1",
        step_id="step-1",
        status="success",
        duration_ms=25,
        counts=ExecutionCounts(success=1, failed=0, skipped=0, blocked=0),
    )


def test_step_started_event_is_typed() -> None:
    meta = ExecutionStepMeta(extra={"files": 2})

    event = replace(_started_event(), meta=meta)

    assert event.event_type == "step_started"
    assert event.meta == meta


@pytest.mark.parametrize("status", ["success", "failed", "skipped", "blocked", "interrupted"])
def test_step_finished_accepts_supported_statuses(status: StepStatus) -> None:
    event = replace(_finished_event(), status=status)

    assert event.status == status
    assert event.event_type == "step_finished"


def test_event_timestamp_must_be_utc() -> None:
    non_utc = datetime(2026, 8, 30, 12, 0, tzinfo=timezone(timedelta(hours=2)))

    with pytest.raises(ValueError, match="timestamp must be in UTC"):
        replace(_started_event(), ts=non_utc)


@pytest.mark.parametrize("schema_version", [0, -1, True])
def test_schema_version_must_be_positive_integer(schema_version: object) -> None:
    with pytest.raises(ValueError):
        replace(_started_event(), schema_version=cast(Any, schema_version))


@pytest.mark.parametrize("field_name", ["dataset", "step", "run_id", "step_id"])
def test_common_string_fields_must_not_be_blank(field_name: str) -> None:
    with pytest.raises(ValueError, match=f"{field_name} must be"):
        replace(_started_event(), **{field_name: " "})


def test_layer_must_be_supported() -> None:
    with pytest.raises(ValueError, match="layer must be one of"):
        replace(_started_event(), layer=cast(Any, "gold"))


def test_finished_status_must_be_supported() -> None:
    with pytest.raises(ValueError, match="status must be one of"):
        replace(_finished_event(), status=cast(Any, "mystery"))


@pytest.mark.parametrize("duration_ms", [-1, True, "25"])
def test_duration_must_be_non_negative_integer(duration_ms: object) -> None:
    with pytest.raises(ValueError, match="duration_ms must be"):
        replace(_finished_event(), duration_ms=cast(Any, duration_ms))


@pytest.mark.parametrize(
    ("field_name", "value"), [("success", -1), ("failed", True), ("skipped", "1"), ("blocked", -1)]
)
def test_counts_must_be_non_negative_integers(field_name: str, value: object) -> None:
    values: dict[str, Any] = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0}
    values[field_name] = value

    with pytest.raises(ValueError):
        ExecutionCounts(**values)


def test_finished_event_requires_execution_counts() -> None:
    with pytest.raises(TypeError, match="counts must be ExecutionCounts"):
        replace(_finished_event(), counts=cast(Any, {"success": 1}))


def test_execution_error_requires_non_empty_type() -> None:
    with pytest.raises(ValueError, match="error_type must be"):
        ExecutionError(error_type=" ", message="boom")


def test_execution_error_message_must_be_string() -> None:
    with pytest.raises(TypeError, match="message must be a string"):
        ExecutionError(error_type="RuntimeError", message=cast(Any, 123))


def test_finished_event_rejects_untyped_error() -> None:
    with pytest.raises(TypeError, match="error must be ExecutionError"):
        replace(_finished_event(), error=cast(Any, {"type": "RuntimeError"}))


@pytest.mark.parametrize("event", [_started_event(), _finished_event()])
def test_event_rejects_untyped_meta(event: StepStartedEvent | StepFinishedEvent) -> None:
    with pytest.raises(TypeError, match="meta must be ExecutionStepMeta"):
        replace(event, meta=cast(Any, {"table_key": "county"}))
