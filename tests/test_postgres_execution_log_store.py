"""Tests for relational execution-event mapping."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from metrka_core.observability import postgres_stores
from metrka_core.observability.execution_events import (
    ExecutionCounts,
    ExecutionError,
    StepFinishedEvent,
    StepStartedEvent,
)
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.postgres_stores import PostgresExecutionLogStore

FIXED_TIME = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


def _store() -> tuple[PostgresExecutionLogStore, MagicMock]:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value

    return (PostgresExecutionLogStore(session, pipeline_run_id="pipeline-1"), cursor)


@pytest.fixture(autouse=True)
def plain_jsonb(monkeypatch: pytest.MonkeyPatch) -> None:
    """Expose JSONB payloads as plain Python values."""

    monkeypatch.setattr(postgres_stores, "to_jsonb", lambda value: value)


def test_started_event_maps_metadata_to_queryable_columns() -> None:
    store, cursor = _store()
    event = StepStartedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="bronze",
        step="ingest_and_stage",
        run_id="bronze-1",
        step_id="step-1",
        meta=ExecutionStepMeta(dataset_id="wi_dhs_adult_lead.county", dataset_file_id="file-123"),
    )

    store.insert_execution_log(event)

    parameters = cursor.execute.call_args.args[1]

    assert parameters[0] == FIXED_TIME
    assert parameters[3] == "wi_dhs_adult_lead.county"
    assert parameters[4] == "file-123"
    assert parameters[18] == "step_started"

    # Fields that only exist after the step has finished.
    assert parameters[19:25] == (None, None, None, None, None, None)

    assert parameters[40:43] == (None, None, None)
    assert parameters[43] == {
        "dataset_id": "wi_dhs_adult_lead.county",
        "dataset_file_id": "file-123",
    }
    assert parameters[44] == "pipeline-1"


def test_started_event_without_meta_is_persisted() -> None:
    store, cursor = _store()
    event = StepStartedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="bronze",
        step="ingest_and_stage",
        run_id="bronze-1",
        step_id="step-1",
    )

    store.insert_execution_log(event)

    parameters = cursor.execute.call_args.args[1]

    assert parameters[3] is None
    assert parameters[4] is None
    assert parameters[43] is None
    assert parameters[44] == "pipeline-1"


def test_finished_event_maps_counts_error_and_meta() -> None:
    store, cursor = _store()
    event = StepFinishedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="silver",
        step="build",
        run_id="silver-1",
        step_id="step-2",
        status="failed",
        duration_ms=250,
        counts=ExecutionCounts(success=1, failed=2, skipped=3, blocked=4),
        error=ExecutionError(error_type="RuntimeError", message="boom"),
        meta=ExecutionStepMeta(table_key="county", output_row_count=100),
    )

    store.insert_execution_log(event)

    parameters = cursor.execute.call_args.args[1]

    assert parameters[18] == "step_finished"
    assert parameters[19] == "failed"
    assert parameters[20] == 250
    assert parameters[21:25] == (1, 2, 3, 4)

    assert parameters[40] == "RuntimeError"
    assert parameters[41] == "boom"
    assert parameters[42] == {"type": "RuntimeError", "message": "boom"}
    assert parameters[43] == {"table_key": "county", "output_row_count": 100}
    assert parameters[44] == "pipeline-1"


def test_finished_event_without_error_persists_null_error_fields() -> None:
    store, cursor = _store()
    event = StepFinishedEvent(
        ts=FIXED_TIME,
        schema_version=1,
        dataset="adult-lead",
        layer="silver",
        step="build",
        run_id="silver-1",
        step_id="step-2",
        status="success",
        duration_ms=25,
        counts=ExecutionCounts(success=1, failed=0, skipped=0, blocked=0),
    )

    store.insert_execution_log(event)

    parameters = cursor.execute.call_args.args[1]

    assert parameters[40:43] == (None, None, None)
