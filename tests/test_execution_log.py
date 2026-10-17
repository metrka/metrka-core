from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, call

import pytest

from metrka_core.observability.execution_events import (
    ExecutionCounts,
    StepFinishedEvent,
    StepStartedEvent,
)
from metrka_core.observability.execution_log import ExecutionLog, ExecutionTimer
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.pipeline.runtime_services import UuidExecutionIdGenerator


class FixedExecutionIds:
    """Return predictable identifiers for tests."""

    def new_run_id(self, prefix: str) -> str:
        return f"{prefix}_fixed_run"

    def new_step_id(self, prefix: str = "step") -> str:
        return f"{prefix}_fixed_step"


FROZEN_TIME = datetime(2026, 8, 14, 12, 30, 0, 123456, tzinfo=UTC)


class FrozenClock:
    """Return one deterministic UTC timestamp."""

    def now_utc(self) -> datetime:
        return FROZEN_TIME


class SequenceMonotonicClock:
    """Return configured monotonic values."""

    def __init__(self, *values: float) -> None:
        self._values = iter(values)

    def monotonic(self) -> float:
        return next(self._values)


def test_execution_log_uses_injected_identifiers() -> None:
    log = ExecutionLog(
        dataset="adult-lead",
        step="prepare",
        layer="silver",
        store=MagicMock(),
        ids=FixedExecutionIds(),
    )

    assert log.run_id == "silver_fixed_run"
    assert log.step_id == "step_fixed_step"


def test_identifiers_have_requested_prefixes() -> None:
    ids = UuidExecutionIdGenerator()

    assert ids.new_run_id("bronze").startswith("bronze_")
    assert ids.new_step_id().startswith("step_")
    assert ids.new_step_id("load").startswith("load_")


def test_timer_uses_monotonic_clock() -> None:
    clock = SequenceMonotonicClock(100.0, 100.125)

    timer = ExecutionTimer(clock=clock)

    assert timer.ms() == 125


@pytest.mark.parametrize("layer", ["", "Bronze", "gold", "platinum"])
def test_invalid_layer_is_not_inserted(layer: str) -> None:
    store = MagicMock()
    log = ExecutionLog(
        dataset="adult-lead",
        step="extract_archive",
        layer=layer,  # type: ignore[arg-type]
        store=store,
    )

    with pytest.raises(ValueError, match="layer must be one of"):
        log.step_started()

    store.insert_execution_log.assert_not_called()


def test_execution_log_requires_dataset_step_and_store() -> None:
    with pytest.raises(ValueError, match="dataset must not be blank"):
        ExecutionLog(dataset=" ", step="load", store=MagicMock())

    with pytest.raises(ValueError, match="step must not be blank"):
        ExecutionLog(dataset="adult-lead", step=" ", store=MagicMock())

    with pytest.raises(ValueError, match="store is required"):
        ExecutionLog(
            dataset="adult-lead",
            step="load",
            store=None,  # type: ignore[arg-type]
        )


def test_started_event_uses_explicit_dataset_identity() -> None:
    store = MagicMock()
    meta = ExecutionStepMeta(extra={"source": "landing"})
    log = ExecutionLog(
        dataset="adult-lead",
        step="extract_archive",
        layer="bronze",
        run_id="run-1",
        step_id="step-1",
        store=store,
        clock=FrozenClock(),
    )

    event = log.step_started(meta=meta)

    assert isinstance(event, StepStartedEvent)
    assert event.ts == FROZEN_TIME
    assert event.dataset == "adult-lead"
    assert event.run_id == "run-1"
    assert event.step_id == "step-1"
    assert event.meta == meta
    store.insert_execution_log.assert_called_once_with(event)


def test_step_events_are_written_as_typed_events() -> None:
    store = MagicMock()
    log = ExecutionLog(
        dataset="adult-lead",
        step="prepare",
        layer="silver",
        run_id="run-1",
        step_id="step-1",
        store=store,
        clock=FrozenClock(),
    )

    started = log.step_started(meta=ExecutionStepMeta(table_key="county"))
    finished = log.step_finished(
        status="success",
        duration_ms=15,
        counts=ExecutionCounts(success=1, failed=0, skipped=0, blocked=0),
    )

    assert isinstance(started, StepStartedEvent)
    assert isinstance(finished, StepFinishedEvent)
    assert started.event_type == "step_started"
    assert finished.event_type == "step_finished"

    assert store.insert_execution_log.call_args_list == [call(started), call(finished)]


class NaiveClock:
    """Return an invalid timestamp without timezone information."""

    def now_utc(self) -> datetime:
        return datetime(2026, 8, 30, 12, 0)


def test_invalid_event_is_never_inserted() -> None:
    store = MagicMock()
    log = ExecutionLog(dataset="adult-lead", step="prepare", store=store, clock=NaiveClock())

    with pytest.raises(ValueError, match="timestamp must be in UTC"):
        log.step_started()

    store.insert_execution_log.assert_not_called()
