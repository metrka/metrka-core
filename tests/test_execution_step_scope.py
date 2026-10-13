from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from metrka_core.observability.execution_events import ExecutionCounts, ExecutionError
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import StepContext, run_step


class FixedExecutionIds:
    """Return predictable execution identifiers."""

    def new_run_id(self, prefix: str) -> str:
        return f"{prefix}_fixed_run"

    def new_step_id(self, prefix: str = "step") -> str:
        return f"{prefix}_fixed_step"


class FrozenClock:
    """Return deterministic UTC time."""

    def now_utc(self) -> datetime:
        return datetime(2026, 8, 14, 12, 30, tzinfo=UTC)


class FixedMonotonicClock:
    """Return deterministic monotonic time."""

    def monotonic(self) -> float:
        return 100.0


def _execution_mock(mocker: MockerFixture, *, duration_ms: int = 25) -> MagicMock:
    execution = MagicMock()
    execution.timer.return_value.ms.return_value = duration_ms
    mocker.patch(
        "metrka_core.observability.execution_step_scope.ExecutionLog", return_value=execution
    )
    return execution


def test_step_context_aggregates_counters() -> None:
    context = StepContext(execution=MagicMock())
    context.count_success(2)
    context.count_failed(1)
    context.count_skipped(3)
    context.count_blocked(4)

    assert context.execution_counts() == ExecutionCounts(success=2, failed=1, skipped=3, blocked=4)


@pytest.mark.parametrize("value", [-1, True, 1.5, "1"])
def test_step_context_rejects_invalid_increment(value: object) -> None:
    context = StepContext(execution=MagicMock())

    with pytest.raises(ValueError, match="non-negative int"):
        context.count_success(value)  # type: ignore[arg-type]


def test_run_step_logs_success_with_narrow_dependencies(mocker: MockerFixture) -> None:
    execution = MagicMock()
    execution.timer.return_value.ms.return_value = 25
    log_class = mocker.patch(
        "metrka_core.observability.execution_step_scope.ExecutionLog", return_value=execution
    )
    store = MagicMock()
    execution_ids = FixedExecutionIds()
    clock = FrozenClock()
    monotonic_clock = FixedMonotonicClock()

    with run_step(
        dataset="adult-lead",
        step="prepare",
        layer="silver",
        execution_log_store=store,
        execution_ids=execution_ids,
        clock=clock,
        monotonic_clock=monotonic_clock,
        run_id="run-1",
        start_meta=ExecutionStepMeta(table_key="county"),
    ) as context:
        context.count_success(2)
        context.set_finish_meta(ExecutionStepMeta(output_row_count=100))

    log_class.assert_called_once_with(
        dataset="adult-lead",
        step="prepare",
        layer="silver",
        run_id="run-1",
        store=store,
        ids=execution_ids,
        clock=clock,
        monotonic_clock=monotonic_clock,
    )
    execution.step_started.assert_called_once_with(meta=ExecutionStepMeta(table_key="county"))
    execution.step_finished.assert_called_once_with(
        status="success",
        duration_ms=25,
        counts=ExecutionCounts(success=2, failed=0, skipped=0, blocked=0),
        error=None,
        meta=ExecutionStepMeta(table_key="county", output_row_count=100),
    )


def test_run_step_logs_failure_and_reraises(mocker: MockerFixture) -> None:
    execution = _execution_mock(mocker, duration_ms=30)

    with (
        pytest.raises(RuntimeError, match="boom"),
        run_step(
            dataset="adult-lead", step="prepare", layer="silver", execution_log_store=MagicMock()
        ) as context,
    ):
        context.count_failed()
        raise RuntimeError("boom")

    execution.step_finished.assert_called_once_with(
        status="failed",
        duration_ms=30,
        counts=ExecutionCounts(success=0, failed=1, skipped=0, blocked=0),
        error=ExecutionError(error_type="RuntimeError", message="boom"),
        meta=None,
    )


def test_run_step_records_keyboard_interrupt_and_reraises(mocker: MockerFixture) -> None:
    execution = _execution_mock(mocker)

    with (
        pytest.raises(KeyboardInterrupt),
        run_step(
            dataset="adult-lead", step="build", layer="silver", execution_log_store=MagicMock()
        ),
    ):
        raise KeyboardInterrupt

    execution.step_finished.assert_called_once_with(
        status="interrupted",
        duration_ms=25,
        counts=ExecutionCounts(success=0, failed=0, skipped=0, blocked=0),
        error=ExecutionError(error_type="KeyboardInterrupt", message=""),
        meta=None,
    )


def test_run_step_records_system_exit_and_reraises(mocker: MockerFixture) -> None:
    execution = _execution_mock(mocker)

    with (
        pytest.raises(SystemExit) as raised,
        run_step(
            dataset="adult-lead", step="build", layer="silver", execution_log_store=MagicMock()
        ),
    ):
        raise SystemExit(130)

    assert raised.value.code == 130
    execution.step_finished.assert_called_once_with(
        status="interrupted",
        duration_ms=25,
        counts=ExecutionCounts(success=0, failed=0, skipped=0, blocked=0),
        error=ExecutionError(error_type="SystemExit", message="130"),
        meta=None,
    )


def test_run_step_preserves_primary_error_when_finish_logging_fails(mocker: MockerFixture) -> None:
    execution = _execution_mock(mocker)
    execution.step_finished.side_effect = ConnectionError("metadata database unavailable")

    with (
        pytest.raises(ValueError, match="invalid source row"),
        run_step(
            dataset="adult-lead", step="build", layer="silver", execution_log_store=MagicMock()
        ),
    ):
        raise ValueError("invalid source row")


def test_run_step_propagates_finish_logging_failure_after_success(mocker: MockerFixture) -> None:
    execution = _execution_mock(mocker)
    execution.step_finished.side_effect = ConnectionError("metadata database unavailable")

    with (
        pytest.raises(ConnectionError, match="metadata database unavailable"),
        run_step(
            dataset="adult-lead", step="build", layer="silver", execution_log_store=MagicMock()
        ),
    ):
        pass
