"""Tests for typed action dependency binding."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import Mock, patch

import pytest

from metrka_core.pipeline.action_models import ActionDefinition, ActionOutcome
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.actions.silver import (
    SilverProcessActionDeps,
    SilverProcessOptions,
    process_silver_action,
)
from metrka_core.pipeline.context import PipelineContext
from metrka_core.pipeline.models import PipelineRunState
from metrka_core.pipeline.silver.process_models import (
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
    SilverProcessResult,
)

SILVER_ACTION = "metrka_core.pipeline.actions.silver"


@dataclass(frozen=True)
class SampleOptions:
    """Options used by the action binding test."""

    label: str


@dataclass(frozen=True)
class SampleDeps:
    """Dependencies used by the action binding test."""

    value: str


def test_resolved_action_projects_context_before_handler() -> None:
    runtime = Mock(spec=ActionRuntime)
    context = Mock(spec=PipelineContext)
    context.as_action_runtime.return_value = runtime

    state = PipelineRunState()

    expected_deps = SampleDeps(value="dependency-value")

    received: dict[str, object] = {}

    def parse_options(raw: object) -> SampleOptions:
        assert isinstance(raw, dict)

        return SampleOptions(label=str(raw["label"]))

    def resolve_dependencies(received_context: PipelineContext) -> SampleDeps:
        assert received_context is context

        return expected_deps

    def handler(
        *, runtime: ActionRuntime, deps: SampleDeps, state: PipelineRunState, options: SampleOptions
    ) -> ActionOutcome:
        received["runtime"] = runtime
        received["deps"] = deps
        received["state"] = state
        received["options"] = options

        return ActionOutcome(status="completed")

    definition = ActionDefinition[SampleOptions, SampleDeps](
        key="test.action",
        parse_options=parse_options,
        resolve_dependencies=resolve_dependencies,
        handler=handler,
    )

    resolved = definition.resolve({"label": "prepared"})

    outcome = resolved.execute(context=context, state=state)

    assert outcome.status == "completed"
    assert received["runtime"] is runtime
    assert received["deps"] is expected_deps
    assert received["state"] is state
    assert received["options"] == SampleOptions(label="prepared")


def test_silver_action_runs_without_pipeline_context() -> None:
    runtime = Mock(spec=ActionRuntime)
    silver_deps = Mock()

    engine_gate = Mock()
    engine_gate.allowed = True

    state = PipelineRunState()

    with (
        patch(f"{SILVER_ACTION}.evaluate_silver_engine_gate", return_value=engine_gate),
        patch(
            f"{SILVER_ACTION}.process_configured_silver",
            return_value=SilverProcessResult(finalized_dataset_ids=("example.dataset",)),
        ) as process,
    ):
        outcome = process_silver_action(
            runtime=runtime,
            deps=SilverProcessActionDeps(silver=silver_deps),
            state=state,
            options=SilverProcessOptions(target_dataset_id="example.dataset", force_rebuild=True),
        )

    assert outcome.status == "completed"
    assert outcome.metrics["finalized_count"] == 1
    assert outcome.metrics["skipped_count"] == 0
    assert outcome.metrics["warning_count"] == 0

    process.assert_called_once_with(
        runtime=runtime, deps=silver_deps, target_dataset_id="example.dataset", force_rebuild=True
    )


def test_silver_action_preserves_structured_processing_failure() -> None:
    runtime = Mock(spec=ActionRuntime)

    engine_gate = Mock()
    engine_gate.allowed = True

    failure = SilverDatasetFailure(
        dataset_id="example.dataset",
        stage=SilverFailureStage.FINALIZATION,
        error_code="SILVER_FINALIZATION_FAILED",
        message="publication decision failed",
        silver_build_id="silver-build-1",
    )
    state = PipelineRunState()

    with (
        patch(f"{SILVER_ACTION}.evaluate_silver_engine_gate", return_value=engine_gate),
        patch(
            f"{SILVER_ACTION}.process_configured_silver", side_effect=SilverProcessingError(failure)
        ),
        pytest.raises(SilverProcessingError) as raised,
    ):
        process_silver_action(
            runtime=runtime,
            deps=SilverProcessActionDeps(silver=Mock()),
            state=state,
            options=SilverProcessOptions(target_dataset_id="example.dataset"),
        )

    assert raised.value.failure is failure
    assert "silver.process" not in state.action_results
