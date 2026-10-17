from __future__ import annotations

from dataclasses import fields
from inspect import signature

from metrka_core.observability.execution_log import ExecutionLog
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.bootstrap import open_pipeline_context
from metrka_core.pipeline.composition.runtime import RuntimeComposition, build_runtime_composition
from metrka_core.pipeline.silver.candidate_processing import SilverCandidatePreparationRequest
from metrka_core.pipeline.silver.silver_builder import build_silver_table
from metrka_core.transform.ops.casting import cast_columns
from metrka_core.transform.ops.dates import parse_dates
from metrka_core.transform.ops.text import convert_case, normalize_values
from metrka_core.transform.schema import apply_transformation


def test_pipeline_boundary_does_not_offer_lenient_execution() -> None:
    assert "strict" not in signature(open_pipeline_context).parameters
    assert "strict" not in signature(build_runtime_composition).parameters
    assert "strict" not in {field.name for field in fields(RuntimeComposition)}
    assert "strict" not in {field.name for field in fields(ActionRuntime)}


def test_silver_boundary_does_not_offer_lenient_execution() -> None:
    assert "strict" not in {field.name for field in fields(SilverCandidatePreparationRequest)}
    assert "strict" not in signature(build_silver_table).parameters


def test_audit_and_transformation_boundaries_always_fail_closed() -> None:
    assert "strict" not in {field.name for field in fields(ExecutionLog)}

    fail_closed_functions = (
        apply_transformation,
        normalize_values,
        cast_columns,
        parse_dates,
        convert_case,
    )

    for function in fail_closed_functions:
        assert "strict" not in signature(function).parameters
