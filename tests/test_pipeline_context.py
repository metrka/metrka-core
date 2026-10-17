"""Tests for the composed pipeline context."""

from __future__ import annotations

from dataclasses import fields
from unittest.mock import Mock

from metrka_core.pipeline.context import PipelineContext


def test_pipeline_context_contains_only_composition_groups() -> None:
    assert {field.name for field in fields(PipelineContext)} == {
        "runtime",
        "workspace",
        "metadata",
        "acquisition",
        "bronze",
        "silver",
    }


def test_pipeline_context_projects_action_runtime() -> None:
    code_provenance = Mock()

    runtime = Mock()
    runtime.pipeline_run_id = "pipeline-test"
    runtime.code_provenance = code_provenance

    workspace = Mock()
    workspace.run_name = "example_workspace"

    context = PipelineContext(
        runtime=runtime,
        workspace=workspace,
        metadata=Mock(),
        bronze=Mock(),
        silver=Mock(),
        acquisition=Mock(),
    )

    action_runtime = context.as_action_runtime()

    assert action_runtime.pipeline_run_id == "pipeline-test"
    assert action_runtime.dataset_name == "example_workspace"
    assert action_runtime.code_provenance is code_provenance
