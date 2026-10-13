"""Behavioural tests for the high-level pipeline application service."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import metrka_core.pipeline.run as pipeline_run_module
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.models import PipelineRunState
from metrka_core.pipeline.registry import PipelineRegistry
from metrka_core.pipeline.run import PipelineBootstrapOptions, PipelineRunResult


@dataclass
class RunHarness:
    context: object
    registry: PipelineRegistry
    state: PipelineRunState
    open_calls: list[dict[str, object]]
    execute_calls: list[dict[str, object]]
    events: list[str]


def _install_run_harness(
    monkeypatch: pytest.MonkeyPatch, *, execution_error: BaseException | None = None
) -> RunHarness:
    context = SimpleNamespace(runtime=SimpleNamespace(pipeline_run_id="pipeline-test"))
    registry = PipelineRegistry()
    state = PipelineRunState()
    open_calls: list[dict[str, object]] = []
    execute_calls: list[dict[str, object]] = []
    events: list[str] = []

    @contextmanager
    def fake_open_pipeline_context(**kwargs: object) -> Iterator[object]:
        open_calls.append(kwargs)
        events.append("context_entered")

        try:
            yield context
        finally:
            events.append("context_closed")

    def fake_execute_configured_pipeline(**kwargs: object) -> PipelineRunState:
        execute_calls.append(kwargs)
        events.append("pipeline_executed")

        if execution_error is not None:
            raise execution_error

        return state

    monkeypatch.setattr(pipeline_run_module, "open_pipeline_context", fake_open_pipeline_context)
    monkeypatch.setattr(
        pipeline_run_module, "execute_configured_pipeline", fake_execute_configured_pipeline
    )
    monkeypatch.setattr(pipeline_run_module, "create_core_registry", lambda: registry)

    return RunHarness(
        context=context,
        registry=registry,
        state=state,
        open_calls=open_calls,
        execute_calls=execute_calls,
        events=events,
    )


def test_run_pipeline_executes_the_default_registry_and_returns_run_identity(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = _install_run_harness(monkeypatch)
    caplog.set_level(logging.INFO)

    result = pipeline_run_module.run_pipeline("  example_workspace  ")

    assert result == PipelineRunResult(pipeline_run_id="pipeline-test", state=harness.state)
    assert harness.events == ["context_entered", "pipeline_executed", "context_closed"]
    assert harness.open_calls == [
        {
            "workspace_name": "example_workspace",
            "config_name": "main.yaml",
            "runtime_environment": None,
            "services": None,
            "workspaces_config_path": None,
            "workspace_location_resolver": None,
            "metadata_conninfo": None,
            "metadata_config_path": None,
        }
    ]
    assert harness.execute_calls == [
        {
            "context": harness.context,
            "registry": harness.registry,
            "target_date": None,
            "source_capture_id": None,
            "action_option_overrides": None,
            "run_without_landed_assets": False,
        }
    ]
    assert "Starting pipeline workspace=example_workspace" in caplog.messages
    assert "Pipeline completed workspace=example_workspace run_id=pipeline-test" in caplog.messages


def test_run_pipeline_forwards_runtime_and_bootstrap_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _install_run_harness(monkeypatch)
    custom_registry = PipelineRegistry()
    workspaces_config_path = Path("C:/metrka/workspaces.local.yaml")
    metadata_config_path = Path("C:/metrka/metadata.yaml")
    bootstrap = PipelineBootstrapOptions(
        config_name="scheduled.yaml",
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
        workspaces_config_path=workspaces_config_path,
        metadata_conninfo="postgresql://runtime",
        metadata_config_path=metadata_config_path,
    )

    result = pipeline_run_module.run_pipeline(
        "example_workspace",
        target_date="2026-08-17",
        target_dataset_id="example_workspace.county",
        source_capture_id="capture-test",
        force_rebuild=True,
        registry=custom_registry,
        bootstrap=bootstrap,
    )

    assert result.pipeline_run_id == "pipeline-test"
    assert harness.open_calls == [
        {
            "workspace_name": "example_workspace",
            "config_name": "scheduled.yaml",
            "runtime_environment": RuntimeEnvironment.DEVELOPMENT,
            "services": None,
            "workspaces_config_path": workspaces_config_path,
            "workspace_location_resolver": None,
            "metadata_conninfo": "postgresql://runtime",
            "metadata_config_path": metadata_config_path,
        }
    ]
    assert harness.execute_calls == [
        {
            "context": harness.context,
            "registry": custom_registry,
            "target_date": "2026-08-17",
            "source_capture_id": "capture-test",
            "action_option_overrides": {
                "silver.process": {
                    "target_dataset_id": "example_workspace.county",
                    "force_rebuild": True,
                }
            },
            "run_without_landed_assets": True,
        }
    ]


@pytest.mark.parametrize(
    ("workspace_name", "kwargs", "message"),
    [
        ("", {}, "workspace_name must be a non-empty string"),
        (
            "example_workspace",
            {"source_capture_id": "capture-test"},
            "source_capture_id requires target_date",
        ),
        ("example_workspace", {"force_rebuild": True}, "force_rebuild requires target_dataset_id"),
    ],
)
def test_run_pipeline_rejects_invalid_requests_before_composition(
    workspace_name: str, kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        pipeline_run_module.run_pipeline(workspace_name, **kwargs)


def test_pipeline_bootstrap_options_reject_ambiguous_workspace_resolution() -> None:
    resolver = SimpleNamespace(resolve=lambda _workspace_name: Path("C:/datasets/example"))

    with pytest.raises(
        ValueError, match="either workspace_location_resolver or workspaces_config_path"
    ):
        PipelineBootstrapOptions(
            workspaces_config_path=Path("C:/metrka/workspaces.local.yaml"),
            workspace_location_resolver=resolver,
        )


def test_pipeline_bootstrap_options_do_not_expose_conninfo_in_repr() -> None:
    options = PipelineBootstrapOptions(
        metadata_conninfo="postgresql://pipeline-user:secret@localhost/metrka"
    )

    assert "secret" not in repr(options)


def test_run_pipeline_preserves_execution_errors_and_closes_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_error = RuntimeError("table build failed")
    harness = _install_run_harness(monkeypatch, execution_error=expected_error)

    with pytest.raises(RuntimeError, match="table build failed") as captured:
        pipeline_run_module.run_pipeline("example_workspace")

    assert captured.value is expected_error
    assert harness.events == ["context_entered", "pipeline_executed", "context_closed"]
