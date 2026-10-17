"""High-level application service for running one configured pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.pipeline.bootstrap import open_pipeline_context
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.default_registry import create_core_registry
from metrka_core.pipeline.models import PipelineRunState
from metrka_core.pipeline.registry import PipelineRegistry
from metrka_core.pipeline.runner import execute_configured_pipeline

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineBootstrapOptions:
    """Infrastructure settings used while composing one pipeline run."""

    config_name: str = "main.yaml"
    runtime_environment: RuntimeEnvironment | None = None
    services: RuntimeServices | None = None
    workspaces_config_path: str | Path | None = None
    workspace_location_resolver: WorkspaceLocationResolver | None = None
    metadata_conninfo: str | None = field(default=None, repr=False)
    metadata_config_path: str | Path | None = None

    def __post_init__(self) -> None:
        if not self.config_name.strip():
            raise ValueError("config_name must not be empty")

        if self.workspace_location_resolver is not None and self.workspaces_config_path is not None:
            raise ValueError(
                "Pass either workspace_location_resolver or workspaces_config_path, not both"
            )


@dataclass(frozen=True, slots=True)
class PipelineRunResult:
    """Identity and values produced by one completed pipeline run."""

    pipeline_run_id: str
    state: PipelineRunState


def run_pipeline(
    workspace_name: str,
    *,
    target_date: str | None = None,
    target_dataset_id: str | None = None,
    source_capture_id: str | None = None,
    force_rebuild: bool = False,
    registry: PipelineRegistry | None = None,
    bootstrap: PipelineBootstrapOptions | None = None,
) -> PipelineRunResult:
    """Run acquisition and every configured action for one workspace."""

    if not isinstance(workspace_name, str) or not workspace_name.strip():
        raise ValueError("workspace_name must be a non-empty string")

    if source_capture_id is not None and target_date is None:
        raise ValueError("source_capture_id requires target_date")

    if force_rebuild and target_dataset_id is None:
        raise ValueError("force_rebuild requires target_dataset_id")

    resolved_workspace_name = workspace_name.strip()
    resolved_registry = registry if registry is not None else create_core_registry()
    resolved_bootstrap = bootstrap if bootstrap is not None else PipelineBootstrapOptions()

    silver_options: dict[str, Any] = {}

    if target_dataset_id is not None:
        silver_options["target_dataset_id"] = target_dataset_id

    if force_rebuild:
        silver_options["force_rebuild"] = True

    action_option_overrides = {"silver.process": silver_options} if silver_options else None

    logger.info("Starting pipeline workspace=%s", resolved_workspace_name)

    with open_pipeline_context(
        workspace_name=resolved_workspace_name,
        config_name=resolved_bootstrap.config_name,
        runtime_environment=resolved_bootstrap.runtime_environment,
        services=resolved_bootstrap.services,
        workspaces_config_path=resolved_bootstrap.workspaces_config_path,
        workspace_location_resolver=resolved_bootstrap.workspace_location_resolver,
        metadata_conninfo=resolved_bootstrap.metadata_conninfo,
        metadata_config_path=resolved_bootstrap.metadata_config_path,
    ) as context:
        pipeline_run_id = context.runtime.pipeline_run_id
        state = execute_configured_pipeline(
            context=context,
            registry=resolved_registry,
            target_date=target_date,
            source_capture_id=source_capture_id,
            action_option_overrides=action_option_overrides,
            run_without_landed_assets=force_rebuild,
        )

    logger.info(
        "Pipeline completed workspace=%s run_id=%s", resolved_workspace_name, pipeline_run_id
    )

    return PipelineRunResult(pipeline_run_id=pipeline_run_id, state=state)
