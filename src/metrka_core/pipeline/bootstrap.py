"""Bootstrap and lifecycle management for dataset pipelines."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.metadata.schema_compatibility import require_metadata_schema_current
from metrka_core.pipeline.composition.acquisition import build_acquisition_composition
from metrka_core.pipeline.composition.bronze import build_bronze_composition
from metrka_core.pipeline.composition.lifecycle import pipeline_run
from metrka_core.pipeline.composition.metadata import build_metadata_composition
from metrka_core.pipeline.composition.runtime import build_runtime_composition
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.composition.silver import build_silver_composition
from metrka_core.pipeline.composition.workspace import build_workspace_composition
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    build_workspace_location_resolver,
)
from metrka_core.pipeline.config import RuntimeEnvironment, resolve_runtime_environment
from metrka_core.pipeline.context import PipelineContext
from metrka_core.pipeline.database_config import resolve_metadata_conninfo
from metrka_core.pipeline.silver.engine_models import SilverEnginePolicy
from metrka_core.pipeline.silver.engine_policy import resolve_silver_engine_policy


@contextmanager
def open_pipeline_context(
    *,
    workspace_name: str,
    config_name: str = "main.yaml",
    runtime_environment: RuntimeEnvironment | None = None,
    silver_engine_policy: SilverEnginePolicy | None = None,
    services: RuntimeServices | None = None,
    workspaces_config_path: str | Path | None = None,
    workspace_location_resolver: WorkspaceLocationResolver | None = None,
    metadata_conninfo: str | None = None,
    metadata_config_path: str | Path | None = None,
) -> Iterator[PipelineContext]:
    """Create, expose and close one pipeline execution context."""

    resolved_services = services if services is not None else RuntimeServices()

    resolved_runtime_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )

    configured_silver_engine_policy: SilverEnginePolicy | str | None = (
        silver_engine_policy
        if silver_engine_policy is not None
        else os.environ.get("METRKA_SILVER_ENGINE_POLICY")
    )

    resolved_silver_engine_policy = resolve_silver_engine_policy(
        runtime_environment=resolved_runtime_environment,
        configured_policy=configured_silver_engine_policy,
    )

    if workspace_location_resolver is not None and workspaces_config_path is not None:
        raise ValueError(
            "Pass either workspace_location_resolver or workspaces_config_path, not both"
        )

    resolved_workspace_locations = (
        workspace_location_resolver
        if workspace_location_resolver is not None
        else build_workspace_location_resolver(
            explicit_config_path=workspaces_config_path,
            environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
            runtime_environment=resolved_runtime_environment,
        )
    )

    workspace = build_workspace_composition(
        workspace_name=workspace_name,
        config_name=config_name,
        workspace_locations=resolved_workspace_locations,
        clock=resolved_services.clock,
        source_capture_ids=resolved_services.source_capture_ids,
    )

    runtime = build_runtime_composition(
        definition_path=workspace.layout.definition_root,
        runtime_environment=resolved_runtime_environment,
        silver_engine_policy=resolved_silver_engine_policy,
        run_ids=resolved_services.pipeline_run_ids,
    )

    postgres_conninfo = resolve_metadata_conninfo(
        conninfo=metadata_conninfo, config_path=metadata_config_path
    )

    with PostgresSession(conninfo=postgres_conninfo) as postgres_session:
        require_metadata_schema_current(postgres_session)

        metadata = build_metadata_composition(
            session=postgres_session,
            pipeline_run_id=runtime.pipeline_run_id,
            clock=resolved_services.clock,
            source_schema_ids=resolved_services.source_schema_snapshot_ids,
        )

        acquisition = build_acquisition_composition(workspace=workspace, metadata=metadata)

        bronze = build_bronze_composition(
            workspace=workspace,
            metadata=metadata,
            clock=resolved_services.clock,
            dataset_file_ids=resolved_services.dataset_file_ids,
            bronze_run_ids=resolved_services.bronze_run_ids,
        )

        silver = build_silver_composition(
            session=postgres_session,
            workspace=workspace,
            runtime=runtime,
            metadata=metadata,
            clock=resolved_services.clock,
            build_ids=resolved_services.silver_build_ids,
            candidate_ids=resolved_services.publication_candidate_ids,
            transformation_impact_ids=resolved_services.transformation_impact_ids,
        )

        context = PipelineContext(
            workspace=workspace,
            runtime=runtime,
            metadata=metadata,
            acquisition=acquisition,
            bronze=bronze,
            silver=silver,
        )

        with pipeline_run(
            context=context,
            runtime=runtime,
            pipeline_runs=metadata.pipeline_runs,
            clock=resolved_services.clock,
            run_name=workspace.run_name,
            config_name=config_name,
        ):
            yield context
