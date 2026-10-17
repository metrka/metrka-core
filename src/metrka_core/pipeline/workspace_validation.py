"""Read-only validation for one configured workspace."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from metrka_core.catalog.glossary import load_glossary, validate_contract_terms
from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.source_config import load_source_config, require_dataset_folder_streams
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    build_workspace_location_resolver,
)
from metrka_core.pipeline.config import (
    RuntimeEnvironment,
    parse_quality_settings,
    resolve_runtime_environment,
)
from metrka_core.pipeline.default_registry import create_core_registry
from metrka_core.pipeline.models import parse_pipeline_spec
from metrka_core.pipeline.registry import PipelineRegistry
from metrka_core.pipeline.silver.dataset_links import validate_dataset_links
from metrka_core.pipeline.silver.task_factory import load_silver_contracts
from metrka_core.quality.config import load_quality_config, validate_quality_config
from metrka_core.storage.config_store import LocalConfigStore
from metrka_core.storage.workspace_layout import WorkspaceLayout


@dataclass(frozen=True, slots=True)
class WorkspaceValidationResult:
    """Validated static configuration resolved for one workspace."""

    workspace_name: str
    workspace_root: Path | None
    definition_root: Path
    data_root: Path
    config_path: Path
    quality_config_path: Path
    extractor: str
    stream_names: tuple[str, ...]
    pipeline_actions: tuple[str, ...]
    quality_check_count: int
    silver_contract_paths: tuple[Path, ...]
    # Published Silver table names, shown so a table copied from another dataset stands out.
    silver_table_names: tuple[str, ...] = ()
    glossary_terms: tuple[str, ...] = ()

    @property
    def stream_count(self) -> int:
        return len(self.stream_names)

    @property
    def action_count(self) -> int:
        return len(self.pipeline_actions)

    @property
    def silver_contract_count(self) -> int:
        return len(self.silver_contract_paths)


def validate_workspace(
    workspace_name: str,
    *,
    config_name: str = "main.yaml",
    runtime_environment: RuntimeEnvironment | None = None,
    workspaces_config_path: str | Path | None = None,
    workspace_location_resolver: WorkspaceLocationResolver | None = None,
    registry: PipelineRegistry | None = None,
) -> WorkspaceValidationResult:
    """Validate static workspace configuration without executing or writing runtime state."""

    if not isinstance(workspace_name, str) or not workspace_name.strip():
        raise ValueError("workspace_name must be a non-empty string")

    if workspace_location_resolver is not None and workspaces_config_path is not None:
        raise ValueError(
            "Pass either workspace_location_resolver or workspaces_config_path, not both"
        )

    resolved_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )
    resolved_locations = (
        workspace_location_resolver
        if workspace_location_resolver is not None
        else build_workspace_location_resolver(
            explicit_config_path=workspaces_config_path,
            environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
            runtime_environment=resolved_environment,
        )
    )
    resolved_registry = registry if registry is not None else create_core_registry()

    normalized_workspace_name = workspace_name.strip()
    location = resolved_locations.resolve(normalized_workspace_name)

    if location.name != normalized_workspace_name:
        raise ValueError(
            "WorkspaceLocationResolver returned a location for a different workspace: "
            f"{location.name!r}"
        )

    layout = WorkspaceLayout(location=location)
    config_store = LocalConfigStore(
        workspace_root=layout.definition_root, config_root=layout.conf_dir
    )
    config_path = config_store.path(name=config_name)
    source_config = load_source_config(config_path, expected_ws_name=location.workspace_name)

    if location.dataset_name is not None:
        require_dataset_folder_streams(source_config, dataset_name=location.dataset_name)

    pipeline = parse_pipeline_spec(source_config.pipeline)
    resolved_registry.get_extractor(pipeline.acquisition.extractor)

    for step in pipeline.steps:
        resolved_registry.resolve_action(step.action, step.options)

    quality_settings = parse_quality_settings(source_config.pipeline.get("quality"))
    quality_config_path = config_store.path(name=quality_settings.config)
    quality_config = load_quality_config(quality_config_path)
    contracts = load_silver_contracts(source_config=source_config, config_store=config_store)
    validate_quality_config(quality_config, contracts.values())
    validate_dataset_links(
        source_config=source_config, config_store=config_store, contracts=contracts
    )
    glossary_terms = validate_contract_terms(load_glossary(location.glossary_path), contracts)

    return WorkspaceValidationResult(
        workspace_name=normalized_workspace_name,
        workspace_root=location.workspace_root,
        definition_root=layout.definition_root,
        data_root=layout.data_root,
        config_path=config_path,
        quality_config_path=quality_config_path,
        extractor=pipeline.acquisition.extractor,
        stream_names=tuple(source_config.streams),
        pipeline_actions=tuple(step.action for step in pipeline.steps),
        quality_check_count=quality_config.rule_count,
        silver_contract_paths=tuple(contracts),
        silver_table_names=tuple(
            str(table_key)
            for contract in contracts.values()
            for table_key in contract.get("tables", {})
        ),
        glossary_terms=glossary_terms,
    )
