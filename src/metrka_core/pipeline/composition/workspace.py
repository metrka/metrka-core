"""Build the local workspace used by one pipeline execution."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from metrka_core.catalog.glossary import load_glossary, validate_contract_terms
from metrka_core.datasets.dataset_identity import RunName
from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.source_config import (
    SourceConfig,
    load_source_config,
    require_dataset_folder_streams,
)
from metrka_core.pipeline.config import parse_quality_settings
from metrka_core.pipeline.runtime_services import Clock, SourceCaptureIdGenerator
from metrka_core.pipeline.silver.dataset_links import validate_dataset_links
from metrka_core.pipeline.silver.task_factory import load_silver_contracts
from metrka_core.quality.config import QualityConfig, load_quality_config, validate_quality_config
from metrka_core.storage.bronze_store import BronzeArtifactStore, LocalBronzeArtifactStore
from metrka_core.storage.config_store import ConfigStore, LocalConfigStore
from metrka_core.storage.contract_store import ContractSnapshotStore, LocalContractSnapshotStore
from metrka_core.storage.landing_store import LandingStore, LocalLandingStore
from metrka_core.storage.silver_store import LocalSilverArtifactStore
from metrka_core.storage.workspace_initializer import LocalWorkspaceInitializer
from metrka_core.storage.workspace_layout import WorkspaceLayout

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkspaceComposition:
    """Resolved local storage and configuration for one workspace."""

    run_name: RunName
    layout: WorkspaceLayout
    landing_store: LandingStore
    bronze_store: BronzeArtifactStore
    silver_store: LocalSilverArtifactStore
    config_store: ConfigStore
    contract_store: ContractSnapshotStore
    source_config: SourceConfig
    quality_config: QualityConfig


def build_workspace_composition(
    *,
    workspace_name: str,
    config_name: str,
    workspace_locations: WorkspaceLocationResolver,
    clock: Clock,
    source_capture_ids: SourceCaptureIdGenerator,
) -> WorkspaceComposition:
    """Resolve and validate the local pipeline workspace."""

    if not workspace_name.strip():
        raise ValueError("workspace_name must not be empty")

    location = workspace_locations.resolve(workspace_name)

    if location.name != workspace_name:
        raise ValueError(
            "WorkspaceLocationResolver returned a location for a different workspace: "
            f"{location.name!r}"
        )

    layout = WorkspaceLayout(location=location)
    glossary = load_glossary(location.glossary_path)

    LocalWorkspaceInitializer(layout=layout).ensure_structure()

    landing_store = LocalLandingStore(
        root=layout.bronze_landing_dir, clock=clock, source_capture_ids=source_capture_ids
    )

    bronze_store = LocalBronzeArtifactStore(
        workspace_root=layout.data_root,
        bronze_root=layout.bronze_dir,
        current_root=layout.current_dir,
    )

    silver_store = LocalSilverArtifactStore(
        workspace_root=layout.data_root,
        silver_root=layout.silver_dir,
        current_root=layout.current_dir,
    )

    config_store = LocalConfigStore(
        workspace_root=layout.definition_root, config_root=layout.conf_dir
    )

    contract_store = LocalContractSnapshotStore(
        definition_root=layout.definition_root,
        data_root=layout.data_root,
        snapshots_root=layout.contract_snapshots_dir,
        glossary=glossary,
    )

    source_config = load_source_config(
        config_store.path(name=config_name), expected_ws_name=location.workspace_name
    )

    if location.dataset_name is not None:
        require_dataset_folder_streams(source_config, dataset_name=location.dataset_name)

    quality_settings = parse_quality_settings(source_config.pipeline.get("quality"))

    quality_config = load_quality_config(config_store.path(name=quality_settings.config))

    contracts = load_silver_contracts(source_config=source_config, config_store=config_store)
    validate_quality_config(quality_config, contracts.values())
    validate_dataset_links(
        source_config=source_config, config_store=config_store, contracts=contracts
    )
    validate_contract_terms(glossary, contracts)

    logger.info(
        "Loaded quality config %s with %d rules", quality_settings.config, quality_config.rule_count
    )

    return WorkspaceComposition(
        run_name=location.name,
        layout=layout,
        landing_store=landing_store,
        bronze_store=bronze_store,
        silver_store=silver_store,
        config_store=config_store,
        contract_store=contract_store,
        source_config=source_config,
        quality_config=quality_config,
    )
