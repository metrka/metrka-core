"""Compose Bronze ingestion collaborators."""

from __future__ import annotations

from metrka_core.pipeline.bronze.asset_ingestion import BronzeIngestDeps
from metrka_core.pipeline.composition.metadata import MetadataComposition
from metrka_core.pipeline.composition.workspace import WorkspaceComposition
from metrka_core.pipeline.runtime_services import (
    BronzeRunIdGenerator,
    Clock,
    DatasetFileIdGenerator,
)


def build_bronze_composition(
    *,
    workspace: WorkspaceComposition,
    metadata: MetadataComposition,
    clock: Clock,
    dataset_file_ids: DatasetFileIdGenerator,
    bronze_run_ids: BronzeRunIdGenerator,
) -> BronzeIngestDeps:
    """Build the dependencies of Bronze ingestion."""

    return BronzeIngestDeps(
        clock=clock,
        dataset_file_ids=dataset_file_ids,
        bronze_run_ids=bronze_run_ids,
        source_config=workspace.source_config,
        bronze_store=workspace.bronze_store,
        marshal=metadata.marshal,
        execution_logs=metadata.execution_logs,
        quality_checks=metadata.quality_checks,
        file_marshal_store=metadata.file_marshal_store,
    )
