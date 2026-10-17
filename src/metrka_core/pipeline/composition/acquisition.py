"""Compose source acquisition collaborators."""

from __future__ import annotations

from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.composition.metadata import MetadataComposition
from metrka_core.pipeline.composition.workspace import WorkspaceComposition


def build_acquisition_composition(
    *, workspace: WorkspaceComposition, metadata: MetadataComposition
) -> AcquisitionDeps:
    return AcquisitionDeps(
        source_config=workspace.source_config,
        landing_store=workspace.landing_store,
        execution_log_store=metadata.execution_logs,
    )
