"""Shared runtime context for pipeline actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from metrka_core.pipeline.action_runtime import ActionRuntime

if TYPE_CHECKING:
    from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
    from metrka_core.pipeline.bronze.asset_ingestion import BronzeIngestDeps
    from metrka_core.pipeline.composition.metadata import MetadataComposition
    from metrka_core.pipeline.composition.runtime import RuntimeComposition
    from metrka_core.pipeline.composition.workspace import WorkspaceComposition
    from metrka_core.pipeline.silver.dependencies import SilverProcessDeps


@dataclass(frozen=True)
class PipelineContext:
    """
    Composed dependencies available at the pipeline boundary.

    Concrete services are grouped by architectural responsibility
    instead of being exposed as one flat collection.
    """

    runtime: RuntimeComposition
    workspace: WorkspaceComposition
    metadata: MetadataComposition
    acquisition: AcquisitionDeps
    bronze: BronzeIngestDeps
    silver: SilverProcessDeps

    def as_action_runtime(self) -> ActionRuntime:
        """Return the infrastructure-free runtime view."""

        return ActionRuntime(
            pipeline_run_id=self.runtime.pipeline_run_id,
            dataset_name=self.workspace.run_name,
            code_provenance=self.runtime.code_provenance,
        )
