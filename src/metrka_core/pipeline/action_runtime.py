"""Small runtime identity passed to pipeline actions."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.datasets.dataset_identity import RunName
from metrka_core.pipeline.provenance import CodeProvenance


@dataclass(frozen=True)
class ActionRuntime:
    """
    Runtime identity shared by pipeline actions.

    This object deliberately contains no persistence or storage
    dependencies.
    """

    pipeline_run_id: str
    # The run name (`workspace` or `workspace.dataset`), not the source workspace name.
    dataset_name: RunName
    code_provenance: CodeProvenance
