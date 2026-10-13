"""Bind documentation assets to data assets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from metrka_core.datasets.source_config import SourceConfig
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.action_models import ActionOutcome
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.bronze.models import BronzeBatchResult

if TYPE_CHECKING:
    from metrka_core.pipeline.models import PipelineRunState


@dataclass(frozen=True)
class DocumentationBindOptions:
    """Validated options for documentation binding."""


def parse_documentation_bind_options(raw: Mapping[str, Any]) -> DocumentationBindOptions:
    """Validate YAML options for the ``documentation.bind`` action.

    The action accepts no options. An empty mapping returns the validated
    options object; every supplied key raises ``ValueError``.
    """

    if raw:
        raise ValueError("documentation.bind does not accept options")

    return DocumentationBindOptions()


@dataclass(frozen=True)
class DocumentationBindDeps:
    """Dependencies required to bind documentation assets."""

    source_config: SourceConfig
    execution_logs: ExecutionLogStore


@dataclass(frozen=True)
class DocumentationAssetRef:
    """Identity of one Bronze asset used in a binding."""

    stream_name: str
    dataset_id: str
    dataset_file_id: str
    source_hash: str

    def to_dict(self) -> dict[str, str]:
        """Return the stable audit representation of the asset identity."""

        return {
            "stream_name": self.stream_name,
            "dataset_id": self.dataset_id,
            "dataset_file_id": self.dataset_file_id,
            "source_hash": self.source_hash,
        }


@dataclass(frozen=True)
class DocumentationBinding:
    """Relationship between one data asset and documentation asset."""

    data: DocumentationAssetRef
    documentation: DocumentationAssetRef

    def to_dict(self) -> dict[str, dict[str, str]]:
        """Return the stable audit representation of one binding."""

        return {"data": self.data.to_dict(), "documentation": self.documentation.to_dict()}


@dataclass(frozen=True)
class DocumentationBindResult:
    """Structured result of documentation binding."""

    bindings: tuple[DocumentationBinding, ...]

    @property
    def binding_count(self) -> int:
        """Return the number of recorded data-to-documentation bindings."""

        return len(self.bindings)

    def to_dicts(self) -> list[dict[str, dict[str, str]]]:
        """Return all bindings in their audit representation."""

        return [binding.to_dict() for binding in self.bindings]


def bind_documentation_assets(
    *, runtime: ActionRuntime, deps: DocumentationBindDeps, bronze_batch: BronzeBatchResult
) -> DocumentationBindResult:
    """Bind documentation assets to data assets from one Bronze batch."""

    data_assets: list[DocumentationAssetRef] = []
    documentation_assets: list[DocumentationAssetRef] = []

    for stream_name, bronze_result in bronze_batch.by_stream.items():
        stream = deps.source_config.streams[stream_name]

        asset = DocumentationAssetRef(
            stream_name=stream_name,
            dataset_id=bronze_result.dataset_id,
            dataset_file_id=bronze_result.dataset_file_id,
            source_hash=bronze_result.source_hash,
        )

        if stream.artifact_role == "data":
            data_assets.append(asset)
        elif stream.artifact_role == "documentation":
            documentation_assets.append(asset)

    if not documentation_assets:
        raise RuntimeError("documentation.bind found no documentation assets")

    if not data_assets:
        raise RuntimeError("documentation.bind found no data assets")

    bindings = tuple(
        DocumentationBinding(data=data_asset, documentation=documentation_asset)
        for data_asset in data_assets
        for documentation_asset in documentation_assets
    )

    result = DocumentationBindResult(bindings=bindings)

    binding_payload = result.to_dicts()

    with run_step(
        dataset=runtime.dataset_name,
        step="bind_documentation_assets",
        layer="bronze",
        start_meta=ExecutionStepMeta(
            extra={
                "data_asset_count": len(data_assets),
                "documentation_asset_count": len(documentation_assets),
            }
        ),
        execution_log_store=deps.execution_logs,
    ) as step_context:
        step_context.count_success(result.binding_count)
        step_context.set_finish_meta(
            ExecutionStepMeta(
                extra={
                    "data_asset_count": len(data_assets),
                    "documentation_asset_count": len(documentation_assets),
                    "binding_count": result.binding_count,
                    "bindings": binding_payload,
                }
            )
        )

    return result


def bind_documentation_action(
    *,
    runtime: ActionRuntime,
    deps: DocumentationBindDeps,
    state: PipelineRunState,
    options: DocumentationBindOptions,
) -> ActionOutcome:
    """Bind documentation assets acquired in the same run."""

    _ = options

    if state.bronze_batch is None:
        raise RuntimeError("documentation.bind requires bronze.ingest to run first")

    result = bind_documentation_assets(runtime=runtime, deps=deps, bronze_batch=state.bronze_batch)

    state.action_results["documentation.bind"] = result.to_dicts()

    return ActionOutcome(
        status="completed",
        message=(f"Recorded {result.binding_count} documentation bindings."),
        metrics={"binding_count": result.binding_count},
    )
