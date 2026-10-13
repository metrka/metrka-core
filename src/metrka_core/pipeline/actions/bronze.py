"""Pipeline adapter for Bronze ingestion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from metrka_core.pipeline.acquisition.models import SourceCaptureAssetBinding
from metrka_core.pipeline.action_models import ActionOutcome, ArtifactRef
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.bronze.asset_ingestion import ingest_landed_assets
from metrka_core.pipeline.models import LandedAsset

if TYPE_CHECKING:
    from metrka_core.pipeline.acquisition.postgres_source_capture_store import (
        PostgresSourceCaptureStore,
    )
    from metrka_core.pipeline.bronze.asset_ingestion import BronzeIngestDeps
    from metrka_core.pipeline.models import PipelineRunState


@dataclass(frozen=True)
class BronzeIngestOptions:
    """Validated options for Bronze ingestion."""


@dataclass(frozen=True)
class BronzeIngestActionDeps:
    """Dependencies required by the Bronze action adapter."""

    bronze: BronzeIngestDeps
    source_captures: PostgresSourceCaptureStore


def parse_bronze_ingest_options(raw: Mapping[str, Any]) -> BronzeIngestOptions:
    """Validate YAML options for the ``bronze.ingest`` action.

    The action accepts no options. An empty mapping returns the validated
    options object; every supplied key raises ``ValueError``.
    """

    if raw:
        raise ValueError("bronze.ingest does not accept options")

    return BronzeIngestOptions()


def ingest_bronze_action(
    *,
    runtime: ActionRuntime,
    deps: BronzeIngestActionDeps,
    state: PipelineRunState,
    options: BronzeIngestOptions,
) -> ActionOutcome:
    """Ingest acquired assets into Bronze."""
    _ = options

    if not state.landed_assets:
        raise RuntimeError("bronze.ingest requires acquired landed assets")

    bronze_batch = ingest_landed_assets(
        runtime=runtime, deps=deps.bronze, assets=state.landed_assets
    )

    source_capture = state.source_capture

    if source_capture is None:
        raise RuntimeError("bronze.ingest requires a source capture")

    landed_assets_by_stream: dict[str, list[LandedAsset]] = {}

    for landed_asset in state.landed_assets:
        landed_assets_by_stream.setdefault(landed_asset.stream_name, []).append(landed_asset)

    bindings: list[SourceCaptureAssetBinding] = []

    for stream_name, ingest_result in bronze_batch.by_stream.items():
        stream_assets = landed_assets_by_stream.get(stream_name)

        if not stream_assets:
            raise RuntimeError(f"Bronze result has no corresponding landed asset: {stream_name}")

        for landed_asset in stream_assets:
            try:
                relative_path = (
                    landed_asset.path.resolve()
                    .relative_to(source_capture.directory.resolve())
                    .as_posix()
                )
            except ValueError as error:
                raise RuntimeError(
                    f"Landed asset is outside its source capture directory: {landed_asset.path}"
                ) from error

            bindings.append(
                SourceCaptureAssetBinding(
                    stream_name=stream_name,
                    dataset_id=ingest_result.dataset_id,
                    dataset_file_id=(ingest_result.dataset_file_id),
                    relative_path=relative_path,
                    source_url=landed_asset.source_url,
                    artifact_role=landed_asset.artifact_role,
                    source_last_modified=(landed_asset.source_last_modified),
                )
            )

    deps.source_captures.bind_assets(
        source_capture_id=source_capture.source_capture_id, assets=tuple(bindings)
    )

    state.bronze_batch = bronze_batch
    state.action_results["bronze.ingest"] = bronze_batch

    return ActionOutcome(
        status="completed",
        message=(f"Processed {bronze_batch.total_count} landed assets."),
        produced_artifacts=tuple(
            ArtifactRef(
                kind="bronze_file", identifier=result.dataset_file_id, dataset_id=result.dataset_id
            )
            for result in bronze_batch.by_stream.values()
        ),
        metrics={
            "asset_count": bronze_batch.total_count,
            "new_file_count": bronze_batch.new_count,
            "duplicate_file_count": bronze_batch.duplicate_count,
        },
    )
