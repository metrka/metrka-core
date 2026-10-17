"""Batch ingestion of landed assets into Bronze."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.metadata.bronze_artifact_integrity import capture_bronze_artifacts
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.bronze.bronze_ingestion import (
    _raise_if_quality_failed,
    _update_latest_pointer,
    ingest_to_bronze,
)
from metrka_core.pipeline.bronze.models import BronzeBatchResult, BronzeIngestResult
from metrka_core.pipeline.bronze.source_batch_fingerprint import fingerprint_source_batch
from metrka_core.pipeline.bronze.xlsx_batch_ingestion import build_xlsx_batch_marshaled_file
from metrka_core.pipeline.bronze.xlsx_batch_preparation import (
    PreparedXlsxSourceBatch,
    prepare_xlsx_source_batch,
)
from metrka_core.pipeline.bronze.xlsx_row_assembly import split_skipped_paths
from metrka_core.pipeline.models import LandedAsset
from metrka_core.pipeline.runtime_services import (
    BronzeRunIdGenerator,
    Clock,
    DatasetFileIdGenerator,
)
from metrka_core.quality.gates import (
    BronzeOutput,
    LandedFile,
    RunIds,
    check_bronze_output,
    check_landed_file,
)
from metrka_core.quality.models import QualityOutputFile
from metrka_core.quality.store import QualityCheckStore
from metrka_core.storage.bronze_store import BronzeArtifactStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeIngestDeps:
    """Dependencies required by Bronze batch ingestion."""

    clock: Clock
    dataset_file_ids: DatasetFileIdGenerator
    bronze_run_ids: BronzeRunIdGenerator
    source_config: SourceConfig
    bronze_store: BronzeArtifactStore
    marshal: FileMarshal
    execution_logs: ExecutionLogStore
    quality_checks: QualityCheckStore
    file_marshal_store: PostgresFileMarshalStore


def _group_landed_assets_by_stream(assets: list[LandedAsset]) -> dict[str, tuple[LandedAsset, ...]]:
    """Group landed assets by stream in a deterministic order."""
    grouped: dict[str, list[LandedAsset]] = {}

    for asset in assets:
        grouped.setdefault(asset.stream_name, []).append(asset)

    return {
        stream_name: tuple(
            sorted(
                stream_assets, key=lambda asset: (asset.path.name.casefold(), asset.path.as_posix())
            )
        )
        for stream_name, stream_assets in sorted(grouped.items())
    }


def _persist_xlsx_asset_batch(
    *,
    runtime: ActionRuntime,
    deps: BronzeIngestDeps,
    stream: StreamConfig,
    dataset_id: str,
    assets: tuple[LandedAsset, ...],
) -> tuple[BronzeIngestResult, PreparedXlsxSourceBatch | None]:
    """Ingest one configured XLSX stream batch."""
    assembly_config = stream.bronze_assembly
    filename_config = stream.filename_metadata

    if assembly_config is None or filename_config is None:
        raise RuntimeError(
            f"Stream {stream.name} does not have complete XLSX Bronze assembly configuration"
        )

    input_paths = tuple(asset.path for asset in assets)
    fingerprint = fingerprint_source_batch(input_paths)

    existing = deps.marshal.get_by_hash(dataset_id, fingerprint.sha256)

    if existing is not None:
        if not existing.bronze_artifacts:
            raise RuntimeError(
                "Existing XLSX batch has no Bronze artifact manifest: "
                f"{existing.file.dataset_file_id}"
            )

        return (
            BronzeIngestResult(
                dataset_file_id=existing.file.dataset_file_id,
                dataset_id=existing.file.dataset_id,
                source_hash=existing.file.source_hash,
                bronze_run_id=existing.bronze_run_id,
                is_new=False,
            ),
            None,
        )

    bronze_run_id = deps.bronze_run_ids.new_bronze_run_id()
    dataset_file_id = deps.dataset_file_ids.new_dataset_file_id()

    fingerprint_members = {member.filename: member for member in fingerprint.members}
    quality_ids = RunIds(
        dataset_id=dataset_id,
        run_id=bronze_run_id,
        pipeline_run_id=runtime.pipeline_run_id,
        dataset_file_id=dataset_file_id,
    )

    for asset in assets:
        member = fingerprint_members[asset.path.name]

        pre_quality = check_landed_file(
            LandedFile(
                path=asset.path,
                sha256=member.sha256,
                fingerprint={
                    member.filename: {
                        "name": member.filename,
                        "sha256": member.sha256,
                        "size": member.size_bytes,
                    }
                },
                xlsx_sheet_name=assembly_config.read_config.sheet_name,
                xlsx_header_row=assembly_config.read_config.header_row,
            ),
            ids=quality_ids,
            store=deps.quality_checks,
        )

        _raise_if_quality_failed("XLSX batch pre-checks", pre_quality)

    bronze_run_dir = deps.bronze_store.prepare_run_dir(run_id=bronze_run_id)

    prepared = prepare_xlsx_source_batch(
        assets=assets,
        output_dir=bronze_run_dir,
        filename_config=filename_config,
        assembly_config=assembly_config,
    )

    marshaled_file = build_xlsx_batch_marshaled_file(
        prepared=prepared,
        dataset_id=dataset_id,
        dataset_file_id=dataset_file_id,
        ingested_at=deps.clock.now_utc(),
    )

    payload_fingerprint = {
        member.filename: {
            "name": member.filename,
            "sha256": member.sha256,
            "size": member.size_bytes,
        }
        for member in prepared.fingerprint.members
    }

    landing_paths = [deps.bronze_store.relative_path(path) for path in prepared.input_paths]

    deps.marshal.register(
        marshaled_file,
        meta={
            "stage": "landing",
            "bronze_run_id": bronze_run_id,
            "source_capture_ids": list(prepared.source_capture_ids),
            "landing_paths": landing_paths,
            "payload_fingerprint": payload_fingerprint,
        },
    )

    bronze_artifacts = capture_bronze_artifacts(
        bronze_run_dir=bronze_run_dir, output_paths=list(prepared.output_paths)
    )

    quality_output_files = tuple(
        QualityOutputFile(
            local_path=output_path,
            workspace_relative_path=deps.bronze_store.relative_path(output_path),
        )
        for output_path in prepared.output_paths
    )

    post_quality = check_bronze_output(
        BronzeOutput(files=quality_output_files), ids=quality_ids, store=deps.quality_checks
    )

    _raise_if_quality_failed("XLSX batch post-checks", post_quality)

    deps.marshal.record_bronze_artifacts(
        marshaled_file.dataset_file_id,
        bronze_artifacts,
        meta={"stage": "bronze", "source_capture_ids": list(prepared.source_capture_ids)},
    )

    _update_latest_pointer(
        bronze_store=deps.bronze_store,
        bronze_run_dir=bronze_run_dir,
        dataset_id=dataset_id,
        run_id=bronze_run_id,
        content_hash=prepared.fingerprint.sha256,
        updated_at=deps.clock.now_utc(),
    )

    logger.info(
        "Assembled %d XLSX files into %s with %d rows",
        prepared.input_file_count,
        ", ".join(path.name for path in prepared.output_paths),
        prepared.row_count,
    )

    return (
        BronzeIngestResult(
            dataset_file_id=marshaled_file.dataset_file_id,
            dataset_id=marshaled_file.dataset_id,
            source_hash=marshaled_file.source_hash,
            bronze_run_id=bronze_run_id,
            is_new=True,
        ),
        prepared,
    )


def _split_skipped_assets(
    stream: StreamConfig, assets: tuple[LandedAsset, ...]
) -> tuple[tuple[LandedAsset, ...], tuple[LandedAsset, ...]]:
    """Leave out the assets whose group value the stream skips, before any fingerprint."""
    assembly_config = stream.bronze_assembly
    filename_config = stream.filename_metadata

    if assembly_config is None or filename_config is None:
        return assets, ()

    _, skipped_paths = split_skipped_paths(
        paths=(asset.path for asset in assets),
        filename_config=filename_config,
        assembly_config=assembly_config,
    )
    skipped = set(skipped_paths)

    return (
        tuple(asset for asset in assets if asset.path not in skipped),
        tuple(asset for asset in assets if asset.path in skipped),
    )


def _ingest_xlsx_asset_batch(
    *,
    runtime: ActionRuntime,
    deps: BronzeIngestDeps,
    stream: StreamConfig,
    dataset_id: str,
    assets: tuple[LandedAsset, ...],
) -> BronzeIngestResult:
    """Ingest one XLSX batch and record its execution receipt."""
    assets, skipped_assets = _split_skipped_assets(stream, assets)

    if skipped_assets:
        logger.info(
            "Stream %s leaves %d file(s) out of Bronze: their %s value is skipped",
            stream.name,
            len(skipped_assets),
            stream.bronze_assembly.group_files_by if stream.bronze_assembly else None,
        )

    source_capture_ids = sorted({asset.source_capture_id for asset in assets})

    start_meta = ExecutionStepMeta(
        dataset_id=dataset_id,
        source_capture_id=(source_capture_ids[0] if len(source_capture_ids) == 1 else None),
        input_file_count=len(assets),
        input_byte_count=sum(asset.path.stat().st_size for asset in assets),
        extra={
            "source_capture_ids": source_capture_ids,
            "input_files": [asset.path.name for asset in assets],
            **(
                {"skipped_files": [asset.path.name for asset in skipped_assets]}
                if skipped_assets
                else {}
            ),
        },
    )

    with run_step(
        dataset=runtime.dataset_name,
        step="assemble_xlsx_batch",
        layer="bronze",
        execution_log_store=deps.execution_logs,
        clock=deps.clock,
        start_meta=start_meta,
    ) as context:
        result, prepared = _persist_xlsx_asset_batch(
            runtime=runtime, deps=deps, stream=stream, dataset_id=dataset_id, assets=assets
        )

        if result.is_new:
            context.count_success(1)
        else:
            context.count_skipped(1)

        finish_meta = ExecutionStepMeta(
            dataset_file_id=result.dataset_file_id, bronze_run_id=result.bronze_run_id
        )

        if prepared is not None:
            finish_meta = finish_meta.merged_with(
                ExecutionStepMeta(
                    source_file_name=", ".join(path.name for path in prepared.output_paths),
                    output_row_count=prepared.row_count,
                    output_column_count=prepared.column_count,
                    output_file_count=len(prepared.output_paths),
                    output_byte_count=sum(path.stat().st_size for path in prepared.output_paths),
                    extra={"source_batch_hash": (prepared.fingerprint.sha256)},
                )
            )

        context.set_finish_meta(finish_meta)

    return result


def ingest_landed_assets(
    *, runtime: ActionRuntime, deps: BronzeIngestDeps, assets: list[LandedAsset]
) -> BronzeBatchResult:
    """Register and stage landed stream batches in Bronze."""
    source_config = deps.source_config
    grouped_assets = _group_landed_assets_by_stream(assets)

    ingested_assets: dict[str, BronzeIngestResult] = {}
    new_count = 0
    duplicate_count = 0

    for stream_name, stream_assets in grouped_assets.items():
        if stream_name not in source_config.streams:
            raise RuntimeError(f"Unknown stream returned by acquisition: {stream_name}")

        stream = source_config.streams[stream_name]
        dataset_id = source_config.dataset_id(stream_name)

        for asset in stream_assets:
            if asset.artifact_role != stream.artifact_role:
                raise RuntimeError(
                    f"Artifact role mismatch for stream {stream_name}: "
                    f"asset={asset.artifact_role!r}, "
                    f"configured={stream.artifact_role!r}"
                )

        if stream.bronze_assembly is not None:
            if stream.filename_metadata is None:
                raise RuntimeError(
                    f"Stream {stream_name} configures bronze_assembly without filename_metadata"
                )

            result = _ingest_xlsx_asset_batch(
                runtime=runtime,
                deps=deps,
                stream=stream,
                dataset_id=dataset_id,
                assets=stream_assets,
            )
        else:
            if len(stream_assets) != 1:
                raise RuntimeError(
                    "Acquisition returned multiple assets for stream "
                    f"{stream_name}, but bronze_assembly is not configured"
                )

            asset = stream_assets[0]

            logger.info("Processing landed asset for %s: %s", dataset_id, asset.path.name)

            single_result = ingest_to_bronze(
                dataset_name=runtime.dataset_name,
                bronze_store=deps.bronze_store,
                marshal=deps.marshal,
                landed_file=asset.path,
                dataset_id=dataset_id,
                source_capture_id=asset.source_capture_id,
                source_url=asset.source_url,
                execution_log_store=deps.execution_logs,
                quality_store=deps.quality_checks,
                file_marshal_store=deps.file_marshal_store,
                clock=deps.clock,
                dataset_file_ids=deps.dataset_file_ids,
                bronze_run_ids=deps.bronze_run_ids,
                artifact_role=asset.artifact_role,
                source_last_modified=asset.source_last_modified,
                pipeline_run_id=runtime.pipeline_run_id,
            )

            if single_result is None:
                raise RuntimeError(f"Bronze ingestion returned no result for {asset.path}")

            result = single_result

        ingested_assets[stream_name] = result

        if result.is_new:
            new_count += 1

            logger.info(
                "Registered %s as file %s in Bronze run %s",
                dataset_id,
                result.dataset_file_id,
                result.bronze_run_id,
            )
        else:
            duplicate_count += 1

            logger.info(
                "Skipped duplicate %s; existing file is %s", dataset_id, result.dataset_file_id
            )

    return BronzeBatchResult(
        by_stream=ingested_assets, new_count=new_count, duplicate_count=duplicate_count
    )
