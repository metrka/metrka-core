"""
Generic Silver Builder.

Reads a source file, loads the table config, runs the schema transform and then saves the
result to Silver staging.
Dataset-specific quirks go in pre/post hooks, not here.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from metrka_core.lineage.transformation import (
    TransformationImpact,
    TransformationObservation,
    write_transformation_details,
)
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.config import load_table_cfg
from metrka_core.pipeline.runtime_services import TransformationImpactIdGenerator
from metrka_core.pipeline.silver.artifact_models import SilverArtifactRef
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.fingerprints import (
    SilverTableBuildResult,
    fingerprint_silver_table,
)
from metrka_core.pipeline.silver.version_period import VersionPeriod
from metrka_core.quality.config import QualityConfig
from metrka_core.quality.gates import RunIds, SilverTable, check_silver_input, check_silver_output
from metrka_core.quality.models import QualityOutputFile
from metrka_core.quality.store import QualityCheckStore
from metrka_core.storage.atomic_writes import atomic_write_text
from metrka_core.storage.checksums import sha256_file
from metrka_core.storage.save import save_table
from metrka_core.transform.schema import apply_transformation

logger = logging.getLogger(__name__)
Hook = Callable[[pd.DataFrame], pd.DataFrame]


def _contract_execution_meta(contract_meta: dict[str, str]) -> ExecutionStepMeta:
    return ExecutionStepMeta(
        contract_hash=contract_meta.get("contract_hash"),
        contract_name=contract_meta.get("contract_name"),
        contract_path=contract_meta.get("contract_path"),
        contract_version=contract_meta.get("contract_version"),
        contract_snapshot_yaml_path=contract_meta.get("contract_snapshot_yaml_path"),
        contract_snapshot_json_path=contract_meta.get("contract_snapshot_json_path"),
    )


def build_silver_table(
    dataset_name: str,
    silver_store: SilverArtifactStore,
    dataset_id: str,
    bronze_file_id: str,
    bronze_run_id: str,
    silver_build_id: str,
    version_period: VersionPeriod,
    partition_key: str,
    partition_value: str,
    source_file_name: str,
    bronze_ingested_at: datetime,
    silver_processed_at: datetime,
    input_file_path: Path,
    cfg_path: Path,
    table_key: str,
    execution_log_store: ExecutionLogStore,
    quality_store: QualityCheckStore,
    transformation_impact_store: PostgresTransformationImpactStore,
    transformation_impact_ids: TransformationImpactIdGenerator,
    run_id: str,
    pipeline_run_id: str,
    quality_config: QualityConfig,
    contract_meta: dict[str, str] | None = None,
    input_format: str = "csv",
    input_kwargs: dict[str, Any] | None = None,
    output_formats: str | list[str] | None = None,
    pre_hook: Hook | None = None,
    post_hook: Hook | None = None,
) -> SilverTableBuildResult:
    """Build one Silver table into staging and update execution telemetry."""
    contract_meta = contract_meta or {}

    input_files = [input_file_path]

    # ==============================================================================
    # 1.Resolve Outputs Formats
    # ==============================================================================
    if output_formats is None:
        target_formats = ["parquet"]
    elif isinstance(output_formats, str):
        target_formats = [output_formats]
    else:
        target_formats = list(output_formats)

    content_hash = sha256_file(input_file_path)

    # ==============================================================================
    # 2. Open the Compute Context (Postgres telemetry)
    # ==============================================================================
    with run_step(
        dataset=dataset_name,
        step=f"build_{table_key.lower()}",
        layer="silver",
        run_id=run_id,
        start_meta=ExecutionStepMeta(
            dataset_id=dataset_id,
            dataset_file_id=str(bronze_file_id),
            source_file_name=source_file_name,
            table_key=table_key,
            bronze_run_id=bronze_run_id,
            silver_run_id=run_id,
            silver_build_id=silver_build_id,
            version_period=version_period.value.isoformat(),
            partition_key=partition_key,
            partition_value=partition_value,
            extra={
                "version_period_grain": version_period.grain,
                "version_period_source": version_period.source,
                "content_hash": content_hash,
                "bronze_ingested_at": _utc_isoformat(bronze_ingested_at),
                "input_file": input_file_path.name,
                "input_format": input_format,
                "target_formats": target_formats,
            },
        ).merged_with(_contract_execution_meta(contract_meta)),
        execution_log_store=execution_log_store,
    ) as ctx:
        step_id = ctx.execution.step_id
        logger.info(
            "start: run_id=%s step_id=%s dataset_id=%s in=%s table_key=%s partition=%s=%s formats=%s",
            run_id,
            step_id,
            dataset_id,
            input_file_path.name,
            table_key,
            partition_key,
            partition_value,
            target_formats,
        )

        # ==============================================================================
        # 3. Read inputs + load config
        # ==============================================================================
        read_kwargs = input_kwargs or {}
        if input_format == "tsv":
            read_kwargs.setdefault("sep", "\t")

        if input_format in ("csv", "txt", "tsv"):
            read_kwargs.setdefault("dtype", str)
            df = pd.read_csv(input_file_path, **read_kwargs)
        elif input_format == "parquet":
            df = pd.read_parquet(input_file_path, **read_kwargs)
        else:
            raise ValueError(f"Unsupported input_format: {input_format}")

        input_row_count = len(df)
        input_column_count = len(df.columns)

        table_cfg = load_table_cfg(cfg_path, table_key=table_key)

        expected_source_columns = tuple(str(column) for column in table_cfg.get("columns", {}))

        quality_ids = RunIds(
            dataset_id=dataset_id,
            run_id=run_id,
            pipeline_run_id=pipeline_run_id,
            dataset_file_id=bronze_file_id,
            silver_build_id=silver_build_id,
        )

        pre_quality = check_silver_input(
            SilverTable(
                table_key=table_key,
                frame=df,
                expected_columns=expected_source_columns,
                source_file_name=source_file_name,
            ),
            ids=quality_ids,
            store=quality_store,
        )

        if pre_quality.failed:
            ctx.count_blocked(pre_quality.blocked_count)
            ctx.set_finish_meta(
                ExecutionStepMeta(
                    table_key=table_key,
                    bronze_run_id=bronze_run_id,
                    silver_run_id=run_id,
                    silver_build_id=silver_build_id,
                    input_row_count=input_row_count,
                    input_column_count=input_column_count,
                    extra={"input_file": input_file_path.name, **pre_quality.to_meta()},
                ).merged_with(_contract_execution_meta(contract_meta))
            )
            raise RuntimeError(
                "Blocking PRE_SILVER quality gate "
                f"failed for {dataset_id}."
                f"{table_key}: "
                f"{pre_quality.error_message}"
            )

        # ==============================================================================
        # 4. Optional hooks + transform
        # ==============================================================================

        if pre_hook is not None:
            df = pre_hook(df)

        try:
            transformation_result = apply_transformation(df, table_cfg)
            table = transformation_result.data
        except Exception as e:
            ctx.count_failed(1)
            raise RuntimeError(
                f"Silver transformation failed for table_key={table_key} "
                f"input_file={input_file_path.name}: {e}"
            ) from e

        if post_hook is not None:
            table = post_hook(table)

        canonical_columns = [str(column) for column in table_cfg["canonical_order"]]
        table = _select_canonical_columns(
            table, canonical_columns=canonical_columns, table_key=table_key
        )

        table_fingerprint = fingerprint_silver_table(table_key=table_key, table=table)

        # ==============================================================================
        # 5. Save table (Multi-Target Output)
        # ==============================================================================

        artifact_ref = SilverArtifactRef(
            dataset_id=dataset_id,
            table_key=table_key,
            partition_key=partition_key,
            partition_value=partition_value,
            silver_build_id=silver_build_id,
        )

        target_path = silver_store.staging_file_stem(run_id=run_id, artifact=artifact_ref)

        saved_paths = []

        for fmt in target_formats:
            saved_file = save_table(table, dest_path=target_path, fmt=fmt)
            saved_paths.append(saved_file)
            logger.debug("Saved file to: %s", saved_file)

        preview_path = _write_preview_json(table, target_path, table_cfg=table_cfg, preview_rows=10)
        # Preview JSON is a website sidecar. It is copied with the table files,
        # but promotion manifests inspect only data formats.
        staged_paths = saved_paths + [preview_path]
        # ==============================================================================
        # 6. Silver post-quality gate
        # ==============================================================================
        output_row_count = len(table)
        output_column_count = len(table.columns)

        quality_output_files = tuple(
            QualityOutputFile(
                local_path=path, workspace_relative_path=silver_store.relative_path(path)
            )
            for path in saved_paths
        )

        post_quality = check_silver_output(
            SilverTable(
                table_key=table_key,
                frame=table,
                expected_columns=tuple(canonical_columns),
                source_file_name=source_file_name,
                output_files=quality_output_files,
            ),
            config=quality_config,
            ids=quality_ids,
            store=quality_store,
            as_of=silver_processed_at.date(),
        )

        if post_quality.failed:
            ctx.count_blocked(post_quality.blocked_count)
            ctx.set_finish_meta(
                ExecutionStepMeta(
                    table_key=table_key,
                    bronze_run_id=bronze_run_id,
                    silver_run_id=run_id,
                    silver_build_id=silver_build_id,
                    input_row_count=input_row_count,
                    output_row_count=output_row_count,
                    input_column_count=input_column_count,
                    output_column_count=output_column_count,
                    output_file_count=len(saved_paths),
                    extra={
                        "input_file": input_file_path.name,
                        "saved_files": [silver_store.relative_path(path) for path in saved_paths],
                        **pre_quality.to_meta(),
                        **post_quality.to_meta(),
                    },
                ).merged_with(_contract_execution_meta(contract_meta))
            )
            raise RuntimeError(
                "Blocking POST_SILVER quality gate "
                f"failed for {dataset_id}."
                f"{table_key}: "
                f"{post_quality.error_message}"
            )

        # ======================================================================
        # 7. Persist transformation-impact summaries
        # ======================================================================

        transformation_impacts: list[TransformationImpact] = []

        transformation_detail_paths: list[Path] = []

        try:
            for observation in transformation_result.evidence:
                impact = TransformationImpact(
                    pipeline_run_id=pipeline_run_id,
                    dataset_id=dataset_id,
                    dataset_file_id=str(bronze_file_id),
                    bronze_run_id=bronze_run_id,
                    silver_run_id=run_id,
                    silver_build_id=silver_build_id,
                    table_key=table_key,
                    operation=observation.operation,
                    column_name=observation.column_name,
                    before_value=observation.before_value,
                    after_value=observation.after_value,
                    affected_row_count=observation.affected_row_count,
                    transformation_impact_id=transformation_impact_ids.new_transformation_impact_id(),
                    recorded_at=silver_processed_at,
                    partition_key=partition_key,
                    partition_value=partition_value,
                    version_period=version_period.value,
                    contract_hash=contract_meta.get("contract_hash"),
                    meta={
                        **observation.meta,
                        "step_id": step_id,
                        "source_file_name": source_file_name,
                        "record_details": observation.record_details,
                    },
                )

                if (
                    isinstance(observation, TransformationObservation)
                    and observation.record_details
                ):
                    details_destination = silver_store.transformation_details_path(
                        artifact=artifact_ref,
                        transformation_impact_id=impact.transformation_impact_id,
                    )

                    details_artifact = write_transformation_details(
                        destination=details_destination,
                        transformation_impact_id=impact.transformation_impact_id,
                        observation=observation,
                        pipeline_run_id=pipeline_run_id,
                        dataset_id=dataset_id,
                        dataset_file_id=str(bronze_file_id),
                        bronze_run_id=bronze_run_id,
                        silver_run_id=run_id,
                        silver_build_id=silver_build_id,
                        table_key=table_key,
                        source_file_name=source_file_name,
                        partition_key=partition_key,
                        partition_value=partition_value,
                        version_period=version_period.value,
                        contract_hash=contract_meta.get("contract_hash"),
                    )

                    transformation_detail_paths.append(details_artifact.path)

                    impact = replace(
                        impact,
                        details_path=silver_store.relative_path(details_artifact.path),
                        details_hash=details_artifact.sha256,
                        details_row_count=details_artifact.row_count,
                    )

                transformation_impacts.append(impact)

            if transformation_impacts:
                transformation_impact_store.insert_many(transformation_impacts)

                logger.info(
                    "Recorded %d transformation impacts "
                    "and %d detail files for "
                    "dataset_id=%s table_key=%s",
                    len(transformation_impacts),
                    len(transformation_detail_paths),
                    dataset_id,
                    table_key,
                )

        except Exception:
            for details_path in transformation_detail_paths:
                details_path.unlink(missing_ok=True)

            raise
        # ==============================================================================
        # 8. Log telemetry
        # ==============================================================================

        ctx.count_success(1)
        ctx.set_finish_meta(
            ExecutionStepMeta(
                table_key=table_key,
                bronze_run_id=bronze_run_id,
                silver_run_id=run_id,
                silver_build_id=silver_build_id,
                version_period=version_period.value.isoformat(),
                partition_key=partition_key,
                partition_value=partition_value,
                input_row_count=input_row_count,
                output_row_count=output_row_count,
                input_column_count=input_column_count,
                output_column_count=output_column_count,
                input_file_count=len(input_files),
                output_file_count=len(saved_paths),
                input_byte_count=sum(path.stat().st_size for path in input_files),
                output_byte_count=sum(path.stat().st_size for path in saved_paths),
                extra={
                    "version_period_grain": version_period.grain,
                    "version_period_source": version_period.source,
                    "published_columns": canonical_columns,
                    "input_file": input_file_path.name,
                    "saved_files": [silver_store.relative_path(path) for path in saved_paths],
                    "saved_formats": target_formats,
                    "preview_file": silver_store.relative_path(preview_path),
                    "preview_row_count": min(10, output_row_count),
                    "transformation_impact_count": len(transformation_impacts),
                    "transformation_detail_file_count": len(transformation_detail_paths),
                    "transformation_detail_row_count": sum(
                        impact.details_row_count or 0 for impact in transformation_impacts
                    ),
                    **pre_quality.to_meta(),
                    **post_quality.to_meta(),
                },
            ).merged_with(_contract_execution_meta(contract_meta))
        )

        logger.info(
            "done: rows=%d cols=%d saved_formats=%s",
            output_row_count,
            output_column_count,
            target_formats,
        )

        return SilverTableBuildResult(
            staged_paths=tuple(staged_paths), fingerprint=table_fingerprint
        )


def _select_canonical_columns(
    table: pd.DataFrame, *, canonical_columns: list[str], table_key: str
) -> pd.DataFrame:
    """Select the contract-defined public columns in their canonical order."""
    missing_columns = [column for column in canonical_columns if column not in table.columns]
    if missing_columns:
        raise RuntimeError(
            f"Silver table {table_key!r} is missing canonical columns: {missing_columns}"
        )

    return table.loc[:, canonical_columns].copy()


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)

    return value.astimezone(UTC).isoformat()


def _preview_date_columns(table_cfg: dict[str, Any]) -> set[str]:
    date_columns: set[str] = set()

    for source_name, spec in table_cfg.get("columns", {}).items():
        if not isinstance(spec, dict):
            continue

        if spec.get("cast_to") != "date":
            continue

        date_columns.add(str(spec.get("rename_to") or source_name))

    return date_columns


def _write_preview_json(
    table: pd.DataFrame, target_path: Path, *, table_cfg: dict[str, Any], preview_rows: int = 10
) -> Path:
    preview_path = target_path.with_name(f"{target_path.name}_preview.json")
    preview_path.parent.mkdir(parents=True, exist_ok=True)

    preview_columns = [str(column) for column in table_cfg["canonical_order"]]

    preview = table.loc[:, preview_columns].head(preview_rows).copy()

    for column in _preview_date_columns(table_cfg):
        if column not in preview.columns:
            continue

        parsed = pd.to_datetime(preview[column], errors="coerce")

        formatted_values: list[str | None] = [
            (value.strftime("%Y-%m-%d") if pd.notna(value) else None) for value in parsed
        ]

        preview[column] = formatted_values

    payload = {
        "columns": [str(column) for column in preview.columns],
        "rows": json.loads(preview.to_json(orient="records", date_format="iso")),
    }

    atomic_write_text(preview_path, json.dumps(payload, indent=2, ensure_ascii=False))

    return preview_path
