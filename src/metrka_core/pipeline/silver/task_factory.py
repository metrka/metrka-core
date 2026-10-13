"""Build Silver processing tasks from stream configuration."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from metrka_core.catalog.temporal_coverage import parse_temporal_coverage_spec
from metrka_core.datasets.source_config import SourceConfig
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import parse_pipeline_spec
from metrka_core.pipeline.silver.config_fingerprints import calculate_config_hash
from metrka_core.pipeline.silver.dependencies import SilverProcessDeps
from metrka_core.pipeline.silver.process_models import SilverProcessResult
from metrka_core.pipeline.silver.silver_orchestrator import process_silver_queue
from metrka_core.pipeline.silver.task_models import SilverTaskConfig
from metrka_core.pipeline.silver.version_period import (
    build_version_period_discovery,
    parse_version_period_spec,
)
from metrka_core.storage.config_store import ConfigStore
from metrka_core.storage.table_formats import SUPPORTED_TABLE_FORMATS
from metrka_core.transform.validation import validate_contract_file

logger = logging.getLogger(__name__)


def _parse_output_formats(raw: object, *, stream_name: str) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise RuntimeError(
            f"silver.outputs must be a non-empty list of strings for stream {stream_name}"
        )

    output_formats: list[str] = []

    for value in raw:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(
                f"silver.outputs must be a non-empty list of strings for stream {stream_name}"
            )

        output_formats.append(value.strip().lower())

    unsupported = sorted(set(output_formats) - SUPPORTED_TABLE_FORMATS)

    if unsupported:
        raise RuntimeError(
            f"silver.outputs contains unsupported formats for stream {stream_name}: "
            f"{unsupported}; supported: {sorted(SUPPORTED_TABLE_FORMATS)}"
        )

    return output_formats


def build_silver_tasks(*, source_config: SourceConfig) -> list[SilverTaskConfig]:
    """Build Silver tasks for configured data streams."""

    tasks: list[SilverTaskConfig] = []

    for stream_name, stream in source_config.streams.items():
        if stream.artifact_role != "data":
            continue

        if not stream.yaml_contract_name:
            raise RuntimeError(f"Missing yaml_contract_name for stream {stream_name}")

        silver_config = stream.extra.get("silver")

        if not isinstance(silver_config, dict):
            raise RuntimeError(f"Missing silver configuration for stream {stream_name}")

        partition_key = silver_config.get("partition_by")

        if not isinstance(partition_key, str) or not partition_key.strip():
            raise RuntimeError(f"Missing silver.partition_by for stream {stream_name}")

        if partition_key.strip() != "version_period":
            raise RuntimeError(
                f"silver.partition_by must be 'version_period' "
                f"for stream {stream_name}, found {partition_key!r}"
            )

        input_config = silver_config.get("input", {})

        if not isinstance(input_config, dict):
            raise RuntimeError(f"silver.input must be a mapping for stream {stream_name}")

        input_format = input_config.get("format", "csv")

        if not isinstance(input_format, str) or not input_format.strip():
            raise RuntimeError(
                f"silver.input.format must be a non-empty string for stream {stream_name}"
            )

        version_period_raw = silver_config.get("version_period")

        if version_period_raw is None:
            raise RuntimeError(f"Missing silver.version_period for stream {stream_name}")

        version_period_spec = parse_version_period_spec(version_period_raw, stream_name=stream_name)

        version_period_discovery_func = build_version_period_discovery(
            spec=version_period_spec, input_format=input_format.strip()
        )

        input_options = input_config.get("options", {})

        if not isinstance(input_options, dict):
            raise RuntimeError(f"silver.input.options must be a mapping for stream {stream_name}")

        output_formats = _parse_output_formats(
            silver_config.get("outputs", ["parquet"]), stream_name=stream_name
        )

        temporal_coverage_raw = silver_config.get("temporal_coverage")
        temporal_coverage = None

        if temporal_coverage_raw is not None:
            try:
                temporal_coverage = parse_temporal_coverage_spec(temporal_coverage_raw)
            except ValueError as error:
                raise RuntimeError(
                    f"Invalid silver.temporal_coverage for stream {stream_name}: {error}"
                ) from error

        catalog_config = stream.extra.get("catalog", {})

        if not isinstance(catalog_config, dict):
            raise RuntimeError(f"catalog must be a mapping for stream {stream_name}")

        catalog_highlights = catalog_config.get("highlights", [])

        if not isinstance(catalog_highlights, list) or not all(
            isinstance(highlight, dict) for highlight in catalog_highlights
        ):
            raise RuntimeError(
                f"catalog.highlights must be a list of mappings for stream {stream_name}"
            )

        dataset_id = source_config.dataset_id(stream_name)

        processing_config = {
            "dataset_id": dataset_id,
            "yaml_contract_name": stream.yaml_contract_name,
            "partition_key": partition_key.strip(),
            "version_period": version_period_raw,
            "input": {"format": input_format.strip(), "options": dict(input_options)},
            "outputs": output_formats,
            "catalog_highlights": [dict(highlight) for highlight in catalog_highlights],
        }

        if temporal_coverage is not None:
            processing_config["temporal_coverage"] = temporal_coverage.to_config_dict()

        tasks.append(
            SilverTaskConfig(
                dataset_id=dataset_id,
                yaml_contract_name=stream.yaml_contract_name,
                partition_key=partition_key.strip(),
                version_period_discovery_func=version_period_discovery_func,
                input_format=input_format.strip(),
                input_kwargs=dict(input_options),
                output_formats=output_formats,
                catalog_highlights=[dict(highlight) for highlight in catalog_highlights],
                temporal_coverage=temporal_coverage,
                processing_config_hash=calculate_config_hash(processing_config),
            )
        )

    return tasks


def load_silver_contracts(
    *, source_config: SourceConfig, config_store: ConfigStore
) -> dict[Path, dict[str, Any]]:
    """Validate and load every contract the Silver step will use, keyed by path."""

    pipeline = parse_pipeline_spec(source_config.pipeline)

    if not any(step.action == "silver.process" for step in pipeline.steps):
        return {}

    contracts: dict[Path, dict[str, Any]] = {}

    for task in build_silver_tasks(source_config=source_config):
        path = config_store.path(name=task.yaml_contract_name)

        if path not in contracts:
            contracts[path] = validate_contract_file(path)

    return contracts


def process_configured_silver(
    *,
    runtime: ActionRuntime,
    deps: SilverProcessDeps,
    target_dataset_id: str | None = None,
    force_rebuild: bool = False,
) -> SilverProcessResult:
    """Process Bronze files using configured Silver tasks."""

    if force_rebuild:
        dataset_suffix = (
            f" for dataset_id={target_dataset_id}" if target_dataset_id is not None else ""
        )
        logger.info("Manual Silver force rebuild requested%s.", dataset_suffix)

    tasks = build_silver_tasks(source_config=deps.source_config)

    return process_silver_queue(
        runtime=runtime,
        deps=deps,
        tasks=tasks,
        target_dataset_id=target_dataset_id,
        force_rebuild=force_rebuild,
    )
