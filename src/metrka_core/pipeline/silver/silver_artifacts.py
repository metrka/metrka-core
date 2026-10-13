"""
Silver promotion artifacts.

The orchestrator writes these only after staged Silver files are committed to the
durable table layout. Manifests are the immutable snapshot receipt; views are
Postgres-compatible SQL file-catalog views derived from the manifest.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from metrka_core.catalog.highlights import calculate_catalog_highlights
from metrka_core.catalog.temporal_coverage import TemporalCoverageSpec, calculate_temporal_coverage
from metrka_core.metadata.postgres_contract_metadata import PostgresContractMetadataStore
from metrka_core.pipeline.provenance import CodeProvenance
from metrka_core.pipeline.silver.artifact_models import SilverBuildRef
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.fingerprints import SilverDatasetFingerprint
from metrka_core.pipeline.silver.version_period import VersionPeriod
from metrka_core.storage.checksums import sha256_checksum
from metrka_core.storage.contract_store import (
    ContractSnapshotPaths,
    ContractSnapshotRef,
    ContractSnapshotStore,
)
from metrka_core.transform.profiles import ProfileError, load_contract

SUPPORTED_VIEW_FORMATS = {".csv", ".parquet"}


def contract_snapshot_metadata(
    *, contract_store: ContractSnapshotStore, dataset_id: str, contract_path: Path
) -> dict[str, str]:
    """Build metadata for a content-addressed contract snapshot."""

    try:
        contract = load_contract(contract_path)
    except (yaml.YAMLError, ProfileError):
        contract = {}

    contract_config_meta = contract.get("meta", {}) if isinstance(contract, dict) else {}

    contract_version = (
        contract_config_meta.get("version", "") if isinstance(contract_config_meta, dict) else ""
    )

    # The snapshot carries the definitions the contract uses, so they are part of its identity.
    contract_hash = hashlib.sha256(contract_store.published_bytes(contract_path)).hexdigest()

    snapshot_ref = ContractSnapshotRef(dataset_id=dataset_id, sha256_hash=contract_hash)

    snapshot_paths = contract_store.snapshot_paths(snapshot=snapshot_ref)

    return {
        "contract_hash": contract_hash,
        "contract_name": contract_path.name,
        "contract_path": contract_store.definition_relative_path(contract_path),
        "contract_version": str(contract_version),
        "contract_snapshot_yaml_path": contract_store.snapshot_relative_path(
            snapshot_paths.yaml_path
        ),
        "contract_snapshot_json_path": contract_store.snapshot_relative_path(
            snapshot_paths.json_path
        ),
    }


def snapshot_contract(
    *, contract_store: ContractSnapshotStore, dataset_id: str, contract_path: Path
) -> ContractSnapshotPaths:
    """Persist one validated immutable contract snapshot."""

    published_bytes = contract_store.published_bytes(contract_path)

    return contract_store.write_snapshot(
        snapshot=ContractSnapshotRef(
            dataset_id=dataset_id, sha256_hash=hashlib.sha256(published_bytes).hexdigest()
        ),
        yaml_bytes=published_bytes,
        json_payload=yaml.safe_load(published_bytes),
    )


def register_contract_snapshot(
    *,
    contract_store: ContractSnapshotStore,
    contract_metadata_store: PostgresContractMetadataStore,
    dataset_name: str,
    dataset_id: str,
    contract_path: Path,
    contract_meta: dict[str, str],
) -> Path:
    """Persist and register one immutable contract snapshot."""

    snapshot_paths = snapshot_contract(
        contract_store=contract_store, dataset_id=dataset_id, contract_path=contract_path
    )

    contract_metadata_store.register_contract_snapshot(
        {
            **contract_meta,
            "dataset": dataset_name,
            "dataset_id": dataset_id,
            "contract_stem": contract_path.stem,
        }
    )

    return snapshot_paths.yaml_path


def write_silver_manifest(
    *,
    silver_store: SilverArtifactStore,
    dataset_id: str,
    silver_build_id: str,
    engine_release_id: str,
    processing_config_hash: str,
    quality_config_hash: str,
    build_signature: str,
    rebuild_mode: str,
    rebuild_reasons: list[str],
    bronze_file_id: str,
    bronze_run_id: str,
    silver_run_id: str,
    pipeline_run_id: str,
    code_provenance: CodeProvenance,
    created_at: datetime,
    version_period: VersionPeriod,
    partition_key: str,
    partition_value: str,
    contract_path: Path,
    contract_definition_path: str,
    contract_snapshot_path: Path,
    contract_snapshot_data_path: str,
    contract_version: str | None,
    committed_files: list[Path],
    catalog_highlight_specs: list[dict[str, Any]],
    fingerprint: SilverDatasetFingerprint,
    temporal_coverage_spec: TemporalCoverageSpec | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Write an immutable manifest for one completed Silver build."""

    if created_at.utcoffset() is None:
        raise ValueError("Silver manifest created_at must be timezone-aware")

    data_files = [
        file_path
        for file_path in committed_files
        if file_path.suffix.lower() in SUPPORTED_VIEW_FORMATS
    ]

    table_files = [
        _build_file_manifest_entry(
            silver_store=silver_store, dataset_id=dataset_id, file_path=file_path
        )
        for file_path in sorted(data_files)
    ]

    catalog_highlights = calculate_catalog_highlights(
        specs=catalog_highlight_specs, data_files=data_files, tables_root=silver_store.tables_root
    )
    catalog: dict[str, Any] = {"highlights": catalog_highlights}

    if temporal_coverage_spec is not None:
        catalog["temporal_coverage"] = calculate_temporal_coverage(
            spec=temporal_coverage_spec, data_files=data_files, tables_root=silver_store.tables_root
        )

    manifest = {
        "schema_version": 1,
        "artifact_type": "silver_build_manifest",
        "dataset_id": dataset_id,
        "silver_build_id": silver_build_id,
        "layer": "silver",
        "build": {
            "signature": build_signature,
            "engine_release_id": engine_release_id,
            "processing_config_hash": (processing_config_hash),
            "quality_config_hash": quality_config_hash,
            "rebuild_mode": rebuild_mode,
            "rebuild_reasons": rebuild_reasons,
        },
        "fingerprints": fingerprint.to_manifest_dict(),
        "bronze_file_id": bronze_file_id,
        "bronze_run_id": bronze_run_id,
        "silver_run_id": silver_run_id,
        "provenance": {"pipeline_run_id": pipeline_run_id, "code": code_provenance.to_dict()},
        "version_period": version_period.value.isoformat(),
        "version_period_grain": version_period.grain,
        "version_period_source": version_period.source,
        "partition_key": partition_key,
        "partition_value": partition_value,
        "created_at_utc": created_at.astimezone(UTC).isoformat(),
        "contract": {
            "name": contract_path.name,
            "path": contract_definition_path,
            "snapshot_path": contract_snapshot_data_path,
            "version": contract_version,
            "checksum": sha256_checksum(contract_snapshot_path),
        },
        "table_count": len({entry["table_key"] for entry in table_files}),
        "file_count": len(table_files),
        "tables": table_files,
        "catalog": catalog,
    }

    manifest_path = silver_store.write_manifest(
        build=SilverBuildRef(
            dataset_id=dataset_id,
            partition_key=partition_key,
            partition_value=partition_value,
            silver_build_id=silver_build_id,
        ),
        payload=manifest,
    )

    return manifest_path, manifest


def write_silver_latest_views(
    *, silver_store: SilverArtifactStore, current_manifest: dict[str, Any], publication_id: str
) -> list[Path]:
    """Generate latest-file views from the current publication."""

    dataset_id = _require_manifest_dataset_id(current_manifest)

    if not publication_id.strip():
        raise ValueError("publication_id must not be empty")

    entries_by_table = _group_entries_by_table(_require_manifest_tables(current_manifest))

    if not entries_by_table:
        raise RuntimeError("Current Silver manifest contains no tables")

    written_paths: list[Path] = []

    for table_key in sorted(entries_by_table):
        entries = sorted(entries_by_table[table_key], key=_view_entry_sort_key)

        view_name = _view_name(dataset_id, table_key, "latest_files")

        written_paths.append(
            silver_store.write_latest_view(
                table_key=table_key,
                publication_id=publication_id,
                content=_render_file_catalog_view(view_name, entries),
            )
        )

    return written_paths


def write_silver_history_views(
    *, silver_store: SilverArtifactStore, dataset_id: str, history_entries: list[dict[str, Any]]
) -> list[Path]:
    """Regenerate history views from all active publications."""

    if not dataset_id.strip():
        raise ValueError("History-view dataset_id must not be empty")

    normalized_dataset_id = dataset_id.strip()

    entries_by_table = _group_entries_by_table(history_entries)

    if not entries_by_table:
        raise RuntimeError("Published Silver manifests contain no tables")

    written_paths: list[Path] = []

    for table_key in sorted(entries_by_table):
        entries = sorted(entries_by_table[table_key], key=_view_entry_sort_key)

        view_name = _view_name(normalized_dataset_id, table_key, "history_files")

        written_paths.append(
            silver_store.write_history_view(
                table_key=table_key, content=_render_file_catalog_view(view_name, entries)
            )
        )

    return written_paths


def _require_manifest_dataset_id(manifest: dict[str, Any]) -> str:
    """Return the required normalized manifest dataset ID."""

    dataset_id = manifest.get("dataset_id")

    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ValueError("Silver manifest must contain dataset_id")

    return dataset_id.strip()


def _require_manifest_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validated table entries from a manifest."""

    raw_tables = manifest.get("tables")

    if not isinstance(raw_tables, list):
        raise ValueError("Silver manifest tables must be a list")

    tables: list[dict[str, Any]] = []

    for index, raw_entry in enumerate(raw_tables):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"Silver manifest table entry {index} must be a mapping")

        tables.append({str(key): value for key, value in raw_entry.items()})

    return tables


def _build_file_manifest_entry(
    *, silver_store: SilverArtifactStore, dataset_id: str, file_path: Path
) -> dict[str, Any]:
    relative_path = silver_store.table_relative_path(file_path)
    if len(relative_path.parts) != 4:
        raise ValueError(
            f"Silver table file is not in table/partition/build/file layout: {file_path}"
        )

    table_key = relative_path.parts[0]
    partition_segment = relative_path.parts[1]

    entry_partition_key, entry_partition_value = _split_partition_segment(partition_segment)

    row_count, column_count, columns = _inspect_table_file(file_path)

    return {
        "dataset_id": dataset_id,
        "table_key": table_key,
        "path": silver_store.relative_path(file_path),
        "format": file_path.suffix.lstrip(".").lower(),
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns,
        "size_bytes": file_path.stat().st_size,
        "checksum": sha256_checksum(file_path),
        "partition_key": entry_partition_key,
        "partition_value": entry_partition_value,
        "silver_build_id": _silver_build_id_from_file(file_path),
    }


def _inspect_table_file(file_path: Path) -> tuple[int, int, list[str]]:
    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        df = pd.read_parquet(file_path)
    elif suffix == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        raise ValueError(f"Unsupported Silver view format: {file_path.suffix}")

    return len(df), len(df.columns), [str(column) for column in df.columns]


def _render_file_catalog_view(view_name: str, entries: list[dict[str, Any]]) -> str:
    columns = [
        ("dataset_id", "text"),
        ("table_key", "text"),
        ("partition_key", "text"),
        ("partition_value", "text"),
        ("silver_build_id", "text"),
        ("file_path", "text"),
        ("format", "text"),
        ("row_count", "bigint"),
        ("column_count", "integer"),
        ("size_bytes", "bigint"),
        ("checksum", "text"),
        ("columns_json", "jsonb"),
    ]

    header = [
        "-- Generated by Metrka Silver promotion.",
        "-- Postgres-compatible file-catalog view for promoted Silver table files.",
        f"CREATE OR REPLACE VIEW {_sql_identifier(view_name)} AS",
    ]

    if not entries:
        null_projection = ",\n    ".join(
            f"NULL::{column_type} AS {column_name}" for column_name, column_type in columns
        )
        return "\n".join(header + [f"SELECT\n    {null_projection}\nWHERE FALSE;\n"])

    select_blocks = [_render_entry_select(entry) for entry in entries]
    return "\n".join(header + ["\nUNION ALL\n".join(select_blocks) + ";\n"])


def _render_entry_select(entry: dict[str, Any]) -> str:
    return "\n".join(
        [
            "SELECT",
            f"    {_sql_literal(entry['dataset_id'])}::text AS dataset_id,",
            f"    {_sql_literal(entry['table_key'])}::text AS table_key,",
            f"    {_sql_literal(entry['partition_key'])}::text AS partition_key,",
            f"    {_sql_literal(entry['partition_value'])}::text AS partition_value,",
            f"    {_sql_literal(entry['silver_build_id'])}::text AS silver_build_id,",
            f"    {_sql_literal(entry['path'])}::text AS file_path,",
            f"    {_sql_literal(entry['format'])}::text AS format,",
            f"    {int(entry['row_count'])}::bigint AS row_count,",
            f"    {int(entry['column_count'])}::integer AS column_count,",
            f"    {int(entry['size_bytes'])}::bigint AS size_bytes,",
            f"    {_sql_literal(entry['checksum'])}::text AS checksum,",
            f"    {_sql_literal(json.dumps(entry['columns']))}::jsonb AS columns_json",
        ]
    )


def _group_entries_by_table(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        grouped[str(entry["table_key"])].append(entry)
    return dict(grouped)


def _view_entry_sort_key(entry: dict[str, Any]) -> tuple[str, str, str]:
    """Return deterministic ordering for generated views."""

    return (
        str(entry.get("partition_value", "")),
        str(entry.get("silver_build_id", "")),
        str(entry.get("path", "")),
    )


def _split_partition_segment(partition_segment: str) -> tuple[str, str]:
    if "=" not in partition_segment:
        raise ValueError(f"Invalid Silver partition segment: {partition_segment}")

    partition_key, partition_value = partition_segment.split("=", 1)
    if not partition_key or not partition_value:
        raise ValueError(f"Invalid Silver partition segment: {partition_segment}")

    return partition_key, partition_value


def _silver_build_id_from_file(file_path: Path) -> str:
    build_segment = file_path.parent.name
    prefix = "silver_build_id="

    if not build_segment.startswith(prefix):
        raise ValueError(f"Silver file is not inside a silver_build_id directory: {file_path}")

    silver_build_id = build_segment.removeprefix(prefix)

    if not silver_build_id:
        raise ValueError(f"Silver file contains an empty silver_build_id directory: {file_path}")

    return silver_build_id


def _view_name(dataset_id: str, table_key: str, suffix: str) -> str:
    raw_name = f"silver__{dataset_id}__{table_key}__{suffix}"
    cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", raw_name).strip("_").lower()
    if len(cleaned) <= 63:
        return cleaned

    digest = hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:8]
    return f"{cleaned[:54]}_{digest}"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
