"""Prepare multiple landed XLSX assets as one Bronze stream batch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from metrka_core.metadata.artifact import ArtifactRole
from metrka_core.pipeline.bronze.filename_metadata import FilenameMetadataConfig
from metrka_core.pipeline.bronze.source_batch_fingerprint import (
    SourceBatchFingerprint,
    fingerprint_source_batch,
)
from metrka_core.pipeline.bronze.xlsx_row_assembly import (
    BronzeAssemblyConfig,
    write_assembled_xlsx_csv,
)
from metrka_core.pipeline.models import LandedAsset


@dataclass(frozen=True, slots=True)
class PreparedXlsxSourceBatch:
    """One assembled XLSX source batch ready for Bronze registration."""

    stream_name: str
    input_paths: tuple[Path, ...]
    source_capture_ids: tuple[str, ...]
    source_urls: tuple[str, ...]
    artifact_role: ArtifactRole
    source_last_modified: datetime | None
    fingerprint: SourceBatchFingerprint
    output_path: Path
    input_file_count: int
    row_count: int
    column_count: int


def prepare_xlsx_source_batch(
    *,
    assets: tuple[LandedAsset, ...],
    output_dir: Path,
    filename_config: FilenameMetadataConfig,
    assembly_config: BronzeAssemblyConfig,
) -> PreparedXlsxSourceBatch:
    """Validate and combine one stream's XLSX source assets."""
    if not assets:
        raise ValueError("Cannot prepare an empty XLSX source batch")

    stream_names = {asset.stream_name for asset in assets}

    if len(stream_names) != 1:
        raise ValueError("XLSX source batch must contain exactly one stream")

    artifact_roles = {asset.artifact_role for asset in assets}

    if len(artifact_roles) != 1:
        raise ValueError("XLSX source batch must contain one artifact role")

    ordered_assets = tuple(
        sorted(assets, key=lambda asset: (asset.path.name.casefold(), asset.path.as_posix()))
    )

    input_paths = tuple(asset.path for asset in ordered_assets)

    fingerprint = fingerprint_source_batch(input_paths)

    output_path = output_dir / assembly_config.output_filename

    assembly_result = write_assembled_xlsx_csv(
        paths=input_paths,
        output_path=output_path,
        filename_config=filename_config,
        read_config=assembly_config.read_config,
    )

    source_last_modified_values = [
        asset.source_last_modified
        for asset in ordered_assets
        if asset.source_last_modified is not None
    ]

    source_last_modified = max(source_last_modified_values) if source_last_modified_values else None

    return PreparedXlsxSourceBatch(
        stream_name=ordered_assets[0].stream_name,
        input_paths=input_paths,
        source_capture_ids=tuple(sorted({asset.source_capture_id for asset in ordered_assets})),
        source_urls=tuple(sorted({asset.source_url for asset in ordered_assets})),
        artifact_role=ordered_assets[0].artifact_role,
        source_last_modified=source_last_modified,
        fingerprint=fingerprint,
        output_path=assembly_result.output_path,
        input_file_count=assembly_result.input_file_count,
        row_count=assembly_result.row_count,
        column_count=assembly_result.column_count,
    )
