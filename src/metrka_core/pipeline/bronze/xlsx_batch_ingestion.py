"""Persistence preparation for assembled XLSX Bronze batches."""

from __future__ import annotations

from datetime import datetime

from metrka_core.metadata.file_marshal_models import MarshaledFile
from metrka_core.pipeline.bronze.xlsx_batch_preparation import PreparedXlsxSourceBatch


def build_xlsx_batch_marshaled_file(
    *,
    prepared: PreparedXlsxSourceBatch,
    dataset_id: str,
    dataset_file_id: str,
    ingested_at: datetime,
) -> MarshaledFile:
    """Build one FileMarshal record for an assembled XLSX batch."""
    if len(prepared.source_urls) != 1:
        raise ValueError("XLSX source batch must have exactly one source URL")

    source_file_name = prepared.output_path.name

    return MarshaledFile(
        dataset_file_id=dataset_file_id,
        dataset_id=dataset_id,
        source_url=prepared.source_urls[0],
        source_file_name=source_file_name,
        original_source_file_name=source_file_name,
        source_hash=prepared.fingerprint.sha256,
        file_size=sum(member.size_bytes for member in prepared.fingerprint.members),
        ingestion_timestamp=ingested_at,
        source_last_modified=prepared.source_last_modified,
        row_count_raw=prepared.row_count,
        column_count_raw=prepared.column_count,
        artifact_role=prepared.artifact_role,
    )
