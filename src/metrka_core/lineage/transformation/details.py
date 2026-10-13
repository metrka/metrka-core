"""Write detailed row-level transformation evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from metrka_core.lineage.transformation.models import (
    TransformationDetailRow,
    TransformationObservation,
)
from metrka_core.storage.atomic_writes import atomic_write
from metrka_core.storage.checksums import sha256_file
from metrka_core.values import canonical_tagged_scalar

TRANSFORMATION_DETAILS_SCHEMA_ID = "metrka.transformation-details.parquet.v1"
TRANSFORMATION_DETAILS_VALUE_ENCODING = "metrka.tagged-scalar-json.v1"

_SCHEMA_ID_METADATA_KEY = b"metrka.schema_id"
_VALUE_ENCODING_METADATA_KEY = b"metrka.value_encoding"
_CONTEXT_COLUMNS_METADATA_KEY = b"metrka.context_columns"
_CONTRACT_HASH_METADATA_KEY = b"metrka.contract_hash"


@dataclass(frozen=True, slots=True)
class TransformationDetailsArtifact:
    """Metadata describing one detailed impact Parquet file."""

    path: Path
    sha256: str
    row_count: int
    schema_id: str = TRANSFORMATION_DETAILS_SCHEMA_ID
    value_encoding: str = TRANSFORMATION_DETAILS_VALUE_ENCODING


def write_transformation_details(
    *,
    destination: Path,
    transformation_impact_id: str,
    observation: TransformationObservation,
    pipeline_run_id: str,
    dataset_id: str,
    dataset_file_id: str,
    bronze_run_id: str,
    silver_run_id: str,
    silver_build_id: str,
    table_key: str,
    source_file_name: str,
    partition_key: str,
    partition_value: str,
    version_period: date,
    contract_hash: str | None,
) -> TransformationDetailsArtifact:
    """
    Write detailed evidence for one explicitly tracked transformation.

    One Parquet row represents one affected source data row. The Arrow
    schema and scalar encoding are controlled by metrka-core rather than
    inferred from the values present in a particular run.
    """

    if not observation.record_details:
        raise ValueError("Cannot write details when record_details is disabled")

    if not observation.detail_rows:
        raise ValueError("Cannot write an empty transformation-details file")

    _validate_detail_contexts(observation)
    context_columns = tuple(sorted(observation.detail_columns))
    schema = _transformation_details_schema(
        context_columns=context_columns, contract_hash=contract_hash
    )
    records = [
        _detail_record(
            transformation_impact_id=transformation_impact_id,
            observation=observation,
            detail=detail,
            context_columns=context_columns,
            pipeline_run_id=pipeline_run_id,
            dataset_id=dataset_id,
            dataset_file_id=dataset_file_id,
            bronze_run_id=bronze_run_id,
            silver_run_id=silver_run_id,
            silver_build_id=silver_build_id,
            table_key=table_key,
            source_file_name=source_file_name,
            partition_key=partition_key,
            partition_value=partition_value,
            version_period=version_period,
            contract_hash=contract_hash,
        )
        for detail in observation.detail_rows
    ]
    details_table = pa.Table.from_pylist(records, schema=schema)

    def write_parquet(temporary_path: Path) -> None:
        pq.write_table(
            details_table,
            temporary_path,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
        )

    atomic_write(destination, write_parquet)

    return TransformationDetailsArtifact(
        path=destination, sha256=sha256_file(destination), row_count=details_table.num_rows
    )


def _transformation_details_schema(
    *, context_columns: tuple[str, ...], contract_hash: str | None
) -> pa.Schema:
    fields = [
        pa.field("transformation_impact_id", pa.string(), nullable=False),
        pa.field("pipeline_run_id", pa.string(), nullable=False),
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("dataset_file_id", pa.string(), nullable=False),
        pa.field("bronze_run_id", pa.string(), nullable=False),
        pa.field("silver_run_id", pa.string(), nullable=False),
        pa.field("silver_build_id", pa.string(), nullable=False),
        pa.field("table_key", pa.string(), nullable=False),
        pa.field("source_file_name", pa.string(), nullable=False),
        pa.field("partition_key", pa.string(), nullable=False),
        pa.field("partition_value", pa.string(), nullable=False),
        pa.field("version_period", pa.date32(), nullable=False),
        pa.field("contract_hash", pa.string()),
        pa.field("operation", pa.string(), nullable=False),
        pa.field("column_name", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
    ]
    fields.extend(
        pa.field(f"context__{column_name}", pa.string(), nullable=False)
        for column_name in context_columns
    )
    fields.extend(
        (
            pa.field("before_value", pa.string(), nullable=False),
            pa.field("after_value", pa.string(), nullable=False),
        )
    )

    metadata: dict[bytes, bytes] = {
        _SCHEMA_ID_METADATA_KEY: TRANSFORMATION_DETAILS_SCHEMA_ID.encode("utf-8"),
        _VALUE_ENCODING_METADATA_KEY: TRANSFORMATION_DETAILS_VALUE_ENCODING.encode("utf-8"),
        _CONTEXT_COLUMNS_METADATA_KEY: _canonical_json(list(context_columns)).encode("utf-8"),
    }

    if contract_hash is not None:
        metadata[_CONTRACT_HASH_METADATA_KEY] = contract_hash.encode("utf-8")

    return pa.schema(fields, metadata=metadata)


def _validate_detail_contexts(observation: TransformationObservation) -> None:
    expected = set(observation.detail_columns)

    for detail in observation.detail_rows:
        actual = set(detail.context)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)

        if missing or unexpected:
            raise ValueError(
                "Transformation detail row context does not match detail_columns: "
                f"missing={missing}, unexpected={unexpected}"
            )


def _detail_record(
    *,
    transformation_impact_id: str,
    observation: TransformationObservation,
    detail: TransformationDetailRow,
    context_columns: tuple[str, ...],
    pipeline_run_id: str,
    dataset_id: str,
    dataset_file_id: str,
    bronze_run_id: str,
    silver_run_id: str,
    silver_build_id: str,
    table_key: str,
    source_file_name: str,
    partition_key: str,
    partition_value: str,
    version_period: date,
    contract_hash: str | None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "transformation_impact_id": transformation_impact_id,
        "pipeline_run_id": pipeline_run_id,
        "dataset_id": dataset_id,
        "dataset_file_id": dataset_file_id,
        "bronze_run_id": bronze_run_id,
        "silver_run_id": silver_run_id,
        "silver_build_id": silver_build_id,
        "table_key": table_key,
        "source_file_name": source_file_name,
        "partition_key": partition_key,
        "partition_value": partition_value,
        "version_period": version_period,
        "contract_hash": contract_hash,
        "operation": observation.operation,
        "column_name": observation.column_name,
        "source_row_number": detail.source_row_number,
        "before_value": _encode_evidence_value(observation.before_value),
        "after_value": _encode_evidence_value(observation.after_value),
    }

    for column_name in context_columns:
        record[f"context__{column_name}"] = _encode_evidence_value(detail.context[column_name])

    return record


def _encode_evidence_value(value: object) -> str:
    return _canonical_json(canonical_tagged_scalar(value))


def _canonical_json(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
