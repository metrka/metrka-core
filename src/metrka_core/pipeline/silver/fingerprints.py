"""Canonical fingerprints for logical Silver output."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.values.canonical import canonical_fingerprint_scalar as _canonical_scalar

SILVER_FINGERPRINT_VERSION = 1

LOGICAL_DATA_HASH_ALGORITHM = "metrka.logical-data.sha256.sorted-rows.v1"

SCHEMA_HASH_ALGORITHM = "metrka.logical-schema.sha256.canonical-types.v1"


@dataclass(frozen=True)
class SilverTableFingerprint:
    """Canonical identity of one logical Silver table."""

    table_key: str
    logical_data_hash: str
    schema_hash: str
    row_count: int
    column_count: int
    fingerprint_version: int = SILVER_FINGERPRINT_VERSION
    logical_hash_algorithm: str = LOGICAL_DATA_HASH_ALGORITHM
    schema_hash_algorithm: str = SCHEMA_HASH_ALGORITHM

    def __post_init__(self) -> None:
        if not self.table_key.strip():
            raise ValueError("SilverTableFingerprint.table_key must not be empty")

        for field_name, value in {
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
        }.items():
            if len(value) != 64:
                raise ValueError(f"{field_name} must be a SHA-256 hash")

        if self.row_count < 0:
            raise ValueError("row_count must not be negative")

        if self.column_count < 0:
            raise ValueError("column_count must not be negative")

        if not self.logical_hash_algorithm.strip():
            raise ValueError("logical_hash_algorithm must not be empty")

        if not self.schema_hash_algorithm.strip():
            raise ValueError("schema_hash_algorithm must not be empty")


@dataclass(frozen=True)
class SilverDatasetFingerprint:
    """Canonical identity of all logical tables in one Silver build."""

    logical_data_hash: str
    schema_hash: str
    tables: tuple[SilverTableFingerprint, ...]
    fingerprint_version: int = SILVER_FINGERPRINT_VERSION
    logical_hash_algorithm: str = LOGICAL_DATA_HASH_ALGORITHM
    schema_hash_algorithm: str = SCHEMA_HASH_ALGORITHM

    def __post_init__(self) -> None:
        for field_name, value in {
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
        }.items():
            if len(value) != 64:
                raise ValueError(f"{field_name} must be a SHA-256 hash")

        table_keys = [table.table_key for table in self.tables]

        if len(table_keys) != len(set(table_keys)):
            raise ValueError("Dataset fingerprint contains duplicate table keys")

        if not self.logical_hash_algorithm.strip():
            raise ValueError("logical_hash_algorithm must not be empty")

        if not self.schema_hash_algorithm.strip():
            raise ValueError("schema_hash_algorithm must not be empty")

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "fingerprint_version": self.fingerprint_version,
            "logical_data_algorithm": self.logical_hash_algorithm,
            "schema_algorithm": self.schema_hash_algorithm,
            "logical_data_hash": self.logical_data_hash,
            "schema_hash": self.schema_hash,
            "tables": [
                {
                    "table_key": table.table_key,
                    "logical_data_hash": table.logical_data_hash,
                    "schema_hash": table.schema_hash,
                    "row_count": table.row_count,
                    "column_count": table.column_count,
                }
                for table in self.tables
            ],
        }


@dataclass(frozen=True)
class SilverTableBuildResult:
    """Files and logical fingerprint produced by one table build."""

    staged_paths: tuple[Path, ...]
    fingerprint: SilverTableFingerprint


def fingerprint_silver_table(*, table_key: str, table: pd.DataFrame) -> SilverTableFingerprint:
    """
    Fingerprint transformed business data before Silver metadata columns.

    Row ordering does not affect logical_data_hash. Duplicate rows still
    affect it because every row digest is included.
    """

    if not table_key.strip():
        raise ValueError("table_key must not be empty")

    if not table.columns.is_unique:
        raise ValueError(f"Silver table {table_key!r} has duplicate columns")

    if not all(isinstance(column, str) for column in table.columns):
        raise TypeError("Silver fingerprint requires string column names")

    columns = tuple(table.columns)

    schema_payload = {
        "algorithm": SCHEMA_HASH_ALGORITHM,
        "table_key": table_key,
        "columns": [
            {"name": column, "type": _canonical_dtype(table[column])} for column in columns
        ],
    }

    schema_hash = _sha256_json(schema_payload)

    row_digests: list[bytes] = []

    for row in table.itertuples(index=False, name=None):
        row_payload = [_canonical_scalar(value) for value in row]

        row_digests.append(hashlib.sha256(_canonical_json_bytes(row_payload)).digest())

    data_hasher = hashlib.sha256()

    data_hasher.update(
        _canonical_json_bytes(
            {
                "algorithm": LOGICAL_DATA_HASH_ALGORITHM,
                "table_key": table_key,
                "columns": list(columns),
                "row_count": len(row_digests),
            }
        )
    )

    for row_digest in sorted(row_digests):
        data_hasher.update(row_digest)

    return SilverTableFingerprint(
        table_key=table_key,
        logical_data_hash=data_hasher.hexdigest(),
        schema_hash=schema_hash,
        row_count=len(table),
        column_count=len(columns),
    )


def combine_silver_table_fingerprints(
    tables: list[SilverTableFingerprint],
) -> SilverDatasetFingerprint:
    """Combine table fingerprints into one dataset-build fingerprint."""

    if not tables:
        raise ValueError("A Silver dataset fingerprint requires at least one table")

    ordered_tables = tuple(sorted(tables, key=lambda table: table.table_key))

    table_keys = [table.table_key for table in ordered_tables]

    if len(table_keys) != len(set(table_keys)):
        raise ValueError("Cannot combine duplicate Silver table keys")

    for table in ordered_tables:
        if table.fingerprint_version != SILVER_FINGERPRINT_VERSION:
            raise ValueError(
                f"Table {table.table_key!r} uses fingerprint version "
                f"{table.fingerprint_version}; expected {SILVER_FINGERPRINT_VERSION}"
            )

        if table.logical_hash_algorithm != LOGICAL_DATA_HASH_ALGORITHM:
            raise ValueError(f"Table {table.table_key!r} uses an unexpected logical hash algorithm")

        if table.schema_hash_algorithm != SCHEMA_HASH_ALGORITHM:
            raise ValueError(f"Table {table.table_key!r} uses an unexpected schema hash algorithm")

    logical_data_hash = _sha256_json(
        {
            "fingerprint_version": SILVER_FINGERPRINT_VERSION,
            "tables": [
                {
                    "table_key": table.table_key,
                    "logical_data_hash": table.logical_data_hash,
                    "row_count": table.row_count,
                }
                for table in ordered_tables
            ],
        }
    )

    schema_hash = _sha256_json(
        {
            "fingerprint_version": SILVER_FINGERPRINT_VERSION,
            "tables": [
                {"table_key": table.table_key, "schema_hash": table.schema_hash}
                for table in ordered_tables
            ],
        }
    )

    return SilverDatasetFingerprint(
        logical_data_hash=logical_data_hash, schema_hash=schema_hash, tables=ordered_tables
    )


def _canonical_dtype(series: pd.Series) -> object:
    """Map a pandas dtype to a Metrka-owned logical schema type."""

    dtype = series.dtype

    if isinstance(dtype, pd.CategoricalDtype):
        return {
            "kind": "category",
            "ordered": dtype.ordered,
            "categories": [_canonical_scalar(value) for value in dtype.categories.tolist()],
        }

    if isinstance(dtype, pd.ArrowDtype):
        arrow_dtype = dtype.pyarrow_dtype

        if pa.types.is_date32(arrow_dtype) or pa.types.is_date64(arrow_dtype):
            return "date"

        if pa.types.is_decimal(arrow_dtype):
            precision = getattr(arrow_dtype, "precision", None)
            scale = getattr(arrow_dtype, "scale", None)

            if not isinstance(precision, int) or not isinstance(scale, int):
                raise TypeError(f"Invalid Arrow decimal dtype in Silver fingerprint: {dtype}")

            return {"kind": "decimal", "precision": precision, "scale": scale}

    if pd.api.types.is_object_dtype(dtype):
        raise TypeError(
            f"Silver output column {series.name!r} has ambiguous object dtype. "
            "Cast it explicitly in the data contract or post-transform hook."
        )

    if pd.api.types.is_datetime64_any_dtype(dtype):
        timezone = getattr(dtype, "tz", None)
        return {"kind": "datetime", "timezone": str(timezone) if timezone else None}

    if pd.api.types.is_timedelta64_dtype(dtype):
        return "duration"

    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"

    if pd.api.types.is_integer_dtype(dtype):
        return "integer"

    if pd.api.types.is_float_dtype(dtype):
        return "float"

    if pd.api.types.is_string_dtype(dtype):
        return "string"

    raise TypeError(f"Unmapped dtype in Silver fingerprint: {dtype}")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()
