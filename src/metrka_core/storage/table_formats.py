"""Supported table output formats and their file extensions."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType


class TableFormat(StrEnum):
    """A table format that metrka-core can write."""

    CSV = "csv"
    PARQUET = "parquet"


TABLE_FORMAT_EXTENSIONS: Mapping[TableFormat, str] = MappingProxyType(
    {TableFormat.CSV: ".csv", TableFormat.PARQUET: ".parquet"}
)

SUPPORTED_TABLE_FORMATS = frozenset(table_format.value for table_format in TableFormat)


def normalize_table_format(value: str) -> TableFormat:
    """Normalize and validate one configured table format."""

    normalized = value.strip().lower()

    try:
        return TableFormat(normalized)
    except ValueError as error:
        raise ValueError(
            f"Unsupported format: {normalized!r}. Supported: {sorted(SUPPORTED_TABLE_FORMATS)}"
        ) from error
