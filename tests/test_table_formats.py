from __future__ import annotations

import pytest

from metrka_core.storage.table_formats import (
    SUPPORTED_TABLE_FORMATS,
    TABLE_FORMAT_EXTENSIONS,
    TableFormat,
    normalize_table_format,
)


def test_table_format_registry_is_complete() -> None:
    assert frozenset({"csv", "parquet"}) == SUPPORTED_TABLE_FORMATS
    assert TABLE_FORMAT_EXTENSIONS == {TableFormat.CSV: ".csv", TableFormat.PARQUET: ".parquet"}


def test_normalize_table_format_accepts_case_and_whitespace() -> None:
    assert normalize_table_format(" CSV ") is TableFormat.CSV


def test_normalize_table_format_rejects_unknown_format() -> None:
    with pytest.raises(
        ValueError, match=r"Unsupported format: 'json'. Supported: \['csv', 'parquet'\]"
    ):
        normalize_table_format("json")
