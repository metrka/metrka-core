from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metrka_core.catalog import highlights


def test_shared_columns_are_read_once_without_changing_result_order_or_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    table_file = tmp_path / "facts" / "data.parquet"
    table_file.parent.mkdir(parents=True)
    table_file.touch()
    read_columns: list[str] = []
    selected_tables: list[str] = []
    select_table_files = highlights._select_table_files

    def select_files(*, data_files: list[Path], tables_root: Path, table_key: str) -> list[Path]:
        selected_tables.append(table_key)
        return select_table_files(
            data_files=data_files, tables_root=tables_root, table_key=table_key
        )

    def read_column(*, table_files: list[Path], column: str) -> pd.Series:
        assert table_files == [table_file]
        read_columns.append(column)

        if column == "amount":
            return pd.Series([1, 2, 2, 99, None])

        if column == "region":
            return pd.Series(["north", "south", "north", "west", None])

        raise AssertionError(f"Unexpected test column: {column}")

    monkeypatch.setattr(highlights, "_select_table_files", select_files)
    monkeypatch.setattr(highlights, "_read_column", read_column)

    results = highlights.calculate_catalog_highlights(
        specs=[
            {
                "key": "amount_range",
                "label": "Amount range",
                "calculation": "range",
                "table": "facts",
                "column": "amount",
                "exclude_values": [99],
            },
            {
                "key": "region_count",
                "label": "Region count",
                "calculation": "distinct_count",
                "table": "facts",
                "column": "region",
            },
            {
                "key": "amount_count",
                "label": "Amount count",
                "calculation": "distinct_count",
                "table": "facts",
                "column": "amount",
            },
        ],
        data_files=[table_file],
        tables_root=tmp_path,
    )

    assert selected_tables == ["facts"]
    assert read_columns == ["amount", "region"]
    assert [result["key"] for result in results] == ["amount_range", "region_count", "amount_count"]
    assert results[0]["value"] == {"minimum": 1.0, "maximum": 2.0}
    assert results[1]["value"] == 3
    assert results[2]["value"] == 3


def test_invalid_specifications_fail_before_reading_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def unexpected_read(*, table_files: list[Path], column: str) -> pd.Series:
        raise AssertionError(f"Data should not be read: {table_files}, {column}")

    monkeypatch.setattr(highlights, "_read_column", unexpected_read)

    with pytest.raises(ValueError, match="exclude_values must be a list"):
        highlights.calculate_catalog_highlights(
            specs=[
                {
                    "key": "invalid",
                    "label": "Invalid",
                    "calculation": "range",
                    "table": "facts",
                    "column": "amount",
                    "exclude_values": "99",
                }
            ],
            data_files=[],
            tables_root=tmp_path,
        )
