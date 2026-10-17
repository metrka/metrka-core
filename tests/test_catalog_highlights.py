from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from metrka_core.catalog import highlights


def test_range_highlight_serializes_decimal_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    table_file = tmp_path / "prices" / "data.parquet"
    table_file.parent.mkdir(parents=True)
    table_file.touch()

    def read_decimal_column(*, table_files: list[Path], column: str) -> pd.Series:
        assert table_files == [table_file]
        assert column == "amount"
        return pd.Series([Decimal("1.50"), Decimal("2.75")], dtype="object")

    monkeypatch.setattr(highlights, "_read_column", read_decimal_column)

    result = highlights.calculate_catalog_highlights(
        specs=[
            {
                "key": "amount_range",
                "label": "Amount range",
                "calculation": "range",
                "table": "prices",
                "column": "amount",
            }
        ],
        data_files=[table_file],
        tables_root=tmp_path,
    )

    assert result[0]["value"] == {"minimum": "1.50", "maximum": "2.75"}
    assert result[0]["display_value"] == "1.50–2.75"
    json.dumps(result, allow_nan=False)
