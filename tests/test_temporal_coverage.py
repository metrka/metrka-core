from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metrka_core.catalog import temporal_coverage
from metrka_core.catalog.temporal_coverage import (
    TemporalCoverageSpec,
    calculate_temporal_coverage,
    parse_temporal_coverage_spec,
)


def test_calculates_compacted_year_periods_from_silver_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    table_file = tmp_path / "facts" / "data.parquet"
    table_file.parent.mkdir(parents=True)
    table_file.touch()

    monkeypatch.setattr(
        temporal_coverage,
        "_read_column",
        lambda **_kwargs: pd.Series([2025, 2019, 2018, 2020, 2023, 2022, 2025, None]),
    )

    result = calculate_temporal_coverage(
        spec=TemporalCoverageSpec(table_key="facts", column="reporting_year", grain="year"),
        data_files=[table_file],
        tables_root=tmp_path,
    )

    assert result == {
        "grain": "year",
        "source": {"table_key": "facts", "column": "reporting_year"},
        "periods": [
            {"start": 2018, "end": 2020},
            {"start": 2022, "end": 2023},
            {"start": 2025, "end": 2025},
        ],
        "minimum": 2018,
        "maximum": 2025,
        "distinct_value_count": 6,
    }


def test_rejects_invalid_year_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    table_file = tmp_path / "facts" / "data.parquet"
    table_file.parent.mkdir(parents=True)
    table_file.touch()
    monkeypatch.setattr(
        temporal_coverage, "_read_column", lambda **_kwargs: pd.Series([2024, "not-a-year"])
    )

    with pytest.raises(ValueError, match="invalid year"):
        calculate_temporal_coverage(
            spec=TemporalCoverageSpec("facts", "reporting_year", "year"),
            data_files=[table_file],
            tables_root=tmp_path,
        )


def test_parses_supported_temporal_coverage_spec() -> None:
    spec = parse_temporal_coverage_spec(
        {"table": "facts", "column": "reporting_year", "grain": "year"}
    )

    assert spec.to_config_dict() == {"table": "facts", "column": "reporting_year", "grain": "year"}


def test_rejects_unsupported_temporal_coverage_grain() -> None:
    with pytest.raises(ValueError, match="grain must be one of"):
        parse_temporal_coverage_spec(
            {"table": "facts", "column": "reported_at", "grain": "quarter"}
        )
