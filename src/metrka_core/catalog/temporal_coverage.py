"""Calculate publication temporal coverage from committed Silver tables."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from numbers import Integral, Real
from pathlib import Path
from typing import Any

from metrka_core.catalog.highlights import _read_column, _select_table_files, only_table_key

SUPPORTED_TEMPORAL_COVERAGE_GRAINS = {"year"}


@dataclass(frozen=True, slots=True)
class TemporalCoverageSpec:
    """Resolved instruction for deriving coverage from one Silver column."""

    table_key: str | None
    column: str
    grain: str

    def to_config_dict(self) -> dict[str, str | None]:
        """Return a stable representation suitable for processing fingerprints."""

        return {"table": self.table_key, "column": self.column, "grain": self.grain}


def parse_temporal_coverage_spec(raw: object) -> TemporalCoverageSpec:
    """Validate one ``silver.temporal_coverage`` mapping."""

    if not isinstance(raw, Mapping):
        raise ValueError("silver.temporal_coverage must be a mapping")

    unknown_keys = sorted(set(raw) - {"table", "column", "grain"})
    if unknown_keys:
        raise ValueError(f"silver.temporal_coverage contains unsupported keys: {unknown_keys}")

    table_key = None if raw.get("table") is None else _required_text(raw, "table")
    column = _required_text(raw, "column")
    grain = _required_text(raw, "grain").lower()

    if grain not in SUPPORTED_TEMPORAL_COVERAGE_GRAINS:
        raise ValueError(
            "silver.temporal_coverage.grain must be one of "
            f"{sorted(SUPPORTED_TEMPORAL_COVERAGE_GRAINS)}"
        )

    return TemporalCoverageSpec(table_key=table_key, column=column, grain=grain)


def calculate_temporal_coverage(
    *, spec: TemporalCoverageSpec, data_files: list[Path], tables_root: Path
) -> dict[str, Any]:
    """Derive exact, compacted temporal periods from committed Silver values."""

    table_key = spec.table_key or only_table_key(data_files=data_files, tables_root=tables_root)
    table_files = _select_table_files(
        data_files=data_files, tables_root=tables_root, table_key=table_key
    )
    values = _read_column(table_files=table_files, column=spec.column).dropna()

    if values.empty:
        raise ValueError(
            f"Temporal coverage source contains no non-null values: {table_key}.{spec.column}"
        )

    if spec.grain != "year":
        raise ValueError(f"Unsupported temporal coverage grain: {spec.grain!r}")

    years = sorted({_year_value(value) for value in values.tolist()})

    return {
        "grain": spec.grain,
        "source": {"table_key": table_key, "column": spec.column},
        "periods": _compact_years(years),
        "minimum": years[0],
        "maximum": years[-1],
        "distinct_value_count": len(years),
    }


def _compact_years(years: list[int]) -> list[dict[str, int]]:
    periods: list[dict[str, int]] = []
    start = years[0]
    end = start

    for year in years[1:]:
        if year == end + 1:
            end = year
            continue

        periods.append({"start": start, "end": end})
        start = year
        end = year

    periods.append({"start": start, "end": end})
    return periods


def _year_value(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("Temporal coverage year values must not be booleans")

    year: int

    if isinstance(value, Integral):
        year = int(value)
    elif isinstance(value, Real):
        numeric = float(value)
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(f"Temporal coverage contains a non-integer year: {value!r}")
        year = int(numeric)
    elif isinstance(value, str) and value.strip().isdigit():
        year = int(value.strip())
    else:
        raise ValueError(f"Temporal coverage contains an invalid year: {value!r}")

    if year < 1 or year > 9999:
        raise ValueError(f"Temporal coverage year is outside 1..9999: {year}")

    return year


def _required_text(raw: Mapping[object, object], field_name: str) -> str:
    value = raw.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"silver.temporal_coverage.{field_name} must be a non-empty string")

    return value.strip()
