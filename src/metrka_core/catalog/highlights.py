"""Calculate human-readable catalog highlights from tabular data files."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from metrka_core.values.canonical import json_scalar

SUPPORTED_CALCULATIONS = {"distinct_count", "range"}


@dataclass(frozen=True, slots=True)
class _CatalogHighlightSpec:
    position: int
    key: str
    label: str
    calculation: str
    table_key: str | None
    column: str
    exclude_values: tuple[Any, ...]


def calculate_catalog_highlights(
    *, specs: list[dict[str, Any]], data_files: list[Path], tables_root: Path
) -> list[dict[str, Any]]:
    """Calculate configured highlights while reading each source column once."""

    parsed_specs = _parse_specs(specs)

    if any(spec.table_key is None for spec in parsed_specs):
        only_table = only_table_key(data_files=data_files, tables_root=tables_root)
        parsed_specs = [
            replace(spec, table_key=spec.table_key or only_table) for spec in parsed_specs
        ]

    specs_by_column = _group_specs_by_column(parsed_specs)
    table_files_by_key: dict[str, list[Path]] = {}
    results_by_position: dict[int, dict[str, Any]] = {}

    for (table_key, column), column_specs in specs_by_column.items():
        if table_key not in table_files_by_key:
            table_files_by_key[table_key] = _select_table_files(
                data_files=data_files, tables_root=tables_root, table_key=table_key
            )

        values = _read_column(table_files=table_files_by_key[table_key], column=column).dropna()

        for spec in column_specs:
            results_by_position[spec.position] = _calculate_highlight(
                spec=spec, source_values=values
            )

    return [results_by_position[position] for position in range(len(parsed_specs))]


def _parse_specs(specs: list[dict[str, Any]]) -> list[_CatalogHighlightSpec]:
    parsed_specs: list[_CatalogHighlightSpec] = []
    seen_keys: set[str] = set()

    for position, spec in enumerate(specs):
        key = _required_text(spec, "key")
        label = _required_text(spec, "label")
        calculation = _required_text(spec, "calculation")
        table_key = _optional_text(spec, "table")
        column = _required_text(spec, "column")

        if key in seen_keys:
            raise ValueError(f"Duplicate catalog highlight key: {key}")

        seen_keys.add(key)

        if calculation not in SUPPORTED_CALCULATIONS:
            raise ValueError(f"Unsupported catalog highlight calculation: {calculation!r}")

        exclude_values = spec.get("exclude_values", [])

        if not isinstance(exclude_values, list):
            raise ValueError(f"catalog highlight {key}: exclude_values must be a list")

        parsed_specs.append(
            _CatalogHighlightSpec(
                position=position,
                key=key,
                label=label,
                calculation=calculation,
                table_key=table_key,
                column=column,
                exclude_values=tuple(exclude_values),
            )
        )

    return parsed_specs


def _group_specs_by_column(
    specs: list[_CatalogHighlightSpec],
) -> dict[tuple[str, str], list[_CatalogHighlightSpec]]:
    grouped: dict[tuple[str, str], list[_CatalogHighlightSpec]] = {}

    for spec in specs:
        if spec.table_key is None:
            raise ValueError(f"catalog highlight {spec.key} has no table")

        grouped.setdefault((spec.table_key, spec.column), []).append(spec)

    return grouped


def _calculate_highlight(
    *, spec: _CatalogHighlightSpec, source_values: pd.Series[Any]
) -> dict[str, Any]:
    values = source_values

    if spec.exclude_values:
        values = values[~values.isin(spec.exclude_values)]

    if values.empty:
        raise ValueError(f"catalog highlight {spec.key}: no values remain after filtering")

    result: dict[str, Any] = {
        "key": spec.key,
        "label": spec.label,
        "calculation": spec.calculation,
        "table_key": spec.table_key,
        "column": spec.column,
    }

    if spec.calculation == "distinct_count":
        value = int(values.nunique(dropna=True))
        result.update({"value": value, "display_value": str(value)})
        return result

    minimum = json_scalar(values.min())
    maximum = json_scalar(values.max())
    display_value = str(minimum) if minimum == maximum else f"{minimum}\N{EN DASH}{maximum}"
    result.update(
        {"value": {"minimum": minimum, "maximum": maximum}, "display_value": display_value}
    )
    return result


def _select_table_files(*, data_files: list[Path], tables_root: Path, table_key: str) -> list[Path]:
    matching_files: list[Path] = []

    for file_path in data_files:
        if not file_path.is_file():
            continue

        relative_path = file_path.resolve().relative_to(tables_root.resolve())

        if relative_path.parts[0] == table_key:
            matching_files.append(file_path)

    for suffix in (".parquet", ".csv"):
        selected = sorted(
            file_path for file_path in matching_files if file_path.suffix.lower() == suffix
        )

        if selected:
            return selected

    raise ValueError(f"No tabular data files found for table {table_key!r}")


def _read_column(*, table_files: list[Path], column: str) -> pd.Series[Any]:
    series_parts: list[pd.Series[Any]] = []

    for file_path in table_files:
        if file_path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(file_path, columns=[column])
        else:
            frame = pd.read_csv(file_path, usecols=[column])

        series_parts.append(frame[column])

    combined: pd.Series[Any] = pd.concat(series_parts, ignore_index=True)
    return combined


def _required_text(spec: dict[str, Any], key: str) -> str:
    value = spec.get(key)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"catalog highlight requires non-empty {key!r}")

    return value.strip()


def _optional_text(spec: dict[str, Any], key: str) -> str | None:
    if spec.get(key) is None:
        return None

    return _required_text(spec, key)


def only_table_key(*, data_files: list[Path], tables_root: Path) -> str:
    """Return the one table a build committed, for settings that omit ``table``."""

    table_keys = {
        file_path.resolve().relative_to(tables_root.resolve()).parts[0] for file_path in data_files
    }

    if len(table_keys) != 1:
        raise ValueError(f"Name the table; this build has tables {sorted(table_keys)}")

    (table_key,) = table_keys
    return table_key
