"""Models and validation for Silver dataset version periods."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from metrka_core.metadata.file_marshal_models import MarshaledFile

VersionPeriodStrategy = Literal["max_column", "source_last_modified"]

VersionPeriodGrain = Literal["year", "month", "day"]


VALID_VERSION_PERIOD_STRATEGIES = {"max_column", "source_last_modified"}

VALID_VERSION_PERIOD_GRAINS = {"year", "month", "day"}


@dataclass(frozen=True)
class VersionPeriodSpec:
    """Configuration describing how a dataset version period is discovered."""

    strategy: VersionPeriodStrategy
    grain: VersionPeriodGrain
    column: str | None = None
    date_format: str | None = None

    def __post_init__(self) -> None:
        if self.strategy == "max_column" and not self.column:
            raise ValueError("version_period.column is required when strategy is 'max_column'")

        if self.strategy == "source_last_modified":
            if self.column is not None:
                raise ValueError(
                    "version_period.column is not allowed when strategy is 'source_last_modified'"
                )

            if self.date_format is not None:
                raise ValueError(
                    "version_period.format is not allowed when strategy is 'source_last_modified'"
                )


@dataclass(frozen=True)
class VersionPeriod:
    """A discovered logical version period for one source dataset."""

    value: date
    grain: VersionPeriodGrain
    source: str

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("VersionPeriod.source must not be empty")

        if self.grain == "year" and (self.value.month != 1 or self.value.day != 1):
            raise ValueError("Year-grain version periods must use January 1")

        if self.grain == "month" and self.value.day != 1:
            raise ValueError("Month-grain version periods must use the first day")


def parse_version_period_spec(raw: Any, *, stream_name: str) -> VersionPeriodSpec:
    """Parse and validate silver.version_period from stream YAML."""

    if not isinstance(raw, dict):
        raise RuntimeError(f"silver.version_period must be a mapping for stream {stream_name}")

    allowed_keys = {"strategy", "grain", "column", "format"}

    unknown_keys = set(raw) - allowed_keys

    if unknown_keys:
        raise RuntimeError(
            f"Unsupported silver.version_period keys "
            f"for stream {stream_name}: {sorted(unknown_keys)}"
        )

    strategy_raw = raw.get("strategy")

    if strategy_raw not in VALID_VERSION_PERIOD_STRATEGIES:
        raise RuntimeError(
            f"Unsupported silver.version_period.strategy for stream {stream_name}: {strategy_raw!r}"
        )

    grain_raw = raw.get("grain")

    if grain_raw not in VALID_VERSION_PERIOD_GRAINS:
        raise RuntimeError(
            f"Unsupported silver.version_period.grain for stream {stream_name}: {grain_raw!r}"
        )

    column_raw = raw.get("column")

    if column_raw is not None:
        if not isinstance(column_raw, str) or not column_raw.strip():
            raise RuntimeError(
                f"silver.version_period.column must be a non-empty string for stream {stream_name}"
            )

        column_raw = column_raw.strip()

    format_raw = raw.get("format")

    if format_raw is not None:
        if not isinstance(format_raw, str) or not format_raw.strip():
            raise RuntimeError(
                f"silver.version_period.format must be a non-empty string for stream {stream_name}"
            )

        format_raw = format_raw.strip()

    return VersionPeriodSpec(
        strategy=cast(VersionPeriodStrategy, strategy_raw),
        grain=cast(VersionPeriodGrain, grain_raw),
        column=column_raw,
        date_format=format_raw,
    )


VersionPeriodDiscovery = Callable[[Path, dict[str, Any], MarshaledFile], VersionPeriod]


def _normalize_period(value: date, *, grain: VersionPeriodGrain) -> date:
    """Normalize a date to its configured version-period grain."""

    if grain == "year":
        return date(value.year, 1, 1)

    if grain == "month":
        return date(value.year, value.month, 1)

    return value


def _read_version_column(
    *, file_path: Path, input_format: str, input_kwargs: dict[str, Any], column: str
) -> pd.Series:
    """Read one complete source column used for version discovery."""

    normalized_format = input_format.strip().lower().lstrip(".")
    read_kwargs = dict(input_kwargs)

    # Version discovery must inspect the complete column.
    read_kwargs.pop("nrows", None)

    if normalized_format in {"csv", "txt", "tsv"}:
        if normalized_format == "tsv":
            read_kwargs.setdefault("sep", "\t")

        read_kwargs["usecols"] = [column]
        read_kwargs["dtype"] = str

        frame = pd.read_csv(file_path, **read_kwargs)

    elif normalized_format in {"xlsx", "xls", "excel"}:
        read_kwargs["usecols"] = [column]
        read_kwargs["dtype"] = str

        frame = pd.read_excel(file_path, **read_kwargs)

    elif normalized_format == "parquet":
        read_kwargs["columns"] = [column]

        frame = pd.read_parquet(file_path, **read_kwargs)

    else:
        raise ValueError(f"Unsupported input format for version-period discovery: {input_format!r}")

    if not isinstance(frame, pd.DataFrame):
        raise ValueError(
            f"Version-period discovery expected one table, but received {type(frame).__name__}"
        )

    if column not in frame.columns:
        raise ValueError(f"Version-period column {column!r} was not found in {file_path.name}")

    return frame[column]


def _discover_from_max_column(
    *, spec: VersionPeriodSpec, file_path: Path, input_format: str, input_kwargs: dict[str, Any]
) -> VersionPeriod:
    """Discover version period from the maximum value in a source column."""

    if spec.column is None:
        raise ValueError("Missing column for max_column version-period strategy")

    series = _read_version_column(
        file_path=file_path,
        input_format=input_format,
        input_kwargs=input_kwargs,
        column=spec.column,
    )

    parsed_values = pd.to_datetime(series.dropna(), format=spec.date_format, errors="coerce")

    max_value = parsed_values.max()

    if pd.isna(max_value):
        raise ValueError(
            f"Could not discover version period from column {spec.column!r} in {file_path.name}"
        )

    discovered_date = max_value.to_pydatetime().date()

    return VersionPeriod(
        value=_normalize_period(discovered_date, grain=spec.grain),
        grain=spec.grain,
        source=f"column:{spec.column}",
    )


def _discover_from_source_last_modified(
    *, spec: VersionPeriodSpec, marshaled_file: MarshaledFile
) -> VersionPeriod:
    """Discover version period from source-provided modification metadata."""

    source_last_modified = marshaled_file.source_last_modified

    if source_last_modified is None:
        raise ValueError(
            "Cannot discover version_period using source_last_modified: "
            "the extractor did not provide source modification metadata"
        )

    return VersionPeriod(
        value=_normalize_period(source_last_modified.date(), grain=spec.grain),
        grain=spec.grain,
        source="source_last_modified",
    )


def build_version_period_discovery(
    *, spec: VersionPeriodSpec, input_format: str
) -> VersionPeriodDiscovery:
    """Build the configured version-period discovery function."""

    def discover(
        file_path: Path, input_kwargs: dict[str, Any], marshaled_file: MarshaledFile
    ) -> VersionPeriod:
        if spec.strategy == "max_column":
            return _discover_from_max_column(
                spec=spec, file_path=file_path, input_format=input_format, input_kwargs=input_kwargs
            )

        if spec.strategy == "source_last_modified":
            return _discover_from_source_last_modified(spec=spec, marshaled_file=marshaled_file)

        raise ValueError(f"Unsupported version-period strategy: {spec.strategy}")

    return discover
