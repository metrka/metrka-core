"""Checks for a Silver table and its columns."""

from __future__ import annotations

import math
from collections.abc import Sequence
from decimal import Decimal
from numbers import Number
from typing import Any

import pandas as pd

from metrka_core.quality.models import Outcome

_EXAMPLE_LIMIT = 5


def has_rows(table: pd.DataFrame, *, min_rows: int = 1) -> Outcome:
    """The table has at least ``min_rows`` row(s)."""

    row_count = len(table)

    return Outcome.of(
        row_count >= min_rows,
        f"Table has {row_count} row(s); minimum is {min_rows}.",
        expected={"min_rows": min_rows},
        actual={"row_count": row_count},
    )


def columns_match(table: pd.DataFrame, *, expected: Sequence[str], allow_extra: bool) -> Outcome:
    """The table has every column the contract declares."""

    actual = [str(column) for column in table.columns]
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    passed = not missing and (allow_extra or not unexpected)

    return Outcome.of(
        passed,
        "Columns match the contract."
        if passed
        else f"Missing columns: {missing}; unexpected columns: {unexpected}.",
        expected={"columns": list(expected), "allow_extra_columns": allow_extra},
        actual={"columns": actual, "missing_columns": missing, "unexpected_columns": unexpected},
    )


def not_null(table: pd.DataFrame, *, column: str) -> Outcome:
    """Every value in the column is present."""

    return _rows_rule(table, column, table[column].isna(), "has no missing values")


def is_null(table: pd.DataFrame, *, column: str) -> Outcome:
    """Every value in the column is missing."""

    return _rows_rule(table, column, table[column].notna(), "is always missing")


def unique(table: pd.DataFrame, *, columns: Sequence[str]) -> Outcome:
    """No two rows share the same values in ``columns``."""

    duplicated = table.duplicated(subset=list(columns), keep=False)
    count = int(duplicated.sum())
    examples = table.loc[duplicated, list(columns)].head(_EXAMPLE_LIMIT)

    return Outcome.of(
        count == 0,
        f"Rows are unique by {list(columns)}."
        if count == 0
        else f"{count} row(s) share values in {list(columns)}.",
        expected={"unique_by": list(columns)},
        actual={
            "duplicated_row_count": count,
            "examples": [
                [_plain(value) for value in row] for row in examples.itertuples(index=False)
            ],
        },
    )


def min_value(table: pd.DataFrame, *, column: str, minimum: float) -> Outcome:
    """Every present value is at least ``minimum``."""

    values = _decimals(table[column])
    return _rows_rule(table, column, values < _decimal(minimum), f"is >= {minimum}")


def max_value(table: pd.DataFrame, *, column: str, maximum: float) -> Outcome:
    """Every present value is at most ``maximum``."""

    values = _decimals(table[column])
    return _rows_rule(table, column, values > _decimal(maximum), f"is <= {maximum}")


def between(table: pd.DataFrame, *, column: str, low: float, high: float) -> Outcome:
    """Every present value is within ``low``..``high`` inclusive."""

    values = _decimals(table[column])
    outside = (values < _decimal(low)) | (values > _decimal(high))
    return _rows_rule(table, column, outside, f"is between {low} and {high}")


def allowed_values(table: pd.DataFrame, *, column: str, values: Sequence[Any]) -> Outcome:
    """Every present value is one of ``values``."""

    series = table[column]
    not_allowed = series.notna() & ~series.isin(list(values))
    return _rows_rule(table, column, not_allowed, f"is one of {list(values)}")


def matches_pattern(table: pd.DataFrame, *, column: str, pattern: str) -> Outcome:
    """Every present value, written as text, matches the regular expression."""

    values = table[column].dropna().astype(str)
    return _rows_rule(table, column, ~values.str.fullmatch(pattern), f"matches {pattern}")


def forbidden_values(table: pd.DataFrame, *, column: str, values: Sequence[Any]) -> Outcome:
    """No value is one of ``values``."""

    forbidden = table[column].isin(list(values))
    return _rows_rule(table, column, forbidden, f"is never one of {list(values)}")


def _rows_rule(table: pd.DataFrame, column: str, bad: pd.Series, rule: str) -> Outcome:
    """Turn a mask of rows that break a column rule into an outcome."""

    if table.empty:
        return Outcome.skipped(f"No rows to check for {column}.")

    bad = bad.reindex(table.index, fill_value=False).astype(bool)
    count = int(bad.sum())

    return Outcome.of(
        count == 0,
        f"{column} {rule}." if count == 0 else f"{count} row(s) where {column} breaks: {rule}.",
        expected={"column": column, "rule": rule},
        actual={
            "failed_row_count": count,
            "examples": [_plain(value) for value in table.loc[bad, column].head(_EXAMPLE_LIMIT)],
        },
    )


def _decimals(series: pd.Series) -> pd.Series:
    """Present values as exact decimals, so 0.1 in YAML equals 0.1 in data."""

    return series.dropna().map(_decimal)


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _plain(value: object) -> object:
    """Make a table value safe for JSON evidence."""

    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
        return None

    if isinstance(value, (bool, str)):
        return value

    if isinstance(value, Number) and not isinstance(value, Decimal):
        return value.item() if hasattr(value, "item") else value

    return str(value)
