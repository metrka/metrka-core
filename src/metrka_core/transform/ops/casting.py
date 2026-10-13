"""
casting.py

Processing logic for casting data types.

Cleans up missing values and applies dtype casting based on schema rules.
Because "everything is a string" is not a data model.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation, localcontext
from functools import partial
from typing import Any

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationEvidenceKind,
    TransformationEvidenceStatus,
)
from metrka_core.transform.cast_types import SilverCastType
from metrka_core.transform.result import TransformationResult

logger = logging.getLogger(__name__)

_DECIMAL_CAST_PATTERN = re.compile(r"^decimal\(([1-9]\d*),(\d+)\)$")


def parse_decimal_cast_type(cast_type: str) -> tuple[int, int] | None:
    """Parse decimal(precision, scale), returning its numeric parameters."""

    match = _DECIMAL_CAST_PATTERN.fullmatch(cast_type)

    if match is None:
        return None

    precision = int(match.group(1))
    scale = int(match.group(2))

    if precision > 38 or scale > precision:
        return None

    return precision, scale


def _cast_decimal_value(value: Any, *, precision: int, scale: int) -> Decimal | None:
    """Convert one value to a fixed-precision Decimal."""

    if pd.isna(value):
        return None

    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Value {value!r} is not a valid decimal") from exc

    if not decimal_value.is_finite():
        raise ValueError(f"Value {value!r} is not a finite decimal")

    quantizer = Decimal(1).scaleb(-scale)

    try:
        with localcontext() as decimal_context:
            decimal_context.prec = max(precision, len(decimal_value.as_tuple().digits)) + scale + 1
            quantized_value = decimal_value.quantize(quantizer)
    except InvalidOperation as exc:
        raise ValueError(
            f"Value {value!r} cannot be represented as decimal({precision},{scale})"
        ) from exc

    if quantized_value != decimal_value:
        raise ValueError(f"Value {value!r} has more than {scale} decimal places")

    _, digits, exponent = quantized_value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError(f"Value {value!r} does not have a finite decimal exponent")
    integer_digits = max(len(digits) + exponent, 0)
    total_digits = integer_digits + scale

    if total_digits > precision:
        raise ValueError(f"Value {value!r} exceeds decimal({precision},{scale})")

    return quantized_value


def normalize_missing(df: pd.DataFrame) -> TransformationResult:
    """Replace blank or whitespace-only strings with missing values."""

    logger.debug("start: rows=%d cols=%d", len(df), len(df.columns))

    out = df.replace(r"^\s*$", pd.NA, regex=True)

    evidence: list[AutomaticColumnEvidence] = []

    for raw_column_name in df.columns:
        changed_mask = df[raw_column_name].notna() & out[raw_column_name].isna()
        affected_row_count = int(changed_mask.sum())

        if affected_row_count == 0:
            continue

        evidence.append(
            AutomaticColumnEvidence(
                operation="normalize_missing",
                kind=TransformationEvidenceKind.MISSING_NORMALIZATION,
                status=TransformationEvidenceStatus.APPLIED,
                column_name=str(raw_column_name),
                affected_row_count=affected_row_count,
                reason="Blank or whitespace-only source strings were represented as missing values.",
                metrics={"blank_value_count": affected_row_count},
            )
        )

    return TransformationResult(data=out, evidence=tuple(evidence))


def cast_columns(df: pd.DataFrame, casts: dict[str, Any]) -> TransformationResult:
    """
    Cast configured columns to their declared target types.

    Each configured conversion produces aggregate automatic evidence.
    """

    evidence: list[AutomaticColumnEvidence] = []

    if not casts:
        logger.info("nothing to cast")
        return TransformationResult(data=df)

    logger.info("start: cols=%d rows=%d", len(casts), len(df))
    logger.debug("casts=%s", casts)

    for col, typ in casts.items():
        target_type = str(typ)
        reason = f"Values were converted to the {target_type} type declared in the data contract."

        if col not in df.columns:
            message = f"cast column not found {col}"
            logger.error("failed: %s", message)
            raise KeyError(message)

        source_dtype = str(df[col].dtype)
        non_null_value_count = int(df[col].notna().sum())

        try:
            decimal_type = parse_decimal_cast_type(typ) if isinstance(typ, str) else None

            if typ == SilverCastType.STRING:
                df[col] = df[col].astype("string")

            elif typ == SilverCastType.BOOL:
                df[col] = df[col].astype("boolean")

            elif typ == SilverCastType.INT:
                df[col] = df[col].astype("Int64")

            elif typ == SilverCastType.FLOAT:
                df[col] = pd.to_numeric(df[col], errors="raise").astype("Float64")

            elif decimal_type is not None:
                precision, scale = decimal_type

                decimal_converter = partial(_cast_decimal_value, precision=precision, scale=scale)
                decimal_values = df[col].map(decimal_converter)
                decimal_dtype = pd.ArrowDtype(pa.decimal128(precision, scale))
                df[col] = pd.Series(decimal_values.tolist(), index=df.index, dtype=decimal_dtype)

            else:
                message = f"Unknown cast {typ!r} for column {col}"
                logger.error("failed: %s", message)
                raise ValueError(message)

        except Exception as exc:
            message = f"failed casting column {col!r} to {typ!r}: {exc}"
            logger.error("failed: %s", message)
            raise

        published_dtype = str(df[col].dtype)

        evidence.append(
            AutomaticColumnEvidence(
                operation="cast_columns",
                kind=TransformationEvidenceKind.TYPE_CAST,
                status=(
                    TransformationEvidenceStatus.APPLIED
                    if non_null_value_count > 0
                    else TransformationEvidenceStatus.NO_CHANGE
                ),
                column_name=col,
                affected_row_count=non_null_value_count,
                reason=reason,
                metrics={
                    "source_dtype": source_dtype,
                    "published_dtype": published_dtype,
                    "target_type": target_type,
                    "non_null_value_count": non_null_value_count,
                    "failed_value_count": 0,
                },
            )
        )

        logger.debug(
            "applied: col=%s typ=%s source_dtype=%s published_dtype=%s values=%d",
            col,
            typ,
            source_dtype,
            published_dtype,
            non_null_value_count,
        )

    return TransformationResult(data=df, evidence=tuple(evidence))
