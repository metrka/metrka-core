"""
dates.py

Processing logic for dates.

Because source systems treat dates like a creative-writing exercise, this module
here tries to turn them into actual datetimes without starting a philosophical debate.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationEvidenceKind,
    TransformationEvidenceStatus,
)
from metrka_core.transform.cast_types import (
    CANONICAL_DATE_CAST_TYPES,
    SilverCastType,
    resolve_silver_cast_type,
)
from metrka_core.transform.result import TransformationResult

logger = logging.getLogger(__name__)


def parse_dates(
    df: pd.DataFrame, date_specs: Mapping[str, Mapping[str, Any]]
) -> TransformationResult:
    """Parse configured columns into dates and record aggregate evidence."""

    evidence: list[AutomaticColumnEvidence] = []

    if not date_specs:
        logger.info("no dates spec found")
        return TransformationResult(data=df)

    logger.info("start: rows=%d cols=%d", len(df), len(df.columns))
    logger.debug("columns=%s", sorted(date_specs.keys()))

    for col, spec in date_specs.items():
        format_value = spec.get("format_in")
        fmt_in = format_value if isinstance(format_value, str) and format_value else None
        target_type = resolve_silver_cast_type(spec.get("cast_to"))

        if target_type not in CANONICAL_DATE_CAST_TYPES:
            raise ValueError(
                f"Date parser received non-date cast type {target_type.value!r} for column {col!r}"
            )

        target_type_name = target_type.value

        if fmt_in:
            reason = (
                f"Values were parsed using the {fmt_in} "
                "source date format declared in the data contract."
            )
        else:
            reason = (
                "Values were parsed as dates using automatic format "
                "detection because no source date format was declared."
            )

        if col not in df.columns:
            message = f"Date column not found: {col}"
            logger.error("failed: %s", message)
            raise KeyError(message)

        source_non_null_mask = df[col].notna()
        source_non_null_count = int(source_non_null_mask.sum())

        parsed = pd.to_datetime(df[col], format=fmt_in, errors="coerce")

        coerced_mask = source_non_null_mask & parsed.isna()
        coerced_to_missing_count = int(coerced_mask.sum())
        parsed_value_count = source_non_null_count - coerced_to_missing_count

        if target_type is SilverCastType.DATE:
            date_dtype = pd.ArrowDtype(pa.date32())
            df[col] = pd.Series(parsed.dt.date.tolist(), index=df.index, dtype=date_dtype)
        else:
            df[col] = parsed

        evidence.append(
            AutomaticColumnEvidence(
                operation="parse_dates",
                kind=TransformationEvidenceKind.DATE_PARSE,
                status=(
                    TransformationEvidenceStatus.APPLIED
                    if source_non_null_count > 0
                    else TransformationEvidenceStatus.NO_CHANGE
                ),
                column_name=col,
                affected_row_count=source_non_null_count,
                reason=reason,
                metrics={
                    "source_format": fmt_in,
                    "target_type": target_type_name,
                    "source_non_null_count": source_non_null_count,
                    "parsed_value_count": parsed_value_count,
                    "coerced_to_missing_count": coerced_to_missing_count,
                },
            )
        )

        logger.info(
            "dates parsed: col=%s format=%s parsed=%d coerced_to_missing=%d",
            col,
            fmt_in or "auto",
            parsed_value_count,
            coerced_to_missing_count,
        )

    logger.info("dates.parse_dates done")

    return TransformationResult(data=df, evidence=tuple(evidence))
