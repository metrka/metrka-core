"""
Text normalization helpers.

Strips whitespace and fixes casing so joins don't break because one system
writes "New York", another writes "new york" and a third prefers "NEW YORK".
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import pandas as pd

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationDetailRow,
    TransformationEvidenceKind,
    TransformationEvidenceStatus,
    TransformationObservation,
)
from metrka_core.transform.result import TransformationResult

logger = logging.getLogger(__name__)


def convert_case(df: pd.DataFrame, conv_logic: Mapping[str, str]) -> TransformationResult:
    """Convert text case and record aggregate evidence."""

    evidence: list[AutomaticColumnEvidence] = []

    if not conv_logic:
        logger.info("no conversion logic exists")
        return TransformationResult(data=df)

    configured = {column_name: mode for column_name, mode in conv_logic.items() if mode}

    if not configured:
        logger.info("nothing configured for conversion")
        return TransformationResult(data=df)

    logger.info("start: configured=%d rows=%d", len(configured), len(df))
    logger.debug("rules=%s", configured)

    supported_modes = {"upper", "lower", "title"}

    for col, how in configured.items():
        reason = (
            f"Text values were stripped and converted to {how} "
            "case as declared in the data contract."
        )

        if col not in df.columns:
            message = f"Column not found {col}"
            logger.error("failed: %s", message)
            raise KeyError(message)

        if how not in supported_modes:
            message = f"Unknown case rule {how!r} for column {col}"
            logger.error("failed: %s", message)
            raise ValueError(message)

        source = df[col].astype("string")
        stripped = source.str.strip()

        if how == "upper":
            converted = stripped.str.upper()
        elif how == "lower":
            converted = stripped.str.lower()
        else:
            converted = stripped.str.title()

        trimmed_mask = source.ne(stripped).fillna(False)
        case_changed_mask = stripped.ne(converted).fillna(False)
        changed_mask = source.ne(converted).fillna(False)

        trimmed_value_count = int(trimmed_mask.sum())
        case_changed_value_count = int(case_changed_mask.sum())
        affected_row_count = int(changed_mask.sum())

        df[col] = converted

        evidence.append(
            AutomaticColumnEvidence(
                operation="convert_case",
                kind=TransformationEvidenceKind.CASE_CONVERSION,
                status=(
                    TransformationEvidenceStatus.APPLIED
                    if affected_row_count > 0
                    else TransformationEvidenceStatus.NO_CHANGE
                ),
                column_name=col,
                affected_row_count=affected_row_count,
                reason=reason,
                metrics={
                    "mode": how,
                    "non_null_value_count": int(source.notna().sum()),
                    "trimmed_value_count": trimmed_value_count,
                    "case_changed_value_count": case_changed_value_count,
                },
            )
        )

        logger.debug("applied: col=%s how=%s affected=%d", col, how, affected_row_count)

    return TransformationResult(data=df, evidence=tuple(evidence))


def normalize_values(
    df: pd.DataFrame, rules: Mapping[str, Mapping[Any, Mapping[str, Any]]]
) -> TransformationResult:
    """
    Replace explicitly configured values and record aggregated impact.

    Rules use final column names, after rename_to has been applied.
    Only replacements that actually affected rows produce observations.
    """
    evidence: list[TransformationObservation] = []

    if not rules:
        logger.info("normalize_values: none")
        return TransformationResult(data=df)

    logger.info("normalize_values: configured_columns=%d", len(rules))

    for column_name, replacements in rules.items():
        if column_name not in df.columns:
            message = f"normalize_values column not found: {column_name}"
            raise KeyError(message)

        for before_value, replacement_rule in replacements.items():
            if not isinstance(replacement_rule, Mapping):
                raise TypeError(
                    "normalize_values replacement rule must "
                    f"be a mapping: {column_name}.{before_value}"
                )

            if "replace_with" not in replacement_rule:
                raise ValueError(
                    "normalize_values replacement rule is missing "
                    f"replace_with: {column_name}.{before_value}"
                )

            after_value = replacement_rule["replace_with"]
            reason = replacement_rule.get("reason")

            if before_value == after_value:
                logger.debug(
                    "normalize_values skipped identical values: column=%s value=%r",
                    column_name,
                    before_value,
                )
                continue

            mask = df[column_name].eq(before_value).fillna(False)
            affected_row_count = int(mask.sum())

            if affected_row_count == 0:
                logger.debug(
                    "normalize_values no matches: column=%s before=%r", column_name, before_value
                )
                continue

            record_details = replacement_rule.get("record_details", False)
            detail_columns = tuple(replacement_rule.get("detail_columns", []))
            detail_rows: tuple[TransformationDetailRow, ...] = ()

            if record_details:
                missing_detail_columns = [
                    detail_column
                    for detail_column in detail_columns
                    if detail_column not in df.columns
                ]

                if missing_detail_columns:
                    message = f"normalize_values detail columns not found: {missing_detail_columns}"
                    raise KeyError(message)

                detail_rows = tuple(
                    TransformationDetailRow(
                        source_row_number=row_position,
                        context={
                            detail_column: df.iloc[row_position - 1][detail_column]
                            for detail_column in detail_columns
                        },
                    )
                    for row_position, matched in enumerate(mask.to_numpy(), start=1)
                    if bool(matched)
                )

            if not isinstance(reason, str) or not reason.strip():
                raise ValueError(
                    f"normalize_values requires a non-empty reason for {column_name}.{before_value}"
                )

            df.loc[mask, column_name] = after_value

            evidence.append(
                TransformationObservation(
                    operation="normalize_values",
                    column_name=column_name,
                    before_value=before_value,
                    after_value=after_value,
                    affected_row_count=affected_row_count,
                    record_details=record_details,
                    detail_columns=detail_columns,
                    detail_rows=detail_rows,
                    meta={
                        "evidence_kind": "value_change",
                        "evidence_status": "applied",
                        "reason": reason.strip(),
                        "metrics": {},
                    },
                )
            )

            logger.info(
                "normalize_values applied: column=%s before=%r after=%r rows=%d",
                column_name,
                before_value,
                after_value,
                affected_row_count,
            )

    return TransformationResult(data=df, evidence=tuple(evidence))
