"""Reconcile parent totals with their child-level rows."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pandas as pd

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationEvidenceKind,
    TransformationEvidenceStatus,
)
from metrka_core.transform.result import TransformationResult


def _plain_scalar(value: Any) -> Any:
    """Convert pandas and NumPy scalar values into serializable Python values."""

    if pd.isna(value):
        return None

    if isinstance(value, Decimal):
        return str(value)

    item = getattr(value, "item", None)

    if callable(item):
        return item()

    return value


def _require_columns(table: pd.DataFrame, *, columns: set[str]) -> None:
    missing = sorted(columns - set(table.columns))

    if missing:
        raise KeyError(f"Parent-child reconciliation columns not found: {missing}")


def reconcile_parent_child_rows(
    table: pd.DataFrame, config: Mapping[str, Any]
) -> TransformationResult:
    """Remove parent rows and add residual rows when child totals are lower.

    A child row with a missing measure fails the reconciliation, unless the contract sets
    ``missing_child_values: allow`` for a source that leaves small counts blank. Then the
    row stays as it is, the shown values are summed, and the residual row holds what the
    children do not show.
    """

    group_by = list(config["group_by"])
    parent_config = config["parent"]
    residual_config = config["residual"]

    parent_column = str(parent_config["column"])
    parent_value = parent_config["equals"]
    measure_column = str(config["measure_column"])
    label_column = str(residual_config["label_column"])
    label_value = residual_config["label_value"]
    remove_parent = config["remove_parent"]
    allow_missing_children = config.get("missing_child_values", "fail") == "allow"

    _require_columns(table, columns={*group_by, parent_column, measure_column, label_column})

    source = table.copy().reset_index(drop=True)
    output_frames: list[pd.DataFrame] = []
    evidence: list[AutomaticColumnEvidence] = []

    grouped = source.groupby(group_by, dropna=False, sort=False)

    for _, group in grouped:
        group = group.copy()

        parent_mask = group[parent_column].eq(parent_value).fillna(False)
        parent_rows = group.loc[parent_mask]
        child_rows = group.loc[~parent_mask]

        if len(parent_rows) != 1:
            group_values = {column: _plain_scalar(group.iloc[0][column]) for column in group_by}
            raise ValueError(
                "Parent-child reconciliation requires exactly one parent "
                f"row per group; group={group_values}, "
                f"parent_rows={len(parent_rows)}"
            )

        if child_rows.empty:
            group_values = {
                column: _plain_scalar(parent_rows.iloc[0][column]) for column in group_by
            }
            raise ValueError(
                f"Parent-child reconciliation requires at least one child row; group={group_values}"
            )

        parent_total = parent_rows.iloc[0][measure_column]

        if pd.isna(parent_total):
            # A source may publish a group with no values at all, parent included: there is
            # nothing to reconcile, so the rows stay as given and no residual is derived.
            if not (allow_missing_children and child_rows[measure_column].isna().all()):
                raise ValueError("Parent-child reconciliation parent total must not be missing")

            output_frames.append(child_rows if remove_parent else group)
            evidence.append(
                AutomaticColumnEvidence(
                    operation="parent_child_reconciliation",
                    kind=TransformationEvidenceKind.PARENT_CHILD_RECONCILIATION,
                    status=TransformationEvidenceStatus.NO_CHANGE,
                    column_name=measure_column,
                    affected_row_count=0,
                    reason=(
                        f"The {parent_column} = {_plain_scalar(parent_value)} row and every "
                        "other row of the group have no value, so there was nothing to compare."
                    ),
                    metrics={
                        "parent": {"column": parent_column, "value": _plain_scalar(parent_value)},
                        "residual": {"column": label_column, "value": _plain_scalar(label_value)},
                        "group": {
                            column: _plain_scalar(parent_rows.iloc[0][column])
                            for column in group_by
                        },
                        "parent_total": None,
                        "child_total": None,
                        "difference": None,
                        "parent_row_count": 1,
                        "child_row_count": len(child_rows),
                        "missing_child_count": len(child_rows),
                        "derived_row_count": 0,
                        "parent_removed": remove_parent,
                    },
                )
            )
            continue

        missing_child_count = int(child_rows[measure_column].isna().sum())

        if missing_child_count and not allow_missing_children:
            raise ValueError("Parent-child reconciliation child totals must not be missing")

        child_total = child_rows[measure_column].sum(skipna=True)
        difference = parent_total - child_total

        group_values = {column: _plain_scalar(parent_rows.iloc[0][column]) for column in group_by}

        if difference < 0:
            raise ValueError(
                "Child total exceeds parent total: "
                f"group={group_values}, "
                f"parent_total={_plain_scalar(parent_total)}, "
                f"child_total={_plain_scalar(child_total)}"
            )

        if remove_parent:
            output_frames.append(child_rows)
        else:
            output_frames.append(group)

        derived_row_count = 0

        if difference > 0:
            residual_row = {column: pd.NA for column in source.columns}

            for column in group_by:
                residual_row[column] = parent_rows.iloc[0][column]

            residual_row[label_column] = label_value
            residual_row[measure_column] = difference

            output_frames.append(pd.DataFrame([residual_row], columns=source.columns))
            derived_row_count = 1

        evidence.append(
            AutomaticColumnEvidence(
                operation="parent_child_reconciliation",
                kind=(TransformationEvidenceKind.PARENT_CHILD_RECONCILIATION),
                status=(
                    TransformationEvidenceStatus.APPLIED
                    if derived_row_count
                    else TransformationEvidenceStatus.NO_CHANGE
                ),
                column_name=measure_column,
                affected_row_count=derived_row_count,
                reason=(
                    f"The {parent_column} = {_plain_scalar(parent_value)} row was compared "
                    "with the sum of the other rows as declared in the data contract."
                ),
                metrics={
                    # Name the rows, so the evidence explains itself without the contract.
                    "parent": {"column": parent_column, "value": _plain_scalar(parent_value)},
                    "residual": {"column": label_column, "value": _plain_scalar(label_value)},
                    "group": group_values,
                    "parent_total": _plain_scalar(parent_total),
                    "child_total": _plain_scalar(child_total),
                    "difference": _plain_scalar(difference),
                    "parent_row_count": 1,
                    "child_row_count": len(child_rows),
                    "missing_child_count": missing_child_count,
                    "derived_row_count": derived_row_count,
                    "parent_removed": remove_parent,
                },
            )
        )

    result = pd.concat(output_frames, ignore_index=True)

    return TransformationResult(data=result, evidence=tuple(evidence))
