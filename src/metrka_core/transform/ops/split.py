"""Split one text column into several columns at a separator.

A contract column names its parts and their rules under ``split``::

    Race/Sex:
      rename_to: race_sex
      cast_to: string
      split:
        separator: "/"
        into:
          race: {cast_to: string}
          sex: {cast_to: string}
        exclude_column: true

The column itself is still published, so every part can be checked against it, unless
``exclude_column: true`` leaves it out (the default is false). Each part is a published column
like any other: it is cast, parsed and case-converted by its own rule.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from operator import itemgetter
from typing import Any

import pandas as pd

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationEvidence,
    TransformationEvidenceKind,
    TransformationEvidenceStatus,
)
from metrka_core.transform.result import TransformationResult

logger = logging.getLogger(__name__)

SPLIT_PART_RULE_KEYS = frozenset({"cast_to", "format_in", "case", "meta"})
SPLIT_KEYS = frozenset({"separator", "into", "exclude_column"})

_EXAMPLE_LIMIT = 5


def split_part_rules(columns: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Rules of the columns a table splits its columns into, keyed by part name.

    Each rule gets ``rename_to`` set to the part's name, like the rule of a source column.
    """

    parts: dict[str, dict[str, Any]] = {}

    for spec in columns.values():
        if not isinstance(spec, Mapping) or not isinstance(spec.get("split"), Mapping):
            continue

        into = spec["split"].get("into")

        if not isinstance(into, Mapping):
            continue

        for name, rule in into.items():
            part_rule = dict(rule) if isinstance(rule, Mapping) else {}
            parts[str(name)] = {**part_rule, "rename_to": str(name)}

    return parts


def excludes_column(spec: Any) -> bool:
    """Whether a column is split into its parts and itself left out of the table."""

    return (
        isinstance(spec, Mapping)
        and isinstance(spec.get("split"), Mapping)
        and spec["split"].get("exclude_column") is True
    )


def published_column_rules(columns: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Rules of every column a contract table publishes, keyed by published name."""

    rules = {
        str(spec.get("rename_to") or source): dict(spec)
        for source, spec in columns.items()
        if isinstance(spec, Mapping) and not excludes_column(spec)
    }
    rules.update(split_part_rules(columns))

    return rules


def split_columns(
    df: pd.DataFrame, splits: Mapping[str, tuple[str, tuple[str, ...], bool]]
) -> TransformationResult:
    """Split each column at its separator into its named parts.

    ``splits`` maps a column, by its name after renaming, to its separator, its part names and
    whether the column itself is left out of the table, which the final column order does.
    Every present value must split into exactly as many pieces as there are parts. Pieces are
    trimmed, and an empty piece is missing. A missing value leaves every part missing.
    """

    if not splits:
        logger.info("split_columns: none")
        return TransformationResult(data=df)

    evidence: list[TransformationEvidence] = []

    for column, (separator, parts, excluded) in splits.items():
        if column not in df.columns:
            raise KeyError(f"split column not found: {column}")

        existing = [part for part in parts if part in df.columns]

        if existing:
            raise ValueError(f"split parts of {column} are already columns: {existing}")

        values = df[column].astype("string")
        present = values.notna()
        pieces = values.str.split(separator, regex=False)
        wrong = present & pieces.map(len, na_action="ignore").ne(len(parts)).fillna(False)

        if wrong.any():
            examples = list(dict.fromkeys(values[wrong].tolist()))[:_EXAMPLE_LIMIT]
            raise ValueError(
                f"{int(wrong.sum())} value(s) of {column} do not split at {separator!r} into "
                f"{len(parts)} parts ({', '.join(parts)}), such as {examples}"
            )

        empty_part_counts: dict[str, int] = {}

        for position, part in enumerate(parts):
            piece = (
                pieces.map(itemgetter(position), na_action="ignore").astype("string").str.strip()
            )
            piece = piece.mask(piece.eq("").fillna(False))
            df[part] = piece
            empty_part_counts[part] = int((present & piece.isna()).sum())

        split_count = int(present.sum())

        evidence.append(
            AutomaticColumnEvidence(
                operation="split_column",
                kind=TransformationEvidenceKind.COLUMN_SPLIT,
                status=(
                    TransformationEvidenceStatus.APPLIED
                    if split_count
                    else TransformationEvidenceStatus.NO_CHANGE
                ),
                column_name=column,
                affected_row_count=split_count,
                reason=(
                    f"Split {column} at {separator!r} into {', '.join(parts)}"
                    + (f"; {column} itself is not published." if excluded else ".")
                ),
                metrics={
                    "separator": separator,
                    "into": list(parts),
                    "column_excluded": excluded,
                    "split_value_count": split_count,
                    "missing_value_count": int((~present).sum()),
                    "empty_part_counts": empty_part_counts,
                },
            )
        )

        logger.info(
            "split_columns applied: column=%s into=%s rows=%d", column, list(parts), split_count
        )

    return TransformationResult(data=df, evidence=tuple(evidence))
