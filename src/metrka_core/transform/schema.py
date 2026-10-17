"""
schema.py

This exists so upstream creativity does not become downstream archaeology.

Schema-driven transformations for a table:
rename => normalize_missing => cast => parse_dates => case => final order.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from metrka_core.lineage.transformation.models import TransformationEvidence
from metrka_core.transform.cast_types import is_date_like_cast_type, resolve_silver_cast_type
from metrka_core.transform.ops.casting import cast_columns, normalize_missing
from metrka_core.transform.ops.dates import parse_dates
from metrka_core.transform.ops.reconciliation import reconcile_parent_child_rows
from metrka_core.transform.ops.text import convert_case, normalize_values
from metrka_core.transform.result import TransformationResult

logger = logging.getLogger(__name__)


def _validate_source_columns(df: pd.DataFrame, cols_cfg: dict[str, Any]) -> None:
    expected_src = set(cols_cfg.keys())
    present_src = set(df.columns)
    missing = expected_src - present_src

    if missing:
        logger.error(
            "failed: missing expected source columns=%s; present_cols=%d expected_cols=%d",
            sorted(missing),
            len(present_src),
            len(expected_src),
        )

        raise KeyError(f"Missing expected source columns: {sorted(missing)}")

    else:
        logger.info("validation ok: expected source columns present (%d)", len(expected_src))


def _build_rename_map(cols_cfg: dict[str, Any]) -> dict[str, str]:
    return {
        src: spec["rename_to"]
        for src, spec in cols_cfg.items()
        if isinstance(spec, dict) and "rename_to" in spec
    }


def _apply_rename(df: pd.DataFrame, rename_map: dict[str, str]) -> pd.DataFrame:
    if not rename_map:
        logger.info("rename: none")
        return df

    logger.info("rename: %d columns will be renamed", len(rename_map))
    logger.debug("rename map=%s", rename_map)
    return df.rename(columns=rename_map)


def _tgt_name(src: str, rename_map: dict[str, str]) -> str:
    return rename_map.get(src, src)


def _require_date_format(source_column: str, spec: dict[str, Any]) -> str:
    format_in = spec.get("format_in")

    if not isinstance(format_in, str) or not format_in.strip():
        raise ValueError(
            f"Date-like cast for source column {source_column!r} "
            "requires a non-empty string format_in"
        )

    return format_in


def _build_cast_map(cols_cfg: dict[str, Any], rename_map: dict[str, str]) -> dict[str, str]:
    cast_map = {
        _tgt_name(src, rename_map): spec["cast_to"]
        for src, spec in cols_cfg.items()
        if isinstance(spec, dict)
        and spec.get("cast_to")
        and not is_date_like_cast_type(spec.get("cast_to"))
    }
    if cast_map:
        logger.info("cast: %d columns", len(cast_map))
        logger.debug("cast map=%s", cast_map)
    else:
        logger.info("cast: none")
    return cast_map


def _build_date_specs(
    cols_cfg: dict[str, Any], rename_map: dict[str, str]
) -> dict[str, dict[str, str]]:

    dates_specs = {
        _tgt_name(src, rename_map): {
            "format_in": _require_date_format(src, spec),
            "cast_to": resolve_silver_cast_type(spec["cast_to"]).value,
        }
        for src, spec in cols_cfg.items()
        if isinstance(spec, dict) and is_date_like_cast_type(spec.get("cast_to"))
    }
    if dates_specs:
        logger.info("parse_dates: %d columns", len(dates_specs))
        logger.debug("date specs=%s", dates_specs)
    else:
        logger.info("parse_dates: none")
    return dates_specs


def _build_case_map(cols_cfg: dict[str, Any], rename_map: dict[str, str]) -> dict[str, str]:
    case_map = {
        _tgt_name(src, rename_map): spec.get("case", "")
        for src, spec in cols_cfg.items()
        if isinstance(spec, dict)
    }

    configured = {k: v for k, v in case_map.items() if v}

    if configured:
        logger.info("convert_case: %d columns configured", len(configured))
        logger.debug("schema case map=%s", configured)
    else:
        logger.info("convert_case: none")
    return case_map  # return full map; convert_case can ignore blanks


def _build_normalize_values_rules(
    cols_cfg: dict[str, Any], rename_map: dict[str, str]
) -> dict[str, dict[Any, dict[str, Any]]]:
    """
    Build value-normalization rules using final renamed column names.
    """

    rules: dict[str, dict[Any, dict[str, Any]]] = {}

    for source_name, spec in cols_cfg.items():
        if not isinstance(spec, dict):
            continue

        normalize_config = spec.get("normalize_values")

        if not normalize_config:
            continue

        default_reason = normalize_config["reason"]
        mappings = normalize_config["mappings"]

        expanded_mappings: dict[Any, dict[str, Any]] = {}

        for before_value, replacement_rule in mappings.items():
            expanded_rule = dict(replacement_rule)
            expanded_rule.setdefault("reason", default_reason)
            expanded_mappings[before_value] = expanded_rule

        rules[_tgt_name(source_name, rename_map)] = expanded_mappings

    if rules:
        logger.info("normalize_values: %d columns configured", len(rules))
        logger.debug("normalize_values rules=%s", rules)
    else:
        logger.info("normalize_values: none")

    return rules


def _apply_canonical_order(df: pd.DataFrame, canonical: list[str] | None) -> pd.DataFrame:
    if not canonical:
        logger.info("canonical order: none")
        return df

    logger.info("canonical order: %d columns", len(canonical))
    missing_out = set(canonical) - set(df.columns)
    if missing_out:
        logger.error(
            "failed: missing_out=%s; current_cols=%d", sorted(missing_out), len(df.columns)
        )
        raise KeyError(f"Missing columns after transformation: {sorted(missing_out)}")

    return df.reindex(columns=canonical)


def apply_transformation(df: pd.DataFrame, cfg: dict[str, Any]) -> TransformationResult:
    """
    Apply table schema rules to a DataFrame.

    Order: validate => rename => normalize_missing => normalize_values => cast => parse_dates => case => reconcile => reorder.
    Contract mismatches always stop transformation of the current dataset.
    """

    logger.info("start: rows=%d cols=%d", len(df), len(df.columns))

    data_df = df.copy()
    cols_cfg = cfg["columns"]

    evidence: list[TransformationEvidence] = []

    # 1) validate source columns exist
    _validate_source_columns(data_df, cols_cfg)

    # 2) rename columns
    rename_map = _build_rename_map(cols_cfg)
    data_df = _apply_rename(data_df, rename_map)

    # 3) normalize missing early
    missing_result = normalize_missing(data_df)
    data_df = missing_result.data
    evidence.extend(missing_result.evidence)
    # 4) normalize explicitly configured values
    normalize_value_rules = _build_normalize_values_rules(cols_cfg, rename_map)

    normalize_result = normalize_values(data_df, normalize_value_rules)
    data_df = normalize_result.data
    evidence.extend(normalize_result.evidence)

    # 5) cast types
    cast_map = _build_cast_map(cols_cfg, rename_map)
    cast_result = cast_columns(data_df, cast_map)
    data_df = cast_result.data
    evidence.extend(cast_result.evidence)

    # 6) date parsing
    date_specs = _build_date_specs(cols_cfg, rename_map)
    date_result = parse_dates(data_df, date_specs)
    data_df = date_result.data
    evidence.extend(date_result.evidence)

    # 7) case conversions
    case_map = _build_case_map(cols_cfg, rename_map)
    case_result = convert_case(data_df, case_map)
    data_df = case_result.data
    evidence.extend(case_result.evidence)

    # 8) reconcile parent totals with child rows
    reconciliation_config = cfg.get("parent_child_reconciliation")

    if reconciliation_config is not None:
        reconciliation_result = reconcile_parent_child_rows(data_df, reconciliation_config)
        data_df = reconciliation_result.data
        evidence.extend(reconciliation_result.evidence)

    # 9) final column order
    data_df = _apply_canonical_order(data_df, cfg.get("canonical_order"))

    logger.info("done: rows=%d cols=%d", len(data_df), len(data_df.columns))

    return TransformationResult(data=data_df, evidence=tuple(evidence))
