"""Fingerprints for configuration affecting Silver builds."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from metrka_core.quality.config import ColumnRule, QualityConfig


def calculate_quality_config_hash(config: QualityConfig) -> str:
    tables: dict[str, object] = {}

    for table_key, rules in config.tables.items():
        table: dict[str, object] = {
            "unique": list(rules.unique),
            "columns": _rules_payload(rules.columns),
        }

        if rules.when:
            table["when"] = [
                {"rows": dict(block.rows), "columns": _rules_payload(block.columns)}
                for block in rules.when
            ]

        tables[table_key] = table

    return calculate_config_hash({"version": config.version, "tables": tables})


def _rules_payload(columns: Mapping[str, tuple[ColumnRule, ...]]) -> dict[str, object]:
    return {
        column: [
            {"rule": rule.rule, "value": rule.value, "severity": rule.severity}
            for rule in column_rules
        ]
        for column, column_rules in columns.items()
    }


def calculate_config_hash(payload: object) -> str:
    normalized = _normalize(payload)

    canonical_json = json.dumps(
        normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _normalize(value: object) -> object:
    if value is None:
        return None

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Path):
        return value.as_posix()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Mapping):
        return {
            str(key): _normalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }

    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]

    if isinstance(value, (str, int, float, bool)):
        return value

    raise TypeError(f"Unsupported configuration fingerprint type: {type(value).__name__}")
