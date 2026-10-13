"""Read a dataset's quality rules from quality.yaml.

Format (version 1)::

    version: 1
    tables:
      <table_key>:
        unique: [geography_name, reporting_year]   # optional
        columns:
          <published column>:
            - not_null
            - unique
            - min: 0
            - max: 100
            - between: [1990, 2030]
            - allowed: [a, b]
            - forbidden: [Florida]
            - {min: 0, severity: warning}

A contract with one table may skip the table level and write ``unique``,
``columns`` and ``when`` at the top of the file::

    version: 1
    columns:
      geography_name: [not_null]

Rules run on the finished Silver table. Checks every dataset needs (file is not
empty, archive is valid, table has rows, columns match the contract, output
files exist) run automatically and are not written here.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from metrka_core.quality.models import QualitySeverity
from metrka_core.transform.ops.casting import parse_decimal_cast_type

FLAG_RULES = frozenset({"not_null", "is_null", "unique"})
VALUE_RULES = frozenset({"min", "max", "between", "allowed", "forbidden", "pattern"})
NUMERIC_RULES = frozenset({"min", "max", "between"})
NUMERIC_CAST_TYPES = frozenset({"int", "float"})
CURRENT_YEAR = "current_year"


@dataclass(frozen=True, slots=True)
class ColumnRule:
    """One rule for one column, e.g. ``min: 0``."""

    rule: str
    value: Any = None
    severity: QualitySeverity = QualitySeverity.BLOCKING


@dataclass(frozen=True, slots=True)
class WhenRules:
    """Column rules for the rows that match ``rows``, e.g. ``geography_name: Unallocated``."""

    rows: Mapping[str, Any]
    columns: Mapping[str, tuple[ColumnRule, ...]]


@dataclass(frozen=True, slots=True)
class TableRules:
    """Rules for one Silver table."""

    unique: tuple[str, ...] = ()
    columns: Mapping[str, tuple[ColumnRule, ...]] = field(default_factory=dict)
    when: tuple[WhenRules, ...] = ()


@dataclass(frozen=True, slots=True)
class QualityConfig:
    """All quality rules of one workspace, by table key."""

    version: int = 1
    tables: Mapping[str, TableRules] = field(default_factory=dict)
    # Top-level rules of a quality.yaml whose contract has one table.
    only_table: TableRules | None = None

    def rules_for(self, table_key: str) -> TableRules | None:
        """Return the rules for one contract table."""

        return self.tables.get(table_key, self.only_table)

    @property
    def rule_count(self) -> int:
        rule_sets = [*self.tables.values(), *([self.only_table] if self.only_table else [])]
        return sum(
            (1 if rules.unique else 0)
            + sum(len(column) for column in rules.columns.values())
            + sum(len(column) for block in rules.when for column in block.columns.values())
            for rules in rule_sets
        )


class _WhenModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows: dict[str, str | int | float | bool]
    columns: dict[str, list[str | dict[str, Any]]]


class _TableModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    unique: list[str] = []
    columns: dict[str, list[str | dict[str, Any]]] = {}
    when: list[_WhenModel] = []


class _ConfigModel(_TableModel):
    version: Literal[1]
    tables: dict[str, _TableModel] = {}


def load_quality_config(path: Path) -> QualityConfig:
    """Load and validate one quality.yaml file."""

    if not path.exists():
        raise FileNotFoundError(f"Quality config does not exist: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid quality YAML: {path}") from exc

    return parse_quality_config(raw, source=str(path))


def parse_quality_config(raw: object, *, source: str = "<memory>") -> QualityConfig:
    """Validate an already parsed quality configuration."""

    try:
        model = _ConfigModel.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid quality config {source}: {exc}") from exc

    has_top_level_rules = bool(model.unique or model.columns or model.when)

    if has_top_level_rules and model.tables:
        raise ValueError(
            f"Invalid quality config {source}: write rules under 'tables' or at the top level "
            "for a one-table contract, not both"
        )

    tables = {
        table_key: _table_rules(table, where=f"{source}: tables.{table_key}")
        for table_key, table in model.tables.items()
    }
    only_table = _table_rules(model, where=source) if has_top_level_rules else None

    return QualityConfig(version=model.version, tables=tables, only_table=only_table)


def _table_rules(table: _TableModel, *, where: str) -> TableRules:
    when = []

    for i, block in enumerate(table.when):
        if not block.rows:
            raise ValueError(f"'rows' needs at least one column at {where}.when[{i}]")

        columns = _parse_columns(block.columns, where=f"{where}.when[{i}]")
        when.append(WhenRules(rows=dict(block.rows), columns=columns))

    return TableRules(
        unique=tuple(table.unique),
        columns=_parse_columns(table.columns, where=where),
        when=tuple(when),
    )


def _parse_columns(
    columns: dict[str, list[str | dict[str, Any]]], *, where: str
) -> dict[str, tuple[ColumnRule, ...]]:
    return {
        column: tuple(
            _parse_rule(item, where=f"{where}.columns.{column}[{i}]")
            for i, item in enumerate(items)
        )
        for column, items in columns.items()
    }


def _parse_rule(item: str | dict[str, Any], *, where: str) -> ColumnRule:
    """Read ``not_null``, ``{min: 0}`` or ``{min: 0, severity: warning}``."""

    if isinstance(item, str):
        if item not in FLAG_RULES:
            raise ValueError(f"Unknown rule {item!r} at {where}; use one of {sorted(FLAG_RULES)}")
        return ColumnRule(rule=item)

    options = dict(item)

    try:
        severity = QualitySeverity(options.pop("severity", QualitySeverity.BLOCKING))
    except ValueError as exc:
        known = [level.value for level in QualitySeverity]
        raise ValueError(f"Unknown severity at {where}; use one of {known}") from exc

    if len(options) != 1:
        raise ValueError(f"Write exactly one rule per item at {where}: {item!r}")

    ((rule, value),) = options.items()

    if rule in FLAG_RULES:
        if value is not True:
            raise ValueError(f"Rule {rule!r} takes the value true at {where}")
        return ColumnRule(rule=rule, severity=severity)

    if rule not in VALUE_RULES:
        known = sorted(FLAG_RULES | VALUE_RULES)
        raise ValueError(f"Unknown rule {rule!r} at {where}; use one of {known}")

    if rule in {"min", "max"} and not _is_bound(value):
        raise ValueError(f"Rule {rule!r} needs a number or {CURRENT_YEAR!r} at {where}")

    if rule == "between":
        if not (isinstance(value, list) and len(value) == 2 and all(map(_is_bound, value))):
            raise ValueError(f"Rule 'between' needs [low, high] at {where}")
        if all(map(_is_number, value)) and value[0] > value[1]:
            raise ValueError(f"Rule 'between' needs low <= high at {where}")
        value = tuple(value)

    if rule == "pattern":
        if not isinstance(value, str):
            raise ValueError(f"Rule 'pattern' needs a regular expression at {where}")
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(
                f"Rule 'pattern' is not a valid regular expression at {where}"
            ) from exc

    if rule in {"allowed", "forbidden"}:
        if not (isinstance(value, list) and value):
            raise ValueError(f"Rule {rule!r} needs a non-empty list at {where}")
        value = tuple(value)

    return ColumnRule(rule=rule, value=value, severity=severity)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_bound(value: object) -> bool:
    return _is_number(value) or value == CURRENT_YEAR


def contract_tables(contract: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Map each contract table to its published columns and their cast types."""

    return {
        str(table_key): {
            str(column["rename_to"]): str(column["cast_to"])
            for column in table.get("columns", {}).values()
        }
        for table_key, table in contract.get("tables", {}).items()
    }


def validate_quality_config(config: QualityConfig, contracts: Iterable[Mapping[str, Any]]) -> None:
    """Check that every rule points at a real table and column of the contracts."""

    tables: dict[str, dict[str, str]] = {}
    for contract in contracts:
        tables.update(contract_tables(contract))

    rule_sets = dict(config.tables)

    if config.only_table is not None:
        if len(tables) != 1:
            raise ValueError(
                "quality.yaml writes rules at the top level, which needs exactly one contract "
                f"table; contract tables: {sorted(tables)}. Put the rules under 'tables'."
            )

        (only_table_key,) = tables
        rule_sets[only_table_key] = config.only_table

    for table_key, rules in rule_sets.items():
        if table_key not in tables:
            raise ValueError(
                f"quality.yaml names table {table_key!r}, which no contract defines; "
                f"contract tables: {sorted(tables)}"
            )

        columns = tables[table_key]
        named = [*rules.unique, *rules.columns]
        column_rules = [*rules.columns.items()]

        for block in rules.when:
            named += [*block.rows, *block.columns]
            column_rules += block.columns.items()

        for column in named:
            if column not in columns:
                raise ValueError(
                    f"quality.yaml names column {column!r} in table {table_key!r}, "
                    f"which the contract does not publish; columns: {sorted(columns)}"
                )

        for column, rules_for_column in column_rules:
            for rule in rules_for_column:
                if rule.rule in NUMERIC_RULES and not _is_numeric_cast(columns[column]):
                    raise ValueError(
                        f"Rule {rule.rule!r} needs a numeric column, but "
                        f"{table_key}.{column} is cast to {columns[column]!r}"
                    )


def _is_numeric_cast(cast_to: str) -> bool:
    return cast_to in NUMERIC_CAST_TYPES or parse_decimal_cast_type(cast_to) is not None
