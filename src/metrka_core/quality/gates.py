"""Run the quality checks of each pipeline gate and record the evidence.

The pipeline calls one function per gate and passes what it has in hand:

    check_landed_file    pre_bronze   a landed source file
    check_bronze_output  post_bronze  files written to Bronze
    check_silver_input   pre_silver   the table read from Bronze
    check_silver_output  post_silver  the finished table, plus quality.yaml rules

Each function decides which checks apply, runs them, writes one evidence row per
check through the store, and returns the gate result.
"""

from __future__ import annotations

import inspect
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from metrka_core.quality.checks import files, outputs, tables
from metrka_core.quality.config import CURRENT_YEAR, ColumnRule, QualityConfig
from metrka_core.quality.models import (
    Outcome,
    QualityGate,
    QualityGateResult,
    QualityOutputFile,
    QualitySeverity,
    QualityStatus,
)
from metrka_core.quality.store import QualityCheckStore

if TYPE_CHECKING:
    from metrka_core.pipeline.bronze.unpack_zip import ZipExtractResult


@dataclass(frozen=True, slots=True)
class RunIds:
    """Identifiers written on every evidence row."""

    dataset_id: str
    run_id: str
    pipeline_run_id: str | None = None
    dataset_file_id: str | None = None
    silver_build_id: str | None = None


@dataclass(frozen=True, slots=True)
class LandedFile:
    """A source file in the landing zone."""

    path: Path
    sha256: str
    fingerprint: Mapping[str, Any]
    xlsx_sheet_name: str | int | None = None
    xlsx_header_row: int | None = None


@dataclass(frozen=True, slots=True)
class BronzeOutput:
    """Files written to Bronze for one source file or batch."""

    files: tuple[QualityOutputFile, ...]
    required: bool = True
    extraction: ZipExtractResult | None = None
    requested_extract_count: int = 0


@dataclass(frozen=True, slots=True)
class SilverTable:
    """A Silver table and the columns its contract expects."""

    table_key: str
    frame: pd.DataFrame
    expected_columns: tuple[str, ...]
    source_file_name: str
    output_files: tuple[QualityOutputFile, ...] = ()


@dataclass(frozen=True, slots=True)
class _Check:
    """One check ready to run: its id suffix, display name, target, and the call."""

    key: str
    name: str
    target: str
    run: partial[Outcome]
    severity: QualitySeverity = QualitySeverity.BLOCKING


def _check(
    key: str,
    name: str,
    target: str,
    function: Callable[..., Outcome],
    *checked: Any,
    severity: QualitySeverity = QualitySeverity.BLOCKING,
    **settings: Any,
) -> _Check:
    """Bind a check function to what it checks (positional) and its settings (keywords)."""

    return _Check(key, name, target, partial(function, *checked, **settings), severity)


def check_landed_file(
    file: LandedFile, *, ids: RunIds, store: QualityCheckStore
) -> QualityGateResult:
    """Pre-Bronze: the landed file is intact. Checks depend on the file type."""

    path = file.path
    suffix = path.suffix.casefold()
    checks = [
        _check("file_not_empty", "Source file is not empty", "file", files.file_not_empty, path),
        _check(
            "sha256_recorded",
            "Source file SHA-256 was recorded",
            "file",
            files.sha256_recorded,
            path,
            file.sha256,
        ),
    ]

    if suffix == ".zip":
        checks.append(
            _check(
                "zip_crc_valid", "ZIP archive is not corrupted", "file", files.zip_crc_valid, path
            )
        )
        checks.append(
            _check(
                "payload_fingerprint_recorded",
                "Files inside the archive were fingerprinted",
                "file",
                files.payload_fingerprint_recorded,
                path,
                file.fingerprint,
            )
        )

    if suffix == ".xlsx":
        checks.append(
            _check(
                "xlsx_package_integrity",
                "XLSX package is valid",
                "file",
                files.xlsx_package_integrity,
                path,
            )
        )

        if file.xlsx_sheet_name is not None and file.xlsx_header_row is not None:
            checks.append(
                _check(
                    "xlsx_has_data_rows",
                    "XLSX sheet contains data rows",
                    "file",
                    files.xlsx_has_data_rows,
                    path,
                    sheet_name=file.xlsx_sheet_name,
                    header_row=file.xlsx_header_row,
                )
            )

    details = {"source_file_name": path.name}
    return _run_gate(QualityGate.PRE_BRONZE, checks, ids=ids, store=store, details=details)


def check_bronze_output(
    output: BronzeOutput, *, ids: RunIds, store: QualityCheckStore
) -> QualityGateResult:
    """Post-Bronze: the expected Bronze files exist and extraction succeeded."""

    target = "bronze_output"
    checks = [
        _check(
            "output_files_created",
            "Bronze output files were created",
            target,
            outputs.output_files_created,
            output.files,
            required=output.required,
        )
    ]

    if output.extraction is not None:
        checks.append(
            _check(
                "bronze_extraction_completed",
                "Source archive was extracted",
                target,
                outputs.bronze_extraction_completed,
                output.extraction,
                requested_count=output.requested_extract_count,
            )
        )

    return _run_gate(QualityGate.POST_BRONZE, checks, ids=ids, store=store, details={})


def check_silver_input(
    table: SilverTable, *, ids: RunIds, store: QualityCheckStore
) -> QualityGateResult:
    """Pre-Silver: the Bronze table has rows and every contract source column."""

    key, frame = table.table_key, table.frame
    target = f"table_key={key}"
    checks = [
        _check(f"{key}.has_rows", "Input table has rows", target, tables.has_rows, frame),
        _check(
            f"{key}.columns_match",
            "Input table has the contract columns",
            target,
            tables.columns_match,
            frame,
            expected=table.expected_columns,
            allow_extra=True,
        ),
    ]

    details = _table_details(table)
    return _run_gate(QualityGate.PRE_SILVER, checks, ids=ids, store=store, details=details)


def check_silver_output(
    table: SilverTable,
    *,
    config: QualityConfig,
    ids: RunIds,
    store: QualityCheckStore,
    as_of: date | None = None,
) -> QualityGateResult:
    """Post-Silver: the finished table matches the contract and the quality.yaml rules.

    ``as_of`` is the date that ``current_year`` in quality.yaml refers to; today by default.
    """

    year = (as_of or datetime.now(UTC).date()).year

    key, frame = table.table_key, table.frame
    target = f"table_key={key}"
    checks = [
        _check(f"{key}.has_rows", "Table has rows", target, tables.has_rows, frame),
        _check(
            f"{key}.columns_match",
            "Table has exactly the contract columns",
            target,
            tables.columns_match,
            frame,
            expected=table.expected_columns,
            allow_extra=False,
        ),
        _check(
            f"{key}.output_files_created",
            "Silver files were created",
            target,
            outputs.output_files_created,
            table.output_files,
        ),
    ]

    rules = config.rules_for(key)

    if rules is not None:
        if rules.unique:
            name = f"Rows are unique by {', '.join(rules.unique)}"
            checks.append(
                _check(f"{key}.unique", name, target, tables.unique, frame, columns=rules.unique)
            )

        for column, column_rules in rules.columns.items():
            checks += [_column_check(table, column, rule, year) for rule in column_rules]

        for block in rules.when:
            for column, column_rules in block.columns.items():
                checks += [
                    _column_check(table, column, rule, year, rows=block.rows)
                    for rule in column_rules
                ]

    details = _table_details(table)
    return _run_gate(QualityGate.POST_SILVER, checks, ids=ids, store=store, details=details)


def _column_check(
    table: SilverTable,
    column: str,
    rule: ColumnRule,
    year: int,
    *,
    rows: Mapping[str, Any] | None = None,
) -> _Check:
    """Turn one quality.yaml rule, e.g. ``min: 0``, into a check.

    With ``rows``, the rule applies only to rows whose columns equal those values.
    """

    value = _resolve_current_year(rule.value, year)
    function: Callable[..., Outcome]
    settings: dict[str, Any]

    if rule.rule == "not_null":
        function, settings, label = tables.not_null, {"column": column}, "is never missing"
    elif rule.rule == "is_null":
        function, settings, label = tables.is_null, {"column": column}, "is always missing"
    elif rule.rule == "unique":
        function, settings, label = tables.unique, {"columns": (column,)}, "has unique values"
    elif rule.rule == "min":
        function, settings, label = (
            tables.min_value,
            {"column": column, "minimum": value},
            f">= {value}",
        )
    elif rule.rule == "max":
        function, settings, label = (
            tables.max_value,
            {"column": column, "maximum": value},
            f"<= {value}",
        )
    elif rule.rule == "between":
        low, high = value
        function, label = tables.between, f"between {low} and {high}"
        settings = {"column": column, "low": low, "high": high}

    elif rule.rule == "allowed":
        function, label = tables.allowed_values, f"one of {list(value)}"
        settings = {"column": column, "values": value}

    elif rule.rule == "pattern":
        function, label = tables.matches_pattern, f"matches {value}"
        settings = {"column": column, "pattern": value}

    elif rule.rule == "forbidden":
        function, label = tables.forbidden_values, f"is never one of {list(value)}"
        settings = {"column": column, "values": value}
    else:
        raise ValueError(f"Unknown quality rule {rule.rule!r}")

    key = table.table_key
    name = f"{column} {label}"
    target = f"table_key={key};column={column}"
    frame = table.frame

    if rows:
        condition = ", ".join(
            f"{row_column} = {row_value}" for row_column, row_value in rows.items()
        )
        key += ".where_" + "_".join(
            f"{row_column}={row_value}" for row_column, row_value in rows.items()
        )
        name += f" where {condition}"
        target += f";where={condition}"
        frame = _matching_rows(frame, rows)

    return _check(
        f"{key}.{column}.{rule.rule}",
        name,
        target,
        function,
        frame,
        severity=rule.severity,
        **settings,
    )


def _matching_rows(frame: pd.DataFrame, rows: Mapping[str, Any]) -> pd.DataFrame:
    """Rows where every named column equals its value."""

    mask = pd.Series(True, index=frame.index)

    for column, value in rows.items():
        mask &= frame[column].eq(value).fillna(False).astype(bool)

    return frame[mask]


def _resolve_current_year(value: Any, year: int) -> Any:
    """Replace ``current_year`` in a rule value with the actual year."""

    if value == CURRENT_YEAR:
        return year

    if isinstance(value, tuple):
        return tuple(_resolve_current_year(item, year) for item in value)

    return value


def _table_details(table: SilverTable) -> dict[str, Any]:
    return {"table_key": table.table_key, "source_file_name": table.source_file_name}


def _run_gate(
    gate: QualityGate,
    checks: list[_Check],
    *,
    ids: RunIds,
    store: QualityCheckStore,
    details: dict[str, Any],
) -> QualityGateResult:
    """Run each check, record its evidence, and summarize the gate."""

    if not checks:
        raise ValueError(f"Quality gate {gate.value!r} has no checks to run")

    results: list[tuple[str, QualitySeverity, Outcome]] = []

    for check in checks:
        check_id = f"{ids.dataset_id}.{gate.value}.{check.key}"
        started = time.perf_counter()

        try:
            outcome = check.run()
        except Exception as exc:
            outcome = Outcome(
                QualityStatus.ERROR,
                f"{type(exc).__name__}: {exc}",
                actual={"error_type": type(exc).__name__, "error_message": str(exc)},
            )

        duration_ms = int((time.perf_counter() - started) * 1000)
        _record(store, gate, check, check_id, outcome, duration_ms, ids, details)
        results.append((check_id, check.severity, outcome))

    return _summarize(results)


def _record(
    store: QualityCheckStore,
    gate: QualityGate,
    check: _Check,
    check_id: str,
    outcome: Outcome,
    duration_ms: int,
    ids: RunIds,
    details: dict[str, Any],
) -> None:
    """Write the check definition and this run's result."""

    function = check.run.func
    params = {name: _plain(value) for name, value in check.run.keywords.items()}

    store.upsert_quality_check_definition(
        {
            "check_id": check_id,
            "check_name": check.name,
            "check_type": check.key.rsplit(".", maxsplit=1)[-1],
            "layer": gate.layer,
            "target": check.target,
            "severity": check.severity.value,
            "description": _describe(function, params),
            "code_ref": f"{function.__module__}.{function.__qualname__}",
            "default_params": params,
            "is_active": True,
        }
    )
    store.insert_quality_check_run(
        {
            "pipeline_run_id": ids.pipeline_run_id,
            "check_id": check_id,
            "dataset_id": ids.dataset_id,
            "dataset_file_id": ids.dataset_file_id,
            "silver_build_id": ids.silver_build_id,
            "run_id": ids.run_id,
            "step_id": "quality_" + check_id.replace(".", "_"),
            "status": outcome.status.value,
            "expected": outcome.expected,
            "actual": outcome.actual,
            "result_summary": outcome.summary,
            "details": {**details, "quality_gate": gate.value},
            "params": params,
            "duration_ms": duration_ms,
        }
    )


def _summarize(results: list[tuple[str, QualitySeverity, Outcome]]) -> QualityGateResult:
    """A gate fails when a blocking check failed or raised."""

    bad = {QualityStatus.FAILED, QualityStatus.ERROR}
    blocked = [
        outcome
        for _, severity, outcome in results
        if outcome.status in bad and severity.blocks_promotion
    ]

    def count(status: QualityStatus) -> int:
        return sum(1 for _, _, outcome in results if outcome.status is status)

    return QualityGateResult(
        status=(QualityStatus.FAILED if blocked else QualityStatus.PASSED).value,
        checks_run=len(results),
        passed_count=count(QualityStatus.PASSED),
        failed_count=count(QualityStatus.FAILED),
        skipped_count=count(QualityStatus.SKIPPED),
        blocked_count=len(blocked),
        error_count=count(QualityStatus.ERROR),
        failed_check_ids=[check_id for check_id, _, outcome in results if outcome.status in bad],
        error_message=blocked[0].summary if blocked else None,
    )


def _first_line(text: str | None) -> str | None:
    lines = (text or "").strip().splitlines()
    return lines[0] if lines else None


_SETTING = re.compile(r"``(\w+)``")


def _describe(function: Callable[..., object], params: Mapping[str, object]) -> str | None:
    """A check's first docstring line with its own settings: ``minimum`` reads 0.

    A setting the check was not given reads as the check's default.
    """

    line = _first_line(function.__doc__)
    if line is None:
        return None

    defaults = {
        name: parameter.default
        for name, parameter in inspect.signature(function).parameters.items()
        if parameter.default is not inspect.Parameter.empty
    }
    settings = {**defaults, **params}

    def setting(match: re.Match[str]) -> str:
        name = match.group(1)
        return _setting_text(settings[name]) if name in settings else name

    return _SETTING.sub(setting, line)


def _setting_text(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(_setting_text(item) for item in value)

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value)


def _plain(value: object) -> object:
    """Make a check setting safe for JSON evidence."""

    if isinstance(value, tuple):
        return [_plain(item) for item in value]

    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value

    return str(value)
