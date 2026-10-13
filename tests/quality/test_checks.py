"""Built-in check functions: what each one passes, fails, and records."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import pytest

from metrka_core.quality.checks import files, outputs, tables
from metrka_core.quality.models import QualityOutputFile, QualityStatus

XLSX_MEMBERS = (
    "[Content_Types].xml",
    "_rels/.rels",
    "xl/workbook.xml",
    "xl/_rels/workbook.xml.rels",
    "xl/worksheets/sheet1.xml",
)


def _write_xlsx_package(path: Path) -> None:
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for member in XLSX_MEMBERS:
            archive.writestr(member, b"<xml />")


# --- files -------------------------------------------------------------------


def test_file_not_empty_passes_and_records_only_the_file_name(tmp_path: Path) -> None:
    path = tmp_path / "source.csv"
    path.write_text("a\n1\n", encoding="utf-8")

    outcome = files.file_not_empty(path)

    assert outcome.status is QualityStatus.PASSED
    assert outcome.actual["file_name"] == "source.csv"
    assert str(tmp_path) not in json.dumps(outcome.actual)


def test_file_not_empty_fails_for_missing_or_small_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"

    assert files.file_not_empty(path).status is QualityStatus.FAILED

    path.write_bytes(b"")
    assert files.file_not_empty(path).status is QualityStatus.FAILED


def test_sha256_recorded_requires_64_lowercase_hex_characters(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"

    assert files.sha256_recorded(path, "a" * 64).status is QualityStatus.PASSED
    assert files.sha256_recorded(path, "abc").status is QualityStatus.FAILED
    assert files.sha256_recorded(path, None).status is QualityStatus.FAILED


def test_zip_crc_valid_passes_for_a_readable_archive(tmp_path: Path) -> None:
    path = tmp_path / "data.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("data.csv", "a\n1\n")

    assert files.zip_crc_valid(path).status is QualityStatus.PASSED


def test_xlsx_package_integrity_passes_and_fails(tmp_path: Path) -> None:
    valid = tmp_path / "valid.xlsx"
    _write_xlsx_package(valid)
    invalid = tmp_path / "invalid.xlsx"
    invalid.write_bytes(b"not a zip")

    assert files.xlsx_package_integrity(valid).status is QualityStatus.PASSED
    assert files.xlsx_package_integrity(invalid).status is QualityStatus.FAILED


def test_xlsx_has_data_rows_fails_for_header_only_sheet(tmp_path: Path) -> None:
    with_rows = tmp_path / "rows.xlsx"
    pd.DataFrame({"County": ["Alachua"], "Count": [3]}).to_excel(with_rows, index=False)
    header_only = tmp_path / "header.xlsx"
    pd.DataFrame({"County": [], "Count": []}).to_excel(header_only, index=False)

    passed = files.xlsx_has_data_rows(with_rows, sheet_name=0, header_row=0)
    failed = files.xlsx_has_data_rows(header_only, sheet_name=0, header_row=0)

    assert passed.status is QualityStatus.PASSED
    assert failed.status is QualityStatus.FAILED
    assert failed.actual["row_count"] == 0


# --- outputs -----------------------------------------------------------------


def test_output_files_created_reports_relative_paths_only(tmp_path: Path) -> None:
    good = tmp_path / "good.csv"
    good.write_text("a\n", encoding="utf-8")
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    files_ = (
        QualityOutputFile(local_path=good, workspace_relative_path="files/bronze/good.csv"),
        QualityOutputFile(local_path=empty, workspace_relative_path="files/bronze/empty.csv"),
        QualityOutputFile(
            local_path=tmp_path / "missing.csv", workspace_relative_path="files/bronze/missing.csv"
        ),
    )

    outcome = outputs.output_files_created(files_)

    assert outcome.status is QualityStatus.FAILED
    assert outcome.actual["missing_files"] == ["files/bronze/missing.csv"]
    assert outcome.actual["undersized_files"] == ["files/bronze/empty.csv"]
    assert str(tmp_path) not in json.dumps(outcome.actual)


def test_output_files_created_is_skipped_when_no_output_was_required() -> None:
    assert outputs.output_files_created((), required=False).status is QualityStatus.SKIPPED


def test_quality_output_file_rejects_absolute_persisted_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        QualityOutputFile(local_path=tmp_path / "a.csv", workspace_relative_path=str(tmp_path))


# --- tables ------------------------------------------------------------------


def test_has_rows_and_columns_match() -> None:
    table = pd.DataFrame({"a": [1], "b": [2]})

    assert tables.has_rows(table).status is QualityStatus.PASSED
    assert tables.has_rows(table.iloc[0:0]).status is QualityStatus.FAILED
    assert (
        tables.columns_match(table, expected=["a"], allow_extra=True).status is QualityStatus.PASSED
    )

    exact = tables.columns_match(table, expected=["a", "c"], allow_extra=False)
    assert exact.status is QualityStatus.FAILED
    assert exact.actual["missing_columns"] == ["c"]
    assert exact.actual["unexpected_columns"] == ["b"]


def test_not_null_counts_missing_values() -> None:
    table = pd.DataFrame({"county": ["A", None, "C"]})

    outcome = tables.not_null(table, column="county")

    assert outcome.status is QualityStatus.FAILED
    assert outcome.actual["failed_row_count"] == 1


def test_unique_reports_duplicated_rows() -> None:
    table = pd.DataFrame({"county": ["A", "A", "B"], "year": [2020, 2020, 2020]})

    outcome = tables.unique(table, columns=("county", "year"))

    assert outcome.status is QualityStatus.FAILED
    assert outcome.actual["duplicated_row_count"] == 2
    assert outcome.actual["examples"] == [["A", 2020], ["A", 2020]]


def test_min_value_ignores_missing_values_and_records_examples() -> None:
    table = pd.DataFrame({"count": pd.array([3, None, -2, -1], dtype="Int64")})

    outcome = tables.min_value(table, column="count", minimum=0)

    assert outcome.status is QualityStatus.FAILED
    assert outcome.actual == {"failed_row_count": 2, "examples": [-2, -1]}
    json.dumps(outcome.actual)


def test_min_value_compares_decimals_exactly() -> None:
    table = pd.DataFrame({"rate": [Decimal("0.1"), Decimal("0.2")]}, dtype=object)

    assert tables.min_value(table, column="rate", minimum=0.1).status is QualityStatus.PASSED
    assert tables.max_value(table, column="rate", maximum=0.1).status is QualityStatus.FAILED


def test_between_and_allowed_values() -> None:
    table = pd.DataFrame({"year": [1989, 2000, 2031], "kind": ["a", "b", None]})

    between = tables.between(table, column="year", low=1990, high=2030)
    allowed = tables.allowed_values(table, column="kind", values=("a",))

    assert between.actual["failed_row_count"] == 2
    assert allowed.actual == {"failed_row_count": 1, "examples": ["b"]}


def test_decimal_examples_are_json_safe() -> None:
    table = pd.DataFrame({"rate": [Decimal("-1.5")]}, dtype=object)

    outcome = tables.min_value(table, column="rate", minimum=0)

    assert outcome.actual["examples"] == ["-1.5"]
    json.dumps(outcome.actual)
