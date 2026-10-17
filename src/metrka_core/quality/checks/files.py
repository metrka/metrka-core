"""Checks for a landed source file, run before Bronze."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from metrka_core.quality.models import Outcome
from metrka_core.validation.preflight.xlsx_verify import verify_single_xlsx
from metrka_core.validation.preflight.zip_verify import verify_single_zip

_SHA256 = re.compile(r"[0-9a-f]{64}")


def file_not_empty(path: Path, *, min_bytes: int = 1) -> Outcome:
    """The file exists and has at least ``min_bytes`` byte(s)."""

    if not path.is_file():
        return Outcome.of(
            False,
            f"File does not exist: {path.name}",
            expected={"min_bytes": min_bytes},
            actual={"file_name": path.name, "exists": False},
        )

    size = path.stat().st_size

    return Outcome.of(
        size >= min_bytes,
        f"File has {size} bytes; minimum is {min_bytes}.",
        expected={"min_bytes": min_bytes},
        actual={"file_name": path.name, "exists": True, "file_size_bytes": size},
    )


def sha256_recorded(path: Path, sha256: str | None) -> Outcome:
    """A SHA-256 digest was recorded for the file."""

    passed = sha256 is not None and _SHA256.fullmatch(sha256) is not None

    return Outcome.of(
        passed,
        "SHA-256 was recorded." if passed else "SHA-256 is missing or malformed.",
        expected={"algorithm": "sha256", "hex_length": 64},
        actual={"file_name": path.name, "sha256": sha256},
    )


def payload_fingerprint_recorded(path: Path, fingerprint: Mapping[str, Any]) -> Outcome:
    """Files inside the archive were fingerprinted."""

    count = len(fingerprint)

    return Outcome.of(
        count > 0,
        f"Fingerprinted {count} archive member(s).",
        expected={"min_member_count": 1},
        actual={"file_name": path.name, "member_count": count, "members": sorted(fingerprint)},
    )


def zip_crc_valid(path: Path) -> Outcome:
    """Every member of the ZIP archive passes CRC verification."""

    result = verify_single_zip(path)

    return Outcome.of(
        bool(result.passed),
        "ZIP archive passed CRC verification."
        if result.passed
        else f"ZIP archive failed CRC verification: {result.error}",
        expected={"zip_crc_valid": True},
        actual={
            "file_name": path.name,
            "zip_crc_valid": bool(result.passed),
            "error": result.error,
        },
    )


def xlsx_package_integrity(path: Path) -> Outcome:
    """The XLSX file is a readable Excel package with at least one worksheet."""

    result = verify_single_xlsx(path)

    return Outcome.of(
        result.passed,
        "XLSX package is valid." if result.passed else f"XLSX package is invalid: {result.error}",
        expected={"valid_xlsx_package": True, "min_worksheets": 1},
        actual={
            "file_name": path.name,
            "valid_xlsx_package": result.passed,
            "crc_ok": result.crc_ok,
            "missing_members": list(result.missing_members),
            "worksheet_count": result.worksheet_count,
            "error": result.error,
        },
    )


def xlsx_has_data_rows(
    path: Path, *, sheet_name: str | int, header_row: int, min_rows: int = 1
) -> Outcome:
    """The configured XLSX sheet has at least ``min_rows`` non-empty row(s)."""

    frame = pd.read_excel(path, sheet_name=sheet_name, header=header_row)
    row_count = len(frame.dropna(axis="index", how="all").index)

    return Outcome.of(
        row_count >= min_rows,
        f"XLSX sheet has {row_count} data row(s); minimum is {min_rows}.",
        expected={"min_rows": min_rows},
        actual={
            "file_name": path.name,
            "row_count": row_count,
            "sheet_name": sheet_name,
            "header_row": header_row,
        },
    )
