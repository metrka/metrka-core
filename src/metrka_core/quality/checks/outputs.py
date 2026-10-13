"""Checks for files the pipeline produced."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from metrka_core.quality.models import Outcome, QualityOutputFile

if TYPE_CHECKING:
    from metrka_core.pipeline.bronze.unpack_zip import ZipExtractResult


def output_files_created(
    files: Sequence[QualityOutputFile],
    *,
    required: bool = True,
    min_files: int = 1,
    min_file_bytes: int = 1,
) -> Outcome:
    """Enough non-empty output files exist. Skipped when no output was required."""

    if not required:
        return Outcome.skipped("No new output was required.")

    paths = [file.workspace_relative_path for file in files]
    missing = [file.workspace_relative_path for file in files if not file.local_path.is_file()]
    undersized = [
        file.workspace_relative_path
        for file in files
        if file.local_path.is_file() and file.local_path.stat().st_size < min_file_bytes
    ]
    passed = len(files) >= min_files and not missing and not undersized

    return Outcome.of(
        passed,
        f"Created {len(files)} output file(s)."
        if passed
        else "Output files are missing, empty, or fewer than expected.",
        expected={"min_files": min_files, "min_file_bytes": min_file_bytes},
        actual={
            "output_file_count": len(files),
            "output_files": paths,
            "missing_files": missing,
            "undersized_files": undersized,
        },
    )


def bronze_extraction_completed(result: ZipExtractResult, *, requested_count: int) -> Outcome:
    """Every requested ZIP member was extracted."""

    return Outcome.of(
        result.passed,
        f"Extracted {result.extracted_count} file(s) to Bronze."
        if result.passed
        else f"Bronze extraction failed: {result.error}",
        expected={"extracted_count": requested_count},
        actual={
            "extracted_count": result.extracted_count,
            "extracted_files": list(result.extracted_files),
            "error": result.error,
        },
    )
