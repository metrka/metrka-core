"""Combine XLSX source files into typed Bronze tables.

A stream normally combines all its files into one Bronze CSV. With ``group_files_by`` the
files are sorted, whole, into one CSV per value of a filename metadata column, such as a
rate type. No row is split: each source file goes into exactly one output.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd

from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataConfig,
    extract_filename_metadata,
    index_paths_by_filename_member_key,
)


@dataclass(frozen=True, slots=True)
class XlsxReadConfig:
    """Options used when reading source XLSX files."""

    sheet_name: str | int = 0
    header_row: int = 0
    drop_fully_empty_rows: bool = True
    drop_fully_empty_columns: bool = True

    def __post_init__(self) -> None:
        if self.header_row < 0:
            raise ValueError("XLSX header_row must not be negative")


type BronzeAssemblyStrategy = Literal["xlsx_rows"]


def _check_output_filename(name: str, *, label: str) -> None:
    output_path = Path(name)

    if not name.strip():
        raise ValueError(f"Bronze assembly {label} must not be empty")

    if output_path.name != name:
        raise ValueError(f"Bronze assembly {label} must contain only a filename")

    if output_path.suffix.casefold() != ".csv":
        raise ValueError(f"XLSX row assembly {label} must end with .csv")


@dataclass(frozen=True, slots=True)
class BronzeAssemblyConfig:
    """Configuration for producing Bronze tables from source files.

    Either ``output_filename`` names the one CSV that all files go into, or
    ``group_files_by`` names a filename metadata column and ``outputs`` maps each of its
    values to the CSV that files with that value go into.
    """

    strategy: BronzeAssemblyStrategy
    output_filename: str | None
    read_config: XlsxReadConfig
    group_files_by: str | None = None
    outputs: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.group_files_by is None:
            if self.outputs:
                raise ValueError("Bronze assembly outputs need group_files_by")

            if self.output_filename is None:
                raise ValueError("Bronze assembly needs output_filename or group_files_by")

            _check_output_filename(self.output_filename, label="output_filename")
            return

        if not self.group_files_by.strip():
            raise ValueError("Bronze assembly group_files_by must not be empty")

        if self.output_filename is not None:
            raise ValueError("Bronze assembly sets output_filename or group_files_by, not both")

        if not self.outputs:
            raise ValueError("Bronze assembly group_files_by needs outputs")

        values = [value for value, _ in self.outputs]
        filenames = [filename for _, filename in self.outputs]

        if len(set(values)) != len(values):
            raise ValueError("Bronze assembly outputs repeat a group value")

        if len({filename.casefold() for filename in filenames}) != len(filenames):
            raise ValueError("Bronze assembly outputs repeat an output filename")

        for value, filename in self.outputs:
            if not value.strip():
                raise ValueError("Bronze assembly outputs need non-empty group values")

            _check_output_filename(filename, label=f"outputs[{value!r}]")

    @property
    def output_filenames(self) -> tuple[str, ...]:
        """Every Bronze CSV this assembly writes."""
        if self.output_filename is not None:
            return (self.output_filename,)

        return tuple(filename for _, filename in self.outputs)


@dataclass(frozen=True, slots=True)
class XlsxRowAssemblyResult:
    """Result of writing one combined Bronze CSV."""

    output_path: Path
    input_file_count: int
    row_count: int
    column_count: int


def assemble_xlsx_rows(
    *, paths: Iterable[Path], filename_config: FilenameMetadataConfig, read_config: XlsxReadConfig
) -> pd.DataFrame:
    """Read XLSX files and combine their rows with filename metadata."""
    indexed_paths = index_paths_by_filename_member_key(paths, filename_config)

    if not indexed_paths:
        raise ValueError("No XLSX files were provided for row assembly")

    frames: list[pd.DataFrame] = []
    reference_path: Path | None = None
    reference_columns: tuple[str, ...] | None = None

    for path in indexed_paths.values():
        frame = pd.read_excel(
            path, sheet_name=read_config.sheet_name, header=read_config.header_row
        )

        source_columns = tuple(str(column) for column in frame.columns)

        if reference_columns is None:
            reference_path = path
            reference_columns = source_columns
        elif source_columns != reference_columns:
            if reference_path is None:
                raise RuntimeError("XLSX reference path was not initialized")

            missing_columns = [
                column for column in reference_columns if column not in source_columns
            ]
            extra_columns = [column for column in source_columns if column not in reference_columns]
            reordered = not missing_columns and not extra_columns

            raise ValueError(
                "XLSX source columns do not match: "
                f"reference={reference_path.name}; "
                f"actual={path.name}; "
                f"expected={list(reference_columns)!r}; "
                f"actual_columns={list(source_columns)!r}; "
                f"missing={missing_columns!r}; "
                f"extra={extra_columns!r}; "
                f"reordered={reordered}"
            )

        metadata = extract_filename_metadata(path.name, filename_config)

        conflicting_columns = set(frame.columns) & set(metadata)

        if conflicting_columns:
            raise ValueError(
                f"XLSX file {path.name} already contains configured "
                "filename metadata columns: "
                + ", ".join(sorted(str(value) for value in conflicting_columns))
            )

        if read_config.drop_fully_empty_rows:
            frame = frame.dropna(axis="index", how="all")

        for column_name, value in metadata.items():
            frame[column_name] = value

        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)

    if read_config.drop_fully_empty_columns:
        combined = combined.dropna(axis="columns", how="all")

    return combined


def write_assembled_xlsx_csv(
    *,
    paths: Iterable[Path],
    output_path: Path,
    filename_config: FilenameMetadataConfig,
    read_config: XlsxReadConfig,
) -> XlsxRowAssemblyResult:
    """Combine source XLSX files and write one deterministic CSV."""
    input_paths = tuple(paths)

    if output_path.suffix.casefold() != ".csv":
        raise ValueError("Combined XLSX output path must end with .csv")

    combined = assemble_xlsx_rows(
        paths=input_paths, filename_config=filename_config, read_config=read_config
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    combined.to_csv(output_path, index=False, lineterminator="\n")

    return XlsxRowAssemblyResult(
        output_path=output_path,
        input_file_count=len(input_paths),
        row_count=len(combined.index),
        column_count=len(combined.columns),
    )


def group_paths_by_filename_value(
    *,
    paths: Iterable[Path],
    filename_config: FilenameMetadataConfig,
    group_files_by: str,
    outputs: tuple[tuple[str, str], ...],
) -> dict[str, tuple[Path, ...]]:
    """Sort whole source files into output CSV names by one filename metadata value.

    Every configured output must receive at least one file, and every file's value must be
    configured, so that no file and no table disappears silently.
    """
    if group_files_by not in filename_config.columns:
        raise ValueError(
            f"Bronze assembly group_files_by {group_files_by!r} is not a filename metadata "
            f"column: {sorted(filename_config.columns)}"
        )

    output_by_value = dict(outputs)
    grouped: dict[str, list[Path]] = {filename: [] for _, filename in outputs}
    unknown: dict[str, list[str]] = {}

    for path in sorted(paths, key=lambda item: (item.name.casefold(), item.as_posix())):
        value = extract_filename_metadata(path.name, filename_config)[group_files_by]
        output_filename = output_by_value.get(str(value)) if value is not None else None

        if output_filename is None:
            unknown.setdefault(str(value), []).append(path.name)
            continue

        grouped[output_filename].append(path)

    if unknown:
        details = "; ".join(
            f"{value!r}: {', '.join(names[:3])}{' …' if len(names) > 3 else ''}"
            for value, names in sorted(unknown.items())
        )
        raise ValueError(
            f"Files have {group_files_by} values that no output names: {details}. "
            f"Configured values: {sorted(output_by_value)}"
        )

    empty = [value for value, filename in outputs if not grouped[filename]]

    if empty:
        raise ValueError(
            f"No files have {group_files_by} value(s) {empty!r}; every configured output "
            "needs at least one file"
        )

    return {filename: tuple(group) for filename, group in grouped.items()}


def write_assembled_xlsx_csvs(
    *,
    paths: Iterable[Path],
    output_dir: Path,
    filename_config: FilenameMetadataConfig,
    assembly_config: BronzeAssemblyConfig,
) -> tuple[XlsxRowAssemblyResult, ...]:
    """Write every Bronze CSV the assembly configures, in output-name order."""
    input_paths = tuple(paths)

    if assembly_config.group_files_by is None:
        if assembly_config.output_filename is None:
            raise ValueError("Bronze assembly needs output_filename or group_files_by")

        return (
            write_assembled_xlsx_csv(
                paths=input_paths,
                output_path=output_dir / assembly_config.output_filename,
                filename_config=filename_config,
                read_config=assembly_config.read_config,
            ),
        )

    grouped = group_paths_by_filename_value(
        paths=input_paths,
        filename_config=filename_config,
        group_files_by=assembly_config.group_files_by,
        outputs=assembly_config.outputs,
    )

    # Assemble every group before writing any, so a mismatch leaves no partial output.
    assembled = [
        (
            filename,
            len(group),
            assemble_xlsx_rows(
                paths=group,
                filename_config=filename_config,
                read_config=assembly_config.read_config,
            ),
        )
        for filename, group in sorted(grouped.items())
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for filename, input_file_count, combined in assembled:
        output_path = output_dir / filename
        combined.to_csv(output_path, index=False, lineterminator="\n")
        results.append(
            XlsxRowAssemblyResult(
                output_path=output_path,
                input_file_count=input_file_count,
                row_count=len(combined.index),
                column_count=len(combined.columns),
            )
        )

    return tuple(results)
