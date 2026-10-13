"""Combine XLSX source files into one typed Bronze table."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class BronzeAssemblyConfig:
    """Configuration for producing one Bronze table from source files."""

    strategy: BronzeAssemblyStrategy
    output_filename: str
    read_config: XlsxReadConfig

    def __post_init__(self) -> None:
        output_path = Path(self.output_filename)

        if not self.output_filename.strip():
            raise ValueError("Bronze assembly output_filename must not be empty")

        if output_path.name != self.output_filename:
            raise ValueError("Bronze assembly output_filename must contain only a filename")

        if output_path.suffix.casefold() != ".csv":
            raise ValueError("XLSX row assembly output_filename must end with .csv")


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
