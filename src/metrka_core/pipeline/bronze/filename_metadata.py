"""Configurable metadata extraction from landed filenames."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

type FilenameMetadataValue = str | int | None
type FilenameMetadataValueType = Literal["string", "integer"]


@dataclass(frozen=True, slots=True)
class FilenameMetadataColumn:
    """Describe one metadata column extracted from a regex group."""

    from_group: str
    value_type: FilenameMetadataValueType
    null_values: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FilenameMetadataConfig:
    """Configure metadata extraction from source filenames."""

    regex: str
    columns: dict[str, FilenameMetadataColumn]
    member_key: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.regex.strip():
            raise ValueError("Filename metadata regex must not be empty")

        try:
            compiled_regex = re.compile(self.regex)
        except re.error as error:
            raise ValueError(f"Invalid filename metadata regex: {error}") from error

        available_groups = set(compiled_regex.groupindex)

        unknown_groups = {
            column.from_group
            for column in self.columns.values()
            if column.from_group not in available_groups
        }

        if unknown_groups:
            raise ValueError(
                "Filename metadata column uses unknown regex group: "
                + ", ".join(sorted(unknown_groups))
            )

        if not self.columns:
            raise ValueError("Filename metadata columns must not be empty")

        if not self.member_key:
            raise ValueError("Filename metadata member_key must not be empty")

        unknown_keys = set(self.member_key) - set(self.columns)

        if unknown_keys:
            raise ValueError(
                "Filename metadata member_key contains unknown columns: "
                + ", ".join(sorted(unknown_keys))
            )


def extract_filename_metadata(
    filename: str, config: FilenameMetadataConfig
) -> dict[str, FilenameMetadataValue]:
    """Extract configured metadata values from one filename."""
    match = re.fullmatch(config.regex, filename)

    if match is None:
        raise ValueError(f"Filename does not match configured metadata pattern: {filename}")

    values: dict[str, FilenameMetadataValue] = {}

    for column_name, column in config.columns.items():
        raw_value = match.group(column.from_group)

        if raw_value in column.null_values:
            values[column_name] = None
        elif column.value_type == "string":
            values[column_name] = raw_value
        else:
            values[column_name] = int(raw_value)

    return values


def build_filename_member_key(
    metadata: dict[str, FilenameMetadataValue], config: FilenameMetadataConfig
) -> tuple[FilenameMetadataValue, ...]:
    """Build a stable logical key for one source file."""
    missing_columns = [
        column_name for column_name in config.member_key if column_name not in metadata
    ]

    if missing_columns:
        raise ValueError(
            "Filename metadata is missing member key columns: " + ", ".join(missing_columns)
        )

    return tuple(metadata[column_name] for column_name in config.member_key)


def index_paths_by_filename_member_key(
    paths: Iterable[Path], config: FilenameMetadataConfig
) -> dict[tuple[FilenameMetadataValue, ...], Path]:
    """Index source paths by their configured logical member key."""
    indexed_paths: dict[tuple[FilenameMetadataValue, ...], Path] = {}

    ordered_paths = sorted(paths, key=lambda path: (path.name.casefold(), path.as_posix()))

    for path in ordered_paths:
        metadata = extract_filename_metadata(path.name, config)
        member_key = build_filename_member_key(metadata, config)

        existing_path = indexed_paths.get(member_key)

        if existing_path is not None:
            raise ValueError(
                f"Duplicate filename member key {member_key!r}: {existing_path} and {path}"
            )

        indexed_paths[member_key] = path

    return indexed_paths
