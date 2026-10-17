"""Domain models for published Silver table assets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import PurePosixPath
from typing import Any


@dataclass(frozen=True)
class DatasetPublicationAssetRequest:
    """One table file recorded during publication."""

    table_key: str
    file_path: str
    file_format: str
    row_count: int
    column_count: int
    columns: tuple[str, ...]
    size_bytes: int
    checksum: str

    def __post_init__(self) -> None:
        required_strings = {
            "table_key": self.table_key,
            "file_path": self.file_path,
            "file_format": self.file_format,
            "checksum": self.checksum,
        }

        for field_name, value in required_strings.items():
            if not value.strip():
                raise ValueError(f"DatasetPublicationAssetRequest.{field_name} must not be empty")

        path = PurePosixPath(self.file_path)

        if path.is_absolute() or ".." in path.parts:
            raise ValueError("Publication asset file_path must be a safe relative POSIX path")

        if self.row_count < 0:
            raise ValueError("Publication asset row_count must not be negative")

        if self.column_count < 0:
            raise ValueError("Publication asset column_count must not be negative")

        if self.size_bytes < 0:
            raise ValueError("Publication asset size_bytes must not be negative")

        if self.column_count != len(self.columns):
            raise ValueError("Publication asset column_count does not match the columns collection")


@dataclass(frozen=True)
class DatasetPublicationAsset:
    """One published table file joined to its publication."""

    publication_id: str
    dataset_id: str
    version_period: date
    revision: int
    partition_key: str
    partition_value: str
    silver_build_id: str
    table_key: str
    file_path: str
    file_format: str
    row_count: int
    column_count: int
    columns: tuple[str, ...]
    size_bytes: int
    checksum: str

    def to_view_entry(self) -> dict[str, Any]:
        """Return the representation used by Silver SQL views."""

        return {
            "dataset_id": self.dataset_id,
            "table_key": self.table_key,
            "partition_key": self.partition_key,
            "partition_value": self.partition_value,
            "silver_build_id": self.silver_build_id,
            "path": self.file_path,
            "format": self.file_format,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": list(self.columns),
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
        }
