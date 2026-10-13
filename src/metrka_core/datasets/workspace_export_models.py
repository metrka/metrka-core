"""Versioned models for portable customer workspace packages."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final, cast

from metrka_core.datasets.workspace_location import WorkspacePlacement
from metrka_core.storage.checksums import parse_sha256_checksum
from metrka_core.storage.portable_paths import validate_portable_relative_path

WORKSPACE_EXPORT_SCHEMA_VERSION: Final = 1
WORKSPACE_EXPORT_PACKAGE_TYPE: Final = "metrka.customer-workspace"
WORKSPACE_EXPORT_MANIFEST_NAME: Final = "metrka-workspace-manifest.json"
_WORKSPACE_NAME_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


class WorkspaceExportFileRole(StrEnum):
    """Logical source of one file in a reconstructed portable workspace."""

    DEFINITION = "definition"
    DATA = "data"


class WorkspaceExportIntegrityError(ValueError):
    """A customer workspace package is malformed or fails integrity verification."""


@dataclass(frozen=True, slots=True)
class WorkspaceExportFile:
    """One immutable payload file declared by an export manifest."""

    path: str
    role: WorkspaceExportFileRole
    size_bytes: int
    checksum: str

    def __post_init__(self) -> None:
        if not isinstance(self.role, WorkspaceExportFileRole):
            raise TypeError("Workspace export file role must be a WorkspaceExportFileRole")

        validate_payload_path(self.path, role=self.role)

        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ValueError("Workspace export file size_bytes must not be negative")

        parse_sha256_checksum(self.checksum)

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "role": self.role.value,
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceExportManifest:
    """Versioned, machine-verifiable description of one exported workspace."""

    workspace_name: str
    source_placement: WorkspacePlacement
    created_at: datetime
    files: tuple[WorkspaceExportFile, ...]

    def __post_init__(self) -> None:
        validate_workspace_name(self.workspace_name)
        validate_utc_timestamp(self.created_at, field_name="created_at")

        if not isinstance(self.source_placement, WorkspacePlacement):
            raise TypeError("source_placement must be a WorkspacePlacement")
        if not all(isinstance(entry, WorkspaceExportFile) for entry in self.files):
            raise TypeError("files must contain only WorkspaceExportFile values")

        paths = [entry.path for entry in self.files]
        if len(paths) != len(set(paths)) or len(paths) != len({path.casefold() for path in paths}):
            raise ValueError("Workspace export manifest contains colliding file paths")
        if paths != sorted(paths):
            raise ValueError("Workspace export manifest files must be sorted by path")

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def total_size_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self.files)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": WORKSPACE_EXPORT_SCHEMA_VERSION,
            "package_type": WORKSPACE_EXPORT_PACKAGE_TYPE,
            "workspace_name": self.workspace_name,
            "source_placement": self.source_placement.value,
            "created_at": self.created_at.isoformat(),
            "file_count": self.file_count,
            "total_size_bytes": self.total_size_bytes,
            "files": [entry.to_dict() for entry in self.files],
        }

    def to_json_bytes(self) -> bytes:
        rendered = json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return (rendered + "\n").encode("utf-8")

    @classmethod
    def from_json_bytes(cls, content: bytes) -> WorkspaceExportManifest:
        try:
            text = content.decode("utf-8")
            raw: object = json.loads(text, object_pairs_hook=_unique_json_object)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise WorkspaceExportIntegrityError(
                "Workspace export manifest is not valid JSON"
            ) from error

        mapping = _require_mapping(raw, where="workspace export manifest")
        _require_exact_keys(
            mapping,
            expected={
                "schema_version",
                "package_type",
                "workspace_name",
                "source_placement",
                "created_at",
                "file_count",
                "total_size_bytes",
                "files",
            },
            where="workspace export manifest",
        )

        schema_version = _require_integer(mapping, "schema_version")
        if schema_version != WORKSPACE_EXPORT_SCHEMA_VERSION:
            raise WorkspaceExportIntegrityError(
                f"Unsupported workspace export schema_version {schema_version}; "
                f"expected {WORKSPACE_EXPORT_SCHEMA_VERSION}"
            )

        package_type = _require_string(mapping, "package_type")
        if package_type != WORKSPACE_EXPORT_PACKAGE_TYPE:
            raise WorkspaceExportIntegrityError(
                f"Unsupported workspace export package_type {package_type!r}"
            )

        raw_placement = _require_string(mapping, "source_placement")
        try:
            source_placement = WorkspacePlacement(raw_placement)
        except ValueError as error:
            raise WorkspaceExportIntegrityError(
                f"Unsupported source_placement in workspace export: {raw_placement!r}"
            ) from error

        raw_created_at = _require_string(mapping, "created_at")
        try:
            created_at = datetime.fromisoformat(raw_created_at)
        except ValueError as error:
            raise WorkspaceExportIntegrityError(
                "Workspace export created_at is not an ISO-8601 timestamp"
            ) from error

        raw_files = mapping["files"]
        if not isinstance(raw_files, list):
            raise WorkspaceExportIntegrityError("Workspace export files must be a list")

        files = tuple(
            _parse_file(raw_entry, index=index) for index, raw_entry in enumerate(raw_files)
        )
        try:
            manifest = cls(
                workspace_name=_require_string(mapping, "workspace_name"),
                source_placement=source_placement,
                created_at=created_at,
                files=files,
            )
        except (TypeError, ValueError) as error:
            raise WorkspaceExportIntegrityError(
                f"Invalid workspace export manifest: {error}"
            ) from error

        if _require_integer(mapping, "file_count") != manifest.file_count:
            raise WorkspaceExportIntegrityError(
                "Workspace export file_count does not match the files list"
            )
        if _require_integer(mapping, "total_size_bytes") != manifest.total_size_bytes:
            raise WorkspaceExportIntegrityError(
                "Workspace export total_size_bytes does not match the files list"
            )

        return manifest


@dataclass(frozen=True, slots=True)
class WorkspaceExportResult:
    """Created customer package and its externally recordable checksum."""

    workspace_name: str
    source_placement: WorkspacePlacement
    package_path: Path
    package_checksum: str
    file_count: int
    total_size_bytes: int


@dataclass(frozen=True, slots=True)
class WorkspaceExportVerificationResult:
    """Successful verification of one customer workspace package."""

    workspace_name: str
    source_placement: WorkspacePlacement
    package_path: Path
    package_checksum: str
    created_at: datetime
    file_count: int
    total_size_bytes: int


def validate_workspace_name(workspace_name: str) -> None:
    """Require a workspace name that is safe as a cross-platform directory name."""

    if (
        not isinstance(workspace_name, str)
        or _WORKSPACE_NAME_PATTERN.fullmatch(workspace_name) is None
    ):
        raise ValueError(
            "workspace_name must contain only letters, digits, dots, underscores, and hyphens"
        )


def validate_payload_path(path: str, *, role: WorkspaceExportFileRole) -> None:
    """Require one canonical cross-platform package-relative payload path."""

    validate_portable_relative_path(path)
    pure_path = PurePosixPath(path)
    is_data_path = pure_path.parts[0] == "data"
    if role is WorkspaceExportFileRole.DATA and not is_data_path:
        raise ValueError("Workspace export data files must be below data/")
    if role is WorkspaceExportFileRole.DEFINITION and is_data_path:
        raise ValueError("Workspace export definition files must not be below data/")


def validate_utc_timestamp(value: datetime, *, field_name: str) -> None:
    """Require a timezone-aware UTC timestamp."""

    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")


def _parse_file(raw: object, *, index: int) -> WorkspaceExportFile:
    entry = _require_mapping(raw, where=f"workspace export files[{index}]")
    _require_exact_keys(
        entry,
        expected={"path", "role", "size_bytes", "checksum"},
        where=f"workspace export files[{index}]",
    )
    raw_role = _require_string(entry, "role")
    try:
        role = WorkspaceExportFileRole(raw_role)
    except ValueError as error:
        raise WorkspaceExportIntegrityError(
            f"Unsupported workspace export file role {raw_role!r} at index {index}"
        ) from error

    try:
        return WorkspaceExportFile(
            path=_require_string(entry, "path"),
            role=role,
            size_bytes=_require_integer(entry, "size_bytes"),
            checksum=_require_string(entry, "checksum"),
        )
    except (TypeError, ValueError) as error:
        raise WorkspaceExportIntegrityError(
            f"Invalid workspace export file at index {index}: {error}"
        ) from error


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise WorkspaceExportIntegrityError(
                f"Workspace export manifest contains duplicate JSON key {key!r}"
            )
        result[key] = value
    return result


def _require_mapping(value: object, *, where: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise WorkspaceExportIntegrityError(f"{where} must be an object with string keys")
    return {cast(str, key): item for key, item in value.items()}


def _require_exact_keys(mapping: Mapping[str, object], *, expected: set[str], where: str) -> None:
    actual = set(mapping)
    if actual != expected:
        raise WorkspaceExportIntegrityError(
            f"{where} has invalid fields: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


def _require_string(mapping: Mapping[str, object], field_name: str) -> str:
    value = mapping[field_name]
    if not isinstance(value, str) or not value:
        raise WorkspaceExportIntegrityError(f"Workspace export {field_name} must be a string")
    return value


def _require_integer(mapping: Mapping[str, object], field_name: str) -> int:
    value = mapping[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WorkspaceExportIntegrityError(
            f"Workspace export {field_name} must be a non-negative integer"
        )
    return value
