"""Shared local-filesystem reader for JSON objects below a trusted data root."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

from metrka_core.storage.portable_paths import validate_portable_relative_path


class JsonObjectFailure(StrEnum):
    """Internal failure reasons shared by narrow public artifact readers."""

    UNSAFE_PATH = "unsafe_path"
    NOT_FOUND = "not_found"
    OUTSIDE_STORAGE = "outside_storage"
    READ_FAILED = "read_failed"
    INVALID_JSON = "invalid_json"
    INVALID_OBJECT = "invalid_object"


class JsonObjectReadError(RuntimeError):
    """Internal structured failure raised by ``LocalJsonObjectReader``."""

    def __init__(self, *, reason: JsonObjectFailure, path: str, message: str) -> None:
        self.reason = reason
        self.path = path
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class LocalJsonObjectReader:
    """Read UTF-8 JSON objects below one explicitly allowed storage root."""

    data_root: Path
    storage_root: Path
    artifact_label: str
    storage_label: str

    def __post_init__(self) -> None:
        for field_name in ("data_root", "storage_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            object.__setattr__(self, field_name, value.expanduser().resolve())

        for field_name in ("artifact_label", "storage_label"):
            value = getattr(self, field_name)

            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

            object.__setattr__(self, field_name, value.strip())

    def read(self, *, path: str) -> dict[str, Any]:
        """Resolve and decode one object without allowing a storage escape."""

        try:
            validate_portable_relative_path(path)
        except ValueError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.UNSAFE_PATH,
                path=path,
                message=f"{self.artifact_label} path is unsafe: {path!r}",
            ) from error

        if not self.data_root.is_dir():
            raise JsonObjectReadError(
                reason=JsonObjectFailure.NOT_FOUND,
                path=path,
                message=f"Workspace data root does not exist: {self.data_root}",
            )

        try:
            resolved_storage_root = self.storage_root.resolve(strict=True)
        except FileNotFoundError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.NOT_FOUND,
                path=path,
                message=f"{self.storage_label} does not exist: {self.storage_root}",
            ) from error
        except (OSError, RuntimeError) as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.READ_FAILED,
                path=path,
                message=f"Could not resolve {self.storage_label}: {self.storage_root}",
            ) from error

        try:
            resolved_storage_root.relative_to(self.data_root)
        except ValueError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.OUTSIDE_STORAGE,
                path=path,
                message=f"{self.storage_label} is outside data_root: {self.storage_root}",
            ) from error

        relative_path = PurePosixPath(path)
        unresolved_path = self.data_root.joinpath(*relative_path.parts)

        try:
            resolved_path = unresolved_path.resolve(strict=True)
        except FileNotFoundError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.NOT_FOUND,
                path=path,
                message=f"{self.artifact_label} does not exist: {unresolved_path}",
            ) from error
        except (OSError, RuntimeError) as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.READ_FAILED,
                path=path,
                message=f"Could not resolve {self.artifact_label}: {unresolved_path}",
            ) from error

        try:
            resolved_path.relative_to(resolved_storage_root)
        except ValueError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.OUTSIDE_STORAGE,
                path=path,
                message=f"{self.artifact_label} is outside {self.storage_label}: {path}",
            ) from error

        if not resolved_path.is_file():
            raise JsonObjectReadError(
                reason=JsonObjectFailure.NOT_FOUND,
                path=path,
                message=f"{self.artifact_label} is not a file: {resolved_path}",
            )

        try:
            raw_payload = resolved_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.READ_FAILED,
                path=path,
                message=f"Could not read {self.artifact_label}: {resolved_path}",
            ) from error

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            raise JsonObjectReadError(
                reason=JsonObjectFailure.INVALID_JSON,
                path=path,
                message=f"{self.artifact_label} is not valid JSON: {resolved_path}",
            ) from error

        if not isinstance(payload, dict):
            raise JsonObjectReadError(
                reason=JsonObjectFailure.INVALID_OBJECT,
                path=path,
                message=f"{self.artifact_label} must contain a JSON object: {resolved_path}",
            )

        return {str(key): value for key, value in payload.items()}
