"""Public port for reading immutable contract snapshots."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Protocol


class ContractSnapshotFailure(StrEnum):
    """Stable reasons why a contract snapshot could not be read."""

    UNSAFE_PATH = "unsafe_path"
    NOT_FOUND = "not_found"
    OUTSIDE_STORAGE = "outside_storage"
    READ_FAILED = "read_failed"
    INVALID_JSON = "invalid_json"
    INVALID_OBJECT = "invalid_object"


class ContractSnapshotReadError(RuntimeError):
    """Structured failure raised while resolving or decoding one snapshot."""

    def __init__(self, *, reason: ContractSnapshotFailure, path: str, message: str) -> None:
        self.reason = reason
        self.path = path
        super().__init__(message)


class ContractSnapshotReader(Protocol):
    """Read contract snapshots without exposing the configured storage adapter."""

    def read_snapshot(self, *, path: str) -> dict[str, Any]:
        """Read one immutable contract snapshot JSON object."""
        ...
