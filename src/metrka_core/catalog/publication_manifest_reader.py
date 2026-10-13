"""Public port for reading immutable publication manifests."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Protocol


class PublicationManifestFailure(StrEnum):
    """Stable reasons why a publication manifest could not be read."""

    UNSAFE_PATH = "unsafe_path"
    NOT_FOUND = "not_found"
    OUTSIDE_STORAGE = "outside_storage"
    READ_FAILED = "read_failed"
    INVALID_JSON = "invalid_json"
    INVALID_OBJECT = "invalid_object"


class PublicationManifestReadError(RuntimeError):
    """Structured failure raised while resolving or decoding one manifest."""

    def __init__(self, *, reason: PublicationManifestFailure, path: str, message: str) -> None:
        self.reason = reason
        self.path = path
        super().__init__(message)


class PublicationManifestReader(Protocol):
    """Read manifests without exposing the configured storage adapter."""

    def read_manifest(self, *, path: str) -> dict[str, Any]:
        """Read one immutable publication manifest JSON object."""
        ...
