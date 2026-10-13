"""Verify immutable workspace files against recorded SHA-256 checksums."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from metrka_core.storage.checksums import format_sha256_checksum, parse_sha256_checksum, sha256_file


class FileIntegrityStatus(StrEnum):
    """Outcome of one immutable-file integrity check."""

    PASSED = "passed"
    FAILED = "failed"


class FileIntegrityFailureCode(StrEnum):
    """Machine-readable reason why an immutable-file check failed."""

    INVALID_PATH = "invalid_path"
    MISSING_FILE = "missing_file"
    UNREADABLE_FILE = "unreadable_file"
    INVALID_EXPECTED_CHECKSUM = "invalid_expected_checksum"
    CHECKSUM_MISMATCH = "checksum_mismatch"


@dataclass(frozen=True, slots=True)
class FileIntegrityExpectation:
    """Recorded identity and checksum for one immutable workspace file."""

    artifact_kind: str
    owner_id: str
    file_path: str
    expected_checksum: str

    def __post_init__(self) -> None:
        for field_name in ("artifact_kind", "owner_id", "file_path"):
            value = getattr(self, field_name)
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")


@dataclass(frozen=True, slots=True)
class FileIntegrityResult:
    """Observed integrity of one immutable workspace file."""

    artifact_kind: str
    owner_id: str
    file_path: str
    status: FileIntegrityStatus
    expected_checksum: str
    actual_checksum: str | None
    failure_codes: tuple[FileIntegrityFailureCode, ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.status is FileIntegrityStatus.PASSED:
            if self.failure_codes:
                raise ValueError("A passed file check cannot contain failure codes")
            if self.actual_checksum is None:
                raise ValueError("A passed file check requires the actual checksum")
            if self.error_type is not None or self.error_message is not None:
                raise ValueError("A passed file check cannot contain an error")
        elif not self.failure_codes:
            raise ValueError("A failed file check requires at least one failure code")

        if (self.error_type is None) != (self.error_message is None):
            raise ValueError("error_type and error_message must be set together")

    @property
    def failed(self) -> bool:
        """Return whether the immutable file failed verification."""

        return self.status is FileIntegrityStatus.FAILED


class FileIntegrityVerifier(Protocol):
    """Port for checking one recorded workspace file."""

    def inspect(self, expectation: FileIntegrityExpectation) -> FileIntegrityResult:
        """Recompute SHA-256 and compare it with the recorded checksum."""
        ...


class Sha256WorkspaceFileIntegrityVerifier:
    """SHA-256 adapter restricted to files below one workspace root."""

    def __init__(self, *, workspace_root: Path) -> None:
        self._workspace_root = workspace_root.expanduser().resolve()

    def inspect(self, expectation: FileIntegrityExpectation) -> FileIntegrityResult:
        try:
            path = self._resolve(expectation.file_path)
        except ValueError as error:
            return self._failed(expectation, FileIntegrityFailureCode.INVALID_PATH, error=error)

        try:
            expected_digest = parse_sha256_checksum(expectation.expected_checksum)
        except ValueError:
            return self._failed(expectation, FileIntegrityFailureCode.INVALID_EXPECTED_CHECKSUM)

        try:
            actual_digest = sha256_file(path)
        except FileNotFoundError as error:
            return self._failed(expectation, FileIntegrityFailureCode.MISSING_FILE, error=error)
        except OSError as error:
            return self._failed(expectation, FileIntegrityFailureCode.UNREADABLE_FILE, error=error)

        actual_checksum = format_sha256_checksum(actual_digest)

        if expected_digest != actual_digest:
            return self._failed(
                expectation,
                FileIntegrityFailureCode.CHECKSUM_MISMATCH,
                actual_checksum=actual_checksum,
            )

        return FileIntegrityResult(
            artifact_kind=expectation.artifact_kind,
            owner_id=expectation.owner_id,
            file_path=expectation.file_path,
            status=FileIntegrityStatus.PASSED,
            expected_checksum=expectation.expected_checksum,
            actual_checksum=actual_checksum,
        )

    def _resolve(self, file_path: str) -> Path:
        relative_path = Path(file_path)
        if relative_path.is_absolute():
            raise ValueError("Immutable artifact path must be workspace-relative")

        resolved = (self._workspace_root / relative_path).resolve()
        try:
            resolved.relative_to(self._workspace_root)
        except ValueError as error:
            raise ValueError("Immutable artifact path is outside the workspace") from error

        return resolved

    @staticmethod
    def _failed(
        expectation: FileIntegrityExpectation,
        failure_code: FileIntegrityFailureCode,
        *,
        actual_checksum: str | None = None,
        error: Exception | None = None,
    ) -> FileIntegrityResult:
        return FileIntegrityResult(
            artifact_kind=expectation.artifact_kind,
            owner_id=expectation.owner_id,
            file_path=expectation.file_path,
            status=FileIntegrityStatus.FAILED,
            expected_checksum=expectation.expected_checksum,
            actual_checksum=actual_checksum,
            failure_codes=(failure_code,),
            error_type=type(error).__name__ if error is not None else None,
            error_message=str(error) if error is not None else None,
        )
