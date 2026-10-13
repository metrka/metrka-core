"""Immutable file-integrity evidence shared by publication workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class AssetIntegrityStatus(StrEnum):
    """Integrity outcome for one file."""

    PASSED = "passed"
    FAILED = "failed"


class AssetIntegrityFailureCode(StrEnum):
    """Structured reason why one file failed integrity verification."""

    INVALID_PATH = "invalid_path"
    MISSING_FILE = "missing_file"
    UNREADABLE_FILE = "unreadable_file"
    INVALID_EXPECTED_CHECKSUM = "invalid_expected_checksum"
    SIZE_MISMATCH = "size_mismatch"
    CHECKSUM_MISMATCH = "checksum_mismatch"


@dataclass(frozen=True, slots=True)
class AssetIntegrityResult:
    """One comparison between an expected file and the file on disk."""

    file_path: str
    status: AssetIntegrityStatus
    expected_size_bytes: int
    actual_size_bytes: int | None
    expected_checksum: str
    actual_checksum: str | None
    failure_codes: tuple[AssetIntegrityFailureCode, ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if not self.file_path.strip():
            raise ValueError("file_path must not be empty")

        if self.expected_size_bytes < 0:
            raise ValueError("expected_size_bytes must not be negative")

        if self.actual_size_bytes is not None and self.actual_size_bytes < 0:
            raise ValueError("actual_size_bytes must not be negative")

        if not self.expected_checksum.strip():
            raise ValueError("expected_checksum must not be empty")

        if len(self.failure_codes) != len(set(self.failure_codes)):
            raise ValueError("failure_codes must not contain duplicates")

        has_error_type = self.error_type is not None
        has_error_message = self.error_message is not None

        if has_error_type != has_error_message:
            raise ValueError("error_type and error_message must be provided together")

        if self.status is AssetIntegrityStatus.PASSED:
            if self.failure_codes:
                raise ValueError("A passed integrity result cannot contain failure codes")

            if self.actual_size_bytes is None or self.actual_checksum is None:
                raise ValueError("A passed integrity result requires actual size and checksum")

            if has_error_type:
                raise ValueError("A passed integrity result cannot contain an error")

        elif not self.failure_codes:
            raise ValueError("A failed integrity result requires at least one failure code")


@dataclass(frozen=True, slots=True)
class AssetIntegrityBatch:
    """All file-integrity results produced by one inspection."""

    checked_at: datetime
    results: tuple[AssetIntegrityResult, ...]

    def __post_init__(self) -> None:
        if self.checked_at.utcoffset() != timedelta(0):
            raise ValueError("checked_at must be timezone-aware UTC")

        if not self.results:
            raise ValueError("An asset integrity batch must contain at least one result")

        file_paths = [result.file_path for result in self.results]
        if len(file_paths) != len(set(file_paths)):
            raise ValueError("An asset integrity batch cannot repeat a file_path")

    @property
    def passed(self) -> bool:
        """Return whether every expected file passed verification."""

        return all(result.status is AssetIntegrityStatus.PASSED for result in self.results)

    @property
    def failed_results(self) -> tuple[AssetIntegrityResult, ...]:
        """Return only failed file comparisons."""

        return tuple(
            result for result in self.results if result.status is AssetIntegrityStatus.FAILED
        )
