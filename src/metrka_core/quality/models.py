"""Shared types for data-quality checks and gates."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from metrka_core.storage.portable_paths import validate_portable_relative_path


class QualityGate(StrEnum):
    """Pipeline boundaries where quality is checked."""

    PRE_BRONZE = "pre_bronze"
    POST_BRONZE = "post_bronze"
    PRE_SILVER = "pre_silver"
    POST_SILVER = "post_silver"

    @property
    def layer(self) -> str:
        return self.value.split("_", maxsplit=1)[1]


class QualitySeverity(StrEnum):
    """Effect of a failed check on the pipeline."""

    BLOCKING = "blocking"
    WARNING = "warning"
    INFO = "info"

    @property
    def blocks_promotion(self) -> bool:
        return self is QualitySeverity.BLOCKING


class QualityStatus(StrEnum):
    """Outcome of one executed check."""

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class QualityOutputFile:
    """One output file with separate runtime and persisted identities."""

    local_path: Path
    workspace_relative_path: str

    def __post_init__(self) -> None:
        if not isinstance(self.local_path, Path):
            raise TypeError("Quality output local_path must be a Path")

        validate_portable_relative_path(self.workspace_relative_path)

        object.__setattr__(self, "local_path", self.local_path.expanduser().resolve(strict=False))


@dataclass(frozen=True, slots=True)
class Outcome:
    """What one check expected, what it saw, and the verdict."""

    status: QualityStatus
    summary: str
    expected: dict[str, Any] = field(default_factory=dict)
    actual: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def of(
        cls,
        passed: bool,
        summary: str,
        *,
        expected: dict[str, Any] | None = None,
        actual: dict[str, Any] | None = None,
    ) -> Outcome:
        status = QualityStatus.PASSED if passed else QualityStatus.FAILED
        return cls(status, summary, expected or {}, actual or {})

    @classmethod
    def skipped(cls, summary: str) -> Outcome:
        return cls(QualityStatus.SKIPPED, summary)


@dataclass(frozen=True)
class QualityGateResult:
    """Aggregated result of all checks executed at one gate."""

    status: str
    checks_run: int
    passed_count: int
    failed_count: int
    skipped_count: int
    blocked_count: int
    error_count: int = 0
    failed_check_ids: list[str] = field(default_factory=list)
    error_message: str | None = None
    failure_code: str | None = None

    @property
    def failed(self) -> bool:
        return self.blocked_count > 0

    def to_meta(self) -> dict[str, Any]:
        return {
            "quality_status": self.status,
            "quality_checks_run": self.checks_run,
            "quality_checks_passed": self.passed_count,
            "quality_checks_failed": self.failed_count,
            "quality_checks_skipped": self.skipped_count,
            "blocking_quality_failures": self.blocked_count,
            "quality_checks_errored": self.error_count,
            "failed_check_ids": self.failed_check_ids,
            "quality_failure_code": self.failure_code,
        }
