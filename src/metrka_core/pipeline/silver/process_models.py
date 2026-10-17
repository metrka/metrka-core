"""Structured outcomes and failures for Silver processing."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class SilverFailureStage(StrEnum):
    """Stage at which one dataset stopped Silver processing."""

    PREPARATION = "preparation"
    TABLE_BUILD = "table_build"
    FINALIZATION = "finalization"
    EMPTY_OUTPUT = "empty_output"


@dataclass(frozen=True, slots=True)
class SilverDatasetFailure:
    """Structured explanation of one rejected Silver dataset build."""

    dataset_id: str
    stage: SilverFailureStage
    error_code: str
    message: str
    silver_build_id: str | None = None
    table_key: str | None = None

    def __post_init__(self) -> None:
        if not self.dataset_id.strip():
            raise ValueError("SilverDatasetFailure.dataset_id must not be empty")

        if not self.error_code.strip():
            raise ValueError("SilverDatasetFailure.error_code must not be empty")

        if not self.message.strip():
            raise ValueError("SilverDatasetFailure.message must not be empty")


class SilverProcessingError(RuntimeError):
    """Stop Silver processing while retaining structured failure details."""

    def __init__(self, failure: SilverDatasetFailure) -> None:
        self.failure = failure

        details = [
            f"dataset_id={failure.dataset_id}",
            f"stage={failure.stage.value}",
            f"error_code={failure.error_code}",
        ]

        if failure.silver_build_id is not None:
            details.append(f"silver_build_id={failure.silver_build_id}")

        if failure.table_key is not None:
            details.append(f"table_key={failure.table_key}")

        super().__init__(f"Silver processing failed ({', '.join(details)}): {failure.message}")


class SilverCandidateOutcomeStatus(StrEnum):
    """Successful disposition of one evaluated Bronze candidate."""

    FINALIZED = "finalized"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class SilverCandidateOutcome:
    """Successful result returned from one candidate execution."""

    dataset_id: str
    status: SilverCandidateOutcomeStatus
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.dataset_id.strip():
            raise ValueError("SilverCandidateOutcome.dataset_id must not be empty")


@dataclass(frozen=True, slots=True)
class SilverProcessResult:
    """Structured successful outcome of processing available Silver candidates."""

    finalized_dataset_ids: tuple[str, ...] = ()
    skipped_dataset_ids: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @classmethod
    def from_outcomes(cls, outcomes: Iterable[SilverCandidateOutcome]) -> SilverProcessResult:
        """Aggregate candidate outcomes without hiding candidate-level failures."""

        finalized_dataset_ids: list[str] = []
        skipped_dataset_ids: list[str] = []
        warnings: list[str] = []

        for outcome in outcomes:
            if outcome.status is SilverCandidateOutcomeStatus.FINALIZED:
                finalized_dataset_ids.append(outcome.dataset_id)
            else:
                skipped_dataset_ids.append(outcome.dataset_id)

            warnings.extend(outcome.warnings)

        return cls(
            finalized_dataset_ids=tuple(finalized_dataset_ids),
            skipped_dataset_ids=tuple(skipped_dataset_ids),
            warnings=tuple(warnings),
        )

    @property
    def finalized_count(self) -> int:
        return len(self.finalized_dataset_ids)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_dataset_ids)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)
