"""Candidate relationships to normalized publication-gate evidence."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PublicationGateAttempt:
    """One candidate decision backed by a persisted integrity batch."""

    candidate_id: str
    silver_build_id: str
    pipeline_run_id: str
    integrity_batch_id: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("candidate_id", self.candidate_id),
            ("silver_build_id", self.silver_build_id),
            ("pipeline_run_id", self.pipeline_run_id),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

        if self.integrity_batch_id <= 0:
            raise ValueError("integrity_batch_id must be positive")
