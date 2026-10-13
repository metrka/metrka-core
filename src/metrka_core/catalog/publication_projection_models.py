"""Domain models for derived publication projection health."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class PublicationProjectionKind(StrEnum):
    """Recoverable filesystem projection maintained from publications."""

    CURRENT = "current"
    HISTORY = "history"


class PublicationProjectionStatus(StrEnum):
    """Synchronization status of one recoverable projection."""

    PENDING = "pending"
    SYNCHRONIZED = "synchronized"
    STALE = "stale"


@dataclass(frozen=True)
class DatasetPublicationProjectionState:
    """Current health of one dataset publication projection."""

    dataset_id: str
    projection_kind: PublicationProjectionKind
    expected_publication_id: str
    projected_publication_id: str | None
    status: PublicationProjectionStatus
    status_changed_at: datetime
    last_attempted_at: datetime | None = None
    last_synchronized_at: datetime | None = None
    error: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        for field_name, text_value in {
            "dataset_id": self.dataset_id,
            "expected_publication_id": self.expected_publication_id,
        }.items():
            if not text_value.strip():
                raise ValueError(f"{field_name} must not be empty")

        if self.projected_publication_id is not None and not self.projected_publication_id.strip():
            raise ValueError("projected_publication_id must not be empty")

        for field_name, timestamp in {
            "status_changed_at": self.status_changed_at,
            "last_attempted_at": self.last_attempted_at,
            "last_synchronized_at": self.last_synchronized_at,
        }.items():
            if timestamp is not None and timestamp.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")

        if self.status is PublicationProjectionStatus.SYNCHRONIZED:
            if self.projected_publication_id != self.expected_publication_id:
                raise ValueError("A synchronized projection must contain the expected publication")

            if self.last_attempted_at is None or self.last_synchronized_at is None:
                raise ValueError(
                    "A synchronized projection requires attempt and synchronization timestamps"
                )

            if self.error is not None:
                raise ValueError("A synchronized projection cannot contain an error")

        if self.status is PublicationProjectionStatus.STALE:
            if self.last_attempted_at is None:
                raise ValueError("A stale projection requires an attempt timestamp")

            if not self.error:
                raise ValueError("A stale projection requires a structured error")

        if self.status is PublicationProjectionStatus.PENDING and self.error is not None:
            raise ValueError("A pending projection cannot contain an error")
