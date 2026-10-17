"""Persistence contract for data-quality metadata."""

from __future__ import annotations

from typing import Any, Protocol


class QualityCheckStore(Protocol):
    """Store quality definitions and executed check results."""

    def upsert_quality_check_definition(self, record: dict[str, Any]) -> None:
        """Insert or update one quality-check definition."""
        ...

    def insert_quality_check_run(self, record: dict[str, Any]) -> None:
        """Insert one executed quality-check result."""
        ...
