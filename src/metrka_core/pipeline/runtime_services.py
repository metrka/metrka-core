"""Injectable services for nondeterministic runtime values."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4


class Clock(Protocol):
    """Provide timezone-aware UTC timestamps."""

    def now_utc(self) -> datetime: ...


class MonotonicClock(Protocol):
    """Measure elapsed time independently of UTC."""

    def monotonic(self) -> float:
        """Return monotonically increasing seconds."""
        ...


class PipelineRunIdGenerator(Protocol):
    """Generate identifiers for pipeline executions."""

    def new_pipeline_run_id(self) -> str: ...


@dataclass(frozen=True)
class SystemClock:
    """Read the current time from the operating system."""

    def now_utc(self) -> datetime:
        return datetime.now(UTC)


@dataclass(frozen=True)
class SystemMonotonicClock:
    """Read the operating system monotonic clock."""

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass(frozen=True)
class UuidPipelineRunIdGenerator:
    """Generate globally unique pipeline run identifiers."""

    def new_pipeline_run_id(self) -> str:
        return f"pipeline_{uuid4().hex}"


class DatasetFileIdGenerator(Protocol):
    """Generate identifiers for File Marshal records."""

    def new_dataset_file_id(self) -> str:
        """Return a PostgreSQL-compatible UUID string."""
        ...


@dataclass(frozen=True)
class UuidDatasetFileIdGenerator:
    """Generate random UUID4 dataset file identifiers."""

    def new_dataset_file_id(self) -> str:
        return str(uuid4())


class SourceSchemaSnapshotIdGenerator(Protocol):
    """Generate identifiers for source-schema snapshots."""

    def new_source_schema_snapshot_id(self) -> str:
        """Return a PostgreSQL-compatible UUID string."""
        ...


@dataclass(frozen=True)
class UuidSourceSchemaSnapshotIdGenerator:
    """Generate random UUID4 source-schema snapshot IDs."""

    def new_source_schema_snapshot_id(self) -> str:
        return str(uuid4())


class BronzeRunIdGenerator(Protocol):
    """Generate identifiers for Bronze materializations."""

    def new_bronze_run_id(self) -> str:
        """Return one Bronze run identifier."""
        ...


@dataclass(frozen=True)
class UuidBronzeRunIdGenerator:
    """Generate random UUID-based Bronze run identifiers."""

    def new_bronze_run_id(self) -> str:
        return f"bronze_{uuid4().hex[:12]}"


class SilverBuildIdGenerator(Protocol):
    """Generate identifiers for Silver build attempts."""

    def new_silver_build_id(self) -> str:
        """Return a PostgreSQL-compatible UUID string."""
        ...


@dataclass(frozen=True)
class UuidSilverBuildIdGenerator:
    """Generate random UUID4 Silver build identifiers."""

    def new_silver_build_id(self) -> str:
        return str(uuid4())


class TransformationImpactIdGenerator(Protocol):
    """Generate identifiers for transformation-impact records."""

    def new_transformation_impact_id(self) -> str:
        """Return a new transformation-impact identifier."""
        ...


@dataclass(frozen=True)
class UuidTransformationImpactIdGenerator:
    """Generate random UUID4 transformation-impact identifiers."""

    def new_transformation_impact_id(self) -> str:
        return f"impact_{uuid4().hex}"


class PublicationIdGenerator(Protocol):
    """Generate identifiers for immutable public revisions."""

    def new_publication_id(self) -> str:
        """Return a new public publication identifier."""
        ...


class PublicationCandidateIdGenerator(Protocol):
    """Generate identifiers for publication candidates."""

    def new_publication_candidate_id(self) -> str:
        """Return a new publication candidate identifier."""
        ...


@dataclass(frozen=True)
class UuidPublicationIdGenerator:
    """Generate random UUID4 publication identifiers."""

    def new_publication_id(self) -> str:
        return f"publication_{uuid4().hex}"


@dataclass(frozen=True)
class UuidPublicationCandidateIdGenerator:
    """Generate random UUID4 publication candidate identifiers."""

    def new_publication_candidate_id(self) -> str:
        return f"publication_candidate_{uuid4().hex}"


class SourceCaptureIdGenerator(Protocol):
    """Generate identifiers for immutable source captures."""

    def new_source_capture_id(self, *, captured_at: datetime) -> str:
        """Return one timestamped source-capture identifier."""
        ...


@dataclass(frozen=True)
class UuidSourceCaptureIdGenerator:
    """Generate timestamped source-capture identifiers."""

    def new_source_capture_id(self, *, captured_at: datetime) -> str:
        if captured_at.tzinfo is None or captured_at.utcoffset() != timedelta(0):
            raise ValueError("captured_at must be timezone-aware UTC")

        timestamp = captured_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")

        return f"capture_{timestamp}_{uuid4().hex[:8]}"


class ExecutionIdGenerator(Protocol):
    """Generate identifiers for execution runs and steps."""

    def new_run_id(self, prefix: str) -> str:
        """Return an execution run identifier."""
        ...

    def new_step_id(self, prefix: str = "step") -> str:
        """Return an execution step identifier."""
        ...


@dataclass(frozen=True)
class UuidExecutionIdGenerator:
    """Generate random UUID-based execution identifiers."""

    def new_run_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid4().hex[:12]}"

    def new_step_id(self, prefix: str = "step") -> str:
        return f"{prefix}_{uuid4().hex[:10]}"
