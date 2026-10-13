"""Tests for injectable runtime services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.runtime_services import (
    SystemClock,
    UuidBronzeRunIdGenerator,
    UuidDatasetFileIdGenerator,
    UuidPipelineRunIdGenerator,
    UuidPublicationCandidateIdGenerator,
    UuidSilverBuildIdGenerator,
    UuidSourceCaptureIdGenerator,
    UuidSourceSchemaSnapshotIdGenerator,
    UuidTransformationImpactIdGenerator,
)


@dataclass(frozen=True)
class FrozenClock:
    """Return one deterministic UTC timestamp."""

    current_time: datetime

    def now_utc(self) -> datetime:
        return self.current_time


def test_system_clock_returns_aware_utc_time() -> None:
    current_time = SystemClock().now_utc()

    assert current_time.utcoffset() == timedelta(0)


def test_uuid_pipeline_run_ids_are_unique() -> None:
    generator = UuidPipelineRunIdGenerator()

    first = generator.new_pipeline_run_id()
    second = generator.new_pipeline_run_id()

    assert first.startswith("pipeline_")
    assert second.startswith("pipeline_")
    assert len(first) == len("pipeline_") + 32
    assert first != second


def test_runtime_services_builds_all_default_ports() -> None:
    services = RuntimeServices()

    assert isinstance(services.clock, SystemClock)
    assert isinstance(services.pipeline_run_ids, UuidPipelineRunIdGenerator)
    assert isinstance(services.source_capture_ids, UuidSourceCaptureIdGenerator)
    assert isinstance(services.dataset_file_ids, UuidDatasetFileIdGenerator)
    assert isinstance(services.bronze_run_ids, UuidBronzeRunIdGenerator)
    assert isinstance(services.silver_build_ids, UuidSilverBuildIdGenerator)
    assert isinstance(services.publication_candidate_ids, UuidPublicationCandidateIdGenerator)
    assert isinstance(services.transformation_impact_ids, UuidTransformationImpactIdGenerator)
    assert isinstance(services.source_schema_snapshot_ids, UuidSourceSchemaSnapshotIdGenerator)


def test_runtime_services_accepts_one_override() -> None:
    fixed_time = datetime(2026, 8, 15, 10, 30, tzinfo=UTC)
    frozen_clock = FrozenClock(fixed_time)

    services = RuntimeServices(clock=frozen_clock)

    assert services.clock is frozen_clock
    assert services.clock.now_utc() == fixed_time

    assert isinstance(services.pipeline_run_ids, UuidPipelineRunIdGenerator)
    assert isinstance(services.dataset_file_ids, UuidDatasetFileIdGenerator)
    assert isinstance(services.silver_build_ids, UuidSilverBuildIdGenerator)


def test_runtime_services_does_not_share_default_instances() -> None:
    first = RuntimeServices()
    second = RuntimeServices()

    assert first.clock is not second.clock
    assert first.pipeline_run_ids is not second.pipeline_run_ids
    assert first.source_capture_ids is not second.source_capture_ids
    assert first.dataset_file_ids is not second.dataset_file_ids
