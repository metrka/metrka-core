"""Compose nondeterministic services for one pipeline run."""

from __future__ import annotations

from dataclasses import dataclass, field

from metrka_core.pipeline.runtime_services import (
    BronzeRunIdGenerator,
    Clock,
    DatasetFileIdGenerator,
    PipelineRunIdGenerator,
    PublicationCandidateIdGenerator,
    SilverBuildIdGenerator,
    SourceCaptureIdGenerator,
    SourceSchemaSnapshotIdGenerator,
    SystemClock,
    TransformationImpactIdGenerator,
    UuidBronzeRunIdGenerator,
    UuidDatasetFileIdGenerator,
    UuidPipelineRunIdGenerator,
    UuidPublicationCandidateIdGenerator,
    UuidSilverBuildIdGenerator,
    UuidSourceCaptureIdGenerator,
    UuidSourceSchemaSnapshotIdGenerator,
    UuidTransformationImpactIdGenerator,
)


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    """Nondeterministic services used by one pipeline run."""

    clock: Clock = field(default_factory=SystemClock)
    pipeline_run_ids: PipelineRunIdGenerator = field(default_factory=UuidPipelineRunIdGenerator)
    source_capture_ids: SourceCaptureIdGenerator = field(
        default_factory=UuidSourceCaptureIdGenerator
    )
    dataset_file_ids: DatasetFileIdGenerator = field(default_factory=UuidDatasetFileIdGenerator)
    bronze_run_ids: BronzeRunIdGenerator = field(default_factory=UuidBronzeRunIdGenerator)
    silver_build_ids: SilverBuildIdGenerator = field(default_factory=UuidSilverBuildIdGenerator)
    publication_candidate_ids: PublicationCandidateIdGenerator = field(
        default_factory=UuidPublicationCandidateIdGenerator
    )
    transformation_impact_ids: TransformationImpactIdGenerator = field(
        default_factory=UuidTransformationImpactIdGenerator
    )
    source_schema_snapshot_ids: SourceSchemaSnapshotIdGenerator = field(
        default_factory=UuidSourceSchemaSnapshotIdGenerator
    )
