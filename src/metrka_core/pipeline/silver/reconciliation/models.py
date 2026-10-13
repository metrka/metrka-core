"""Typed reports shared by focused Silver publication reconcilers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_projection_models import PublicationProjectionKind
from metrka_core.pipeline.silver.artifact_models import SilverArtifactDeletionError
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.quality.publication_integrity_models import PublicationIntegrityCheck
from metrka_core.storage.file_integrity import FileIntegrityResult


class OrphanCleanupStatus(StrEnum):
    """Outcome of the optional cleanup for one orphan build."""

    NOT_ELIGIBLE = "not_eligible"
    DRY_RUN = "dry_run"
    DELETED = "deleted"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OrphanCleanupResult:
    """Structured cleanup result retained in the reconciliation report."""

    status: OrphanCleanupStatus
    deleted_directories: tuple[Path, ...] = ()
    errors: tuple[SilverArtifactDeletionError, ...] = ()

    def __post_init__(self) -> None:
        if self.status is OrphanCleanupStatus.FAILED:
            if not self.errors:
                raise ValueError("A failed orphan cleanup requires at least one error")
        elif self.errors:
            raise ValueError("Only a failed orphan cleanup may contain errors")

        if (
            self.status in {OrphanCleanupStatus.NOT_ELIGIBLE, OrphanCleanupStatus.DRY_RUN}
            and self.deleted_directories
        ):
            raise ValueError("A cleanup that was not attempted cannot contain deleted paths")

    @property
    def deleted(self) -> bool:
        """Return whether the complete orphan cleanup succeeded."""

        return self.status is OrphanCleanupStatus.DELETED


@dataclass(frozen=True, slots=True)
class OrphanSilverBuild:
    """One local Silver build without a publication."""

    silver_build_id: str
    database_status: SilverBuildStatus
    artifact_directories: tuple[Path, ...]
    age_hours: float
    eligible_for_deletion: bool
    cleanup: OrphanCleanupResult
    reason: str

    @property
    def deleted(self) -> bool:
        """Return whether every orphan artifact directory was deleted."""

        return self.cleanup.deleted


class ProjectionReconciliationStatus(StrEnum):
    """Outcome of one independently recoverable projection."""

    SKIPPED = "skipped"
    REPAIRED = "repaired"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ProjectionReconciliationResult:
    """Result of repairing one filesystem projection."""

    projection_kind: PublicationProjectionKind
    status: ProjectionReconciliationStatus
    paths: tuple[Path, ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.status is ProjectionReconciliationStatus.FAILED:
            if not self.error_type or self.error_message is None:
                raise ValueError("A failed projection result requires a structured error")
        elif self.error_type is not None or self.error_message is not None:
            raise ValueError("Only a failed projection result may contain an error")

        if self.status is ProjectionReconciliationStatus.SKIPPED and self.paths:
            raise ValueError("A skipped projection result cannot contain paths")

    @property
    def repaired(self) -> bool:
        """Return whether the requested projection was repaired."""

        return self.status is ProjectionReconciliationStatus.REPAIRED


@dataclass(frozen=True, slots=True)
class ManifestReconciliationFailure:
    """One historical manifest that could not be validated or backfilled."""

    publication_id: str
    manifest_path: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class AssetVerificationFailure:
    """One publication whose asset verification could not be completed."""

    publication_id: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class FileIntegrityExecutionFailure:
    """One immutable file whose integrity check could not be prepared."""

    artifact_kind: str
    owner_id: str
    file_path: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class PublicationRecordReconciliation:
    """Publication records selected for the remaining reconciliation stages."""

    dataset_id: str
    current_publication: DatasetPublication | None
    all_publications: tuple[DatasetPublication, ...]
    integrity_publications: tuple[DatasetPublication, ...]
    manifest_failures: tuple[ManifestReconciliationFailure, ...]
    backfilled_publication_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublicationAssetReconciliation:
    """Integrity outcome for registered publication assets."""

    verifications: tuple[PublicationIntegrityCheck, ...]
    failures: tuple[AssetVerificationFailure, ...]


@dataclass(frozen=True, slots=True)
class PublicationEvidenceReconciliation:
    """Integrity outcome for manifests, transformation details and contracts."""

    manifest_results: tuple[FileIntegrityResult, ...]
    manifest_failures: tuple[FileIntegrityExecutionFailure, ...]
    transformation_detail_results: tuple[FileIntegrityResult, ...]
    transformation_detail_failures: tuple[FileIntegrityExecutionFailure, ...]
    contract_snapshot_results: tuple[FileIntegrityResult, ...]
    contract_snapshot_failures: tuple[FileIntegrityExecutionFailure, ...]


@dataclass(frozen=True, slots=True)
class PublicationProjectionReconciliation:
    """Independent repair results for current and history projections."""

    current: ProjectionReconciliationResult
    history: ProjectionReconciliationResult


@dataclass(frozen=True, slots=True)
class SilverBuildArtifactReconciliation:
    """Unpublished build artifacts inspected for one dataset."""

    orphans: tuple[OrphanSilverBuild, ...]


@dataclass(frozen=True, slots=True)
class SilverPublicationReconciliation:
    """Unified operator-facing result of one dataset reconciliation."""

    dataset_id: str
    current_publication_id: str | None
    current_projection: ProjectionReconciliationResult
    history_projection: ProjectionReconciliationResult
    asset_verifications: tuple[PublicationIntegrityCheck, ...]
    asset_verification_failures: tuple[AssetVerificationFailure, ...]
    manifest_integrity_results: tuple[FileIntegrityResult, ...]
    manifest_integrity_failures: tuple[FileIntegrityExecutionFailure, ...]
    transformation_detail_integrity_results: tuple[FileIntegrityResult, ...]
    transformation_detail_integrity_failures: tuple[FileIntegrityExecutionFailure, ...]
    contract_snapshot_integrity_results: tuple[FileIntegrityResult, ...]
    contract_snapshot_integrity_failures: tuple[FileIntegrityExecutionFailure, ...]
    manifest_failures: tuple[ManifestReconciliationFailure, ...]
    backfilled_publication_ids: tuple[str, ...]
    orphans: tuple[OrphanSilverBuild, ...]
