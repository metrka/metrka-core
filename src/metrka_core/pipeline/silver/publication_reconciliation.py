"""Coordinate focused Silver publication reconciliation stages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from metrka_core.pipeline.silver.reconciliation import (
    PublicationAssetReconciler,
    PublicationEvidenceReconciler,
    PublicationProjectionReconciler,
    PublicationRecordReconciler,
    SilverBuildArtifactReconciler,
)
from metrka_core.pipeline.silver.reconciliation.models import (
    AssetVerificationFailure,
    FileIntegrityExecutionFailure,
    ManifestReconciliationFailure,
    OrphanCleanupResult,
    OrphanCleanupStatus,
    OrphanSilverBuild,
    ProjectionReconciliationResult,
    ProjectionReconciliationStatus,
    SilverPublicationReconciliation,
)

__all__ = [
    "AssetVerificationFailure",
    "FileIntegrityExecutionFailure",
    "ManifestReconciliationFailure",
    "OrphanCleanupResult",
    "OrphanCleanupStatus",
    "OrphanSilverBuild",
    "ProjectionReconciliationResult",
    "ProjectionReconciliationStatus",
    "SilverPublicationReconciliation",
    "SilverPublicationReconciler",
]


@dataclass(frozen=True, slots=True)
class SilverPublicationReconciler:
    """Coordinate independent reconcilers and assemble one operator report."""

    records: PublicationRecordReconciler
    assets: PublicationAssetReconciler
    evidence: PublicationEvidenceReconciler
    projections: PublicationProjectionReconciler
    build_artifacts: SilverBuildArtifactReconciler

    def reconcile(
        self,
        *,
        dataset_id: str,
        now: datetime,
        delete_orphans: bool = False,
        grace_period: timedelta = timedelta(days=7),
        verify_history_assets: bool = False,
    ) -> SilverPublicationReconciliation:
        """Run all reconciliation stages without merging their responsibilities."""

        if not dataset_id.strip():
            raise ValueError("dataset_id must not be empty")

        if grace_period < timedelta(0):
            raise ValueError("grace_period must not be negative")

        if now.utcoffset() != timedelta(0):
            raise ValueError("Reconciliation time must be timezone-aware UTC")

        records = self.records.reconcile(
            dataset_id=dataset_id, include_superseded_history=verify_history_assets
        )
        assets = self.assets.reconcile(publications=records.integrity_publications, checked_at=now)
        evidence = self.evidence.reconcile(publications=records.integrity_publications)
        projections = self.projections.reconcile(
            dataset_id=dataset_id,
            current_publication=records.current_publication,
            all_publications=records.all_publications,
            checked_at=now,
        )
        build_artifacts = self.build_artifacts.reconcile(
            dataset_id=dataset_id,
            publications=records.all_publications,
            now=now,
            delete_orphans=delete_orphans,
            grace_period=grace_period,
        )

        return SilverPublicationReconciliation(
            dataset_id=records.dataset_id,
            current_publication_id=(
                records.current_publication.publication_id
                if records.current_publication is not None
                else None
            ),
            current_projection=projections.current,
            history_projection=projections.history,
            asset_verifications=assets.verifications,
            asset_verification_failures=assets.failures,
            manifest_integrity_results=evidence.manifest_results,
            manifest_integrity_failures=evidence.manifest_failures,
            transformation_detail_integrity_results=evidence.transformation_detail_results,
            transformation_detail_integrity_failures=evidence.transformation_detail_failures,
            contract_snapshot_integrity_results=evidence.contract_snapshot_results,
            contract_snapshot_integrity_failures=evidence.contract_snapshot_failures,
            manifest_failures=records.manifest_failures,
            backfilled_publication_ids=records.backfilled_publication_ids,
            orphans=build_artifacts.orphans,
        )
