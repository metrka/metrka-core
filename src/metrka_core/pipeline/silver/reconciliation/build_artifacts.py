"""Reconcile unpublished Silver builds with their artifact directories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.pipeline.silver.artifact_models import SilverBuildArtifactQuery
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.reconciliation.models import (
    OrphanCleanupResult,
    OrphanCleanupStatus,
    OrphanSilverBuild,
    SilverBuildArtifactReconciliation,
)


@dataclass(frozen=True, slots=True)
class SilverBuildArtifactReconciler:
    """Report and optionally delete eligible unpublished build artifacts."""

    silver_builds: PostgresSilverBuildStore
    silver_store: SilverArtifactStore

    def reconcile(
        self,
        *,
        dataset_id: str,
        publications: tuple[DatasetPublication, ...],
        now: datetime,
        delete_orphans: bool,
        grace_period: timedelta,
    ) -> SilverBuildArtifactReconciliation:
        """Inspect only build directories belonging to the requested dataset."""

        published_build_ids = {publication.silver_build_id for publication in publications}
        unpublished_builds = {
            build.silver_build_id: build
            for build in self.silver_builds.list_for_dataset(dataset_id=dataset_id)
            if build.silver_build_id not in published_build_ids
        }
        build_directories = self.silver_store.list_build_artifact_directories(
            builds=tuple(
                SilverBuildArtifactQuery(
                    dataset_id=build.dataset_id,
                    silver_build_id=build.silver_build_id,
                    partition_key=build.partition_key,
                    partition_value=build.partition_value,
                )
                for build in unpublished_builds.values()
            )
        )
        orphans: list[OrphanSilverBuild] = []

        for silver_build_id, artifact_directories in build_directories.items():
            build = unpublished_builds[silver_build_id]
            lifecycle_time = build.completed_at or build.started_at
            age = now - lifecycle_time
            age_hours = max(0.0, age.total_seconds() / 3600)
            old_enough = age >= grace_period
            eligible_for_deletion = build.status is SilverBuildStatus.FAILED and old_enough
            cleanup = self._cleanup(
                silver_build_id=silver_build_id,
                artifact_directories=artifact_directories,
                eligible_for_deletion=eligible_for_deletion,
                delete_requested=delete_orphans,
            )
            orphans.append(
                OrphanSilverBuild(
                    silver_build_id=silver_build_id,
                    database_status=build.status,
                    artifact_directories=artifact_directories,
                    age_hours=age_hours,
                    eligible_for_deletion=eligible_for_deletion,
                    cleanup=cleanup,
                    reason=self._reason(
                        status=build.status, old_enough=old_enough, cleanup=cleanup
                    ),
                )
            )

        return SilverBuildArtifactReconciliation(orphans=tuple(orphans))

    def _cleanup(
        self,
        *,
        silver_build_id: str,
        artifact_directories: tuple[Path, ...],
        eligible_for_deletion: bool,
        delete_requested: bool,
    ) -> OrphanCleanupResult:
        if not eligible_for_deletion:
            return OrphanCleanupResult(status=OrphanCleanupStatus.NOT_ELIGIBLE)

        if not delete_requested:
            return OrphanCleanupResult(status=OrphanCleanupStatus.DRY_RUN)

        deletion = self.silver_store.delete_build_artifact_directories(
            silver_build_id=silver_build_id, artifact_directories=artifact_directories
        )

        return OrphanCleanupResult(
            status=(
                OrphanCleanupStatus.DELETED if deletion.deleted else OrphanCleanupStatus.FAILED
            ),
            deleted_directories=deletion.deleted_directories,
            errors=deletion.errors,
        )

    @staticmethod
    def _reason(
        *, status: SilverBuildStatus, old_enough: bool, cleanup: OrphanCleanupResult
    ) -> str:
        if cleanup.status is OrphanCleanupStatus.DELETED:
            return "Failed unpublished build was deleted after the grace period."

        if cleanup.status is OrphanCleanupStatus.FAILED:
            return (
                "Failed unpublished build was eligible for deletion, but "
                f"{len(cleanup.errors)} artifact removal operation(s) failed."
            )

        if status is SilverBuildStatus.RUNNING:
            return "Running build is protected from deletion."

        if status is SilverBuildStatus.SUCCEEDED:
            return (
                "Successful build has no publication. "
                "It may require recovery and is protected from automatic deletion."
            )

        if not old_enough:
            return "Failed build is still inside the configured grace period."

        if cleanup.status is OrphanCleanupStatus.DRY_RUN:
            return "Failed build is eligible for deletion; dry-run mode made no changes."

        raise RuntimeError("Inconsistent orphan cleanup result")
