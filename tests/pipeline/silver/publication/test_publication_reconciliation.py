"""Tests for publication reconciliation and orphan safety."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_projection_models import (
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.pipeline.silver.artifact_models import SilverArtifactDeletionError
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.publication_asset_integrity import PublicationAssetExpectation
from metrka_core.pipeline.silver.publication_indexes import SilverPublicationIndexResult
from metrka_core.pipeline.silver.publication_reconciliation import (
    OrphanCleanupStatus,
    ProjectionReconciliationStatus,
    SilverPublicationReconciler,
)
from metrka_core.pipeline.silver.reconciliation import (
    PublicationAssetReconciler,
    PublicationEvidenceReconciler,
    PublicationProjectionReconciler,
    PublicationRecordReconciler,
    SilverBuildArtifactReconciler,
)
from metrka_core.pipeline.silver.workspace_orphan_audit import (
    SilverWorkspaceOrphanAuditor,
    UnknownArtifactCause,
)
from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityFailureCode,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityCheck,
    PublicationIntegrityTrigger,
)
from metrka_core.storage.file_integrity import Sha256WorkspaceFileIntegrityVerifier

from .fakes import (
    DATASET_ID,
    FIXED_NOW,
    FakePublicationIndexService,
    FakePublicationStore,
    FakeSilverArtifactStore,
    FakeSilverBuildStore,
    InMemoryPublicationAssetStore,
    InMemoryPublicationProjectionStateStore,
    make_asset_request,
    make_build,
    make_manifest,
    make_publication,
)


def _reconciler(
    *,
    tmp_path: Path,
    publications: FakePublicationStore,
    builds: FakeSilverBuildStore,
    silver_store: FakeSilverArtifactStore,
    assets: InMemoryPublicationAssetStore,
    backfill_assets: bool = False,
    projection_states: InMemoryPublicationProjectionStateStore | None = None,
    indexes: FakePublicationIndexService | None = None,
    asset_integrity: MagicMock | None = None,
    asset_verifications: MagicMock | None = None,
    transformation_impacts: MagicMock | None = None,
) -> SilverPublicationReconciler:
    current = publications.find_current(DATASET_ID)
    fallback = current or make_publication()
    resolved_indexes = indexes or FakePublicationIndexService(
        current_result=_current_index_result(tmp_path=tmp_path, publication=fallback),
        history_paths=(tmp_path / "history.sql",),
    )
    resolved_projection_states = (
        projection_states
        if projection_states is not None
        else InMemoryPublicationProjectionStateStore()
    )
    if asset_integrity is None:
        asset_integrity = MagicMock()
        asset_integrity.inspect.side_effect = _passed_asset_verification

    resolved_asset_verifications = (
        asset_verifications if asset_verifications is not None else MagicMock()
    )
    resolved_transformation_impacts = (
        transformation_impacts if transformation_impacts is not None else MagicMock()
    )
    if transformation_impacts is None:
        resolved_transformation_impacts.list_for_builds.return_value = ()

    return SilverPublicationReconciler(
        records=PublicationRecordReconciler(
            publications=publications,
            publication_assets=assets,
            silver_store=silver_store,  # type: ignore[arg-type]
            backfill_publication_assets=backfill_assets,
        ),
        assets=PublicationAssetReconciler(
            publication_assets=assets,
            integrity=asset_integrity,
            integrity_checks=resolved_asset_verifications,
        ),
        evidence=PublicationEvidenceReconciler(
            silver_builds=builds,  # type: ignore[arg-type]
            file_integrity=Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path),
            transformation_impacts=resolved_transformation_impacts,
            silver_store=silver_store,  # type: ignore[arg-type]
        ),
        projections=PublicationProjectionReconciler(
            publication_indexes=resolved_indexes, projection_states=resolved_projection_states
        ),
        build_artifacts=SilverBuildArtifactReconciler(
            silver_builds=builds,  # type: ignore[arg-type]
            silver_store=silver_store,  # type: ignore[arg-type]
        ),
    )


def _passed_asset_verification(
    *, assets: Collection[PublicationAssetExpectation], checked_at: datetime
) -> AssetIntegrityBatch:
    return AssetIntegrityBatch(
        checked_at=checked_at,
        results=tuple(
            AssetIntegrityResult(
                file_path=asset.file_path,
                status=AssetIntegrityStatus.PASSED,
                expected_size_bytes=asset.size_bytes,
                actual_size_bytes=asset.size_bytes,
                expected_checksum=asset.checksum,
                actual_checksum=asset.checksum,
            )
            for asset in assets
        ),
    )


def _current_index_result(
    *, tmp_path: Path, publication: DatasetPublication
) -> SilverPublicationIndexResult:
    return SilverPublicationIndexResult(
        current_publication=publication,
        pointer_path=tmp_path / "current.json",
        view_paths=(tmp_path / "latest.sql",),
    )


def test_old_failed_orphan_is_reported_but_not_deleted_in_dry_run(tmp_path: Path) -> None:
    publications = FakePublicationStore()
    assets = InMemoryPublicationAssetStore(publications=publications)
    build = make_build(
        silver_build_id="failed-build",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories[build.silver_build_id] = (tmp_path / "silver_build_id=failed-build",)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([build]),
        silver_store=store,
        assets=assets,
    ).reconcile(dataset_id=DATASET_ID, grace_period=timedelta(days=7), now=FIXED_NOW)

    orphan = result.orphans[0]
    assert orphan.eligible_for_deletion
    assert orphan.cleanup.status is OrphanCleanupStatus.DRY_RUN
    assert orphan.cleanup.errors == ()
    assert not orphan.deleted
    assert store.deleted_build_ids == []


def test_delete_flag_removes_only_eligible_failed_orphan(tmp_path: Path) -> None:
    publications = FakePublicationStore()
    assets = InMemoryPublicationAssetStore(publications=publications)
    build = make_build(
        silver_build_id="failed-build",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories[build.silver_build_id] = (tmp_path / "silver_build_id=failed-build",)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([build]),
        silver_store=store,
        assets=assets,
    ).reconcile(
        dataset_id=DATASET_ID, delete_orphans=True, grace_period=timedelta(days=7), now=FIXED_NOW
    )

    assert result.orphans[0].cleanup.status is OrphanCleanupStatus.DELETED
    assert result.orphans[0].deleted
    assert store.deletion_attempted_build_ids == ["failed-build"]
    assert store.deleted_build_ids == ["failed-build"]


def test_deletion_failure_is_structured_and_does_not_block_another_build(tmp_path: Path) -> None:
    first = make_build(
        silver_build_id="failed-build-1",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    second = make_build(
        silver_build_id="failed-build-2",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    publications = FakePublicationStore()
    store = FakeSilverArtifactStore(tmp_path)
    first_path = tmp_path / "silver_build_id=failed-build-1"
    second_path = tmp_path / "silver_build_id=failed-build-2"
    store.build_directories = {
        first.silver_build_id: (first_path,),
        second.silver_build_id: (second_path,),
    }
    store.deletion_errors_by_build[first.silver_build_id] = (
        SilverArtifactDeletionError(
            artifact_directory=first_path, error_type="PermissionError", message="access denied"
        ),
    )

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([first, second]),
        silver_store=store,
        assets=InMemoryPublicationAssetStore(publications=publications),
    ).reconcile(
        dataset_id=DATASET_ID, delete_orphans=True, grace_period=timedelta(days=7), now=FIXED_NOW
    )

    outcomes = {orphan.silver_build_id: orphan for orphan in result.orphans}
    first_outcome = outcomes[first.silver_build_id]
    second_outcome = outcomes[second.silver_build_id]

    assert first_outcome.cleanup.status is OrphanCleanupStatus.FAILED
    assert not first_outcome.deleted
    assert first_outcome.cleanup.errors[0].error_type == "PermissionError"
    assert second_outcome.cleanup.status is OrphanCleanupStatus.DELETED
    assert second_outcome.deleted
    assert store.deletion_attempted_build_ids == ["failed-build-1", "failed-build-2"]
    assert store.deleted_build_ids == ["failed-build-2"]


@pytest.mark.parametrize("status", [SilverBuildStatus.RUNNING, SilverBuildStatus.SUCCEEDED])
def test_running_and_successful_orphans_are_protected(
    tmp_path: Path, status: SilverBuildStatus
) -> None:
    publications = FakePublicationStore()
    assets = InMemoryPublicationAssetStore(publications=publications)
    build = make_build(
        silver_build_id=f"{status.value}-build",
        status=status,
        started_at=FIXED_NOW - timedelta(days=30),
        completed_at=(
            FIXED_NOW - timedelta(days=29) if status is SilverBuildStatus.SUCCEEDED else None
        ),
    )
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories[build.silver_build_id] = (
        tmp_path / f"silver_build_id={build.silver_build_id}",
    )

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([build]),
        silver_store=store,
        assets=assets,
    ).reconcile(
        dataset_id=DATASET_ID, delete_orphans=True, grace_period=timedelta(0), now=FIXED_NOW
    )

    assert not result.orphans[0].eligible_for_deletion
    assert result.orphans[0].cleanup.status is OrphanCleanupStatus.NOT_ELIGIBLE
    assert not result.orphans[0].deleted
    assert store.deleted_build_ids == []


def test_dataset_reconciliation_does_not_report_unknown_workspace_directory(tmp_path: Path) -> None:
    publications = FakePublicationStore()
    assets = InMemoryPublicationAssetStore(publications=publications)
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories["unknown-build"] = (tmp_path / "silver_build_id=unknown-build",)
    builds = FakeSilverBuildStore()

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=builds,
        silver_store=store,
        assets=assets,
    ).reconcile(
        dataset_id=DATASET_ID, delete_orphans=True, grace_period=timedelta(0), now=FIXED_NOW
    )

    assert result.orphans == ()
    assert builds.dataset_requests == [DATASET_ID]
    assert builds.find_by_ids_requests == []
    assert store.build_directory_requests == [frozenset()]
    assert store.deleted_build_ids == []


def test_workspace_audit_reports_unknown_directory_once(tmp_path: Path) -> None:
    known_build_id = "11111111-1111-1111-1111-111111111111"
    unknown_build_id = "22222222-2222-2222-2222-222222222222"
    known = make_build(silver_build_id=known_build_id)
    builds = FakeSilverBuildStore([known])
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories = {
        known_build_id: (tmp_path / f"silver_build_id={known_build_id}",),
        unknown_build_id: (tmp_path / f"silver_build_id={unknown_build_id}",),
    }

    result = SilverWorkspaceOrphanAuditor(
        silver_builds=builds,  # type: ignore[arg-type]
        silver_store=store,  # type: ignore[arg-type]
    ).audit()

    assert len(result.unknown_builds) == 1
    unknown = result.unknown_builds[0]
    assert unknown.artifact_name == unknown_build_id
    assert unknown.silver_build_id == unknown_build_id
    assert unknown.cause is UnknownArtifactCause.MISSING_BUILD_RECORD
    assert unknown.artifact_directories == (tmp_path / f"silver_build_id={unknown_build_id}",)
    assert builds.find_by_ids_requests == [frozenset({known_build_id, unknown_build_id})]
    assert builds.dataset_requests == []
    assert store.build_directory_requests == [None]


def test_workspace_audit_reports_malformed_build_id_without_database_query(tmp_path: Path) -> None:
    builds = FakeSilverBuildStore()
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories["not-a-uuid"] = (tmp_path / "silver_build_id=not-a-uuid",)

    result = SilverWorkspaceOrphanAuditor(
        silver_builds=builds,  # type: ignore[arg-type]
        silver_store=store,  # type: ignore[arg-type]
    ).audit()

    assert len(result.unknown_builds) == 1
    unknown = result.unknown_builds[0]
    assert unknown.artifact_name == "not-a-uuid"
    assert unknown.silver_build_id is None
    assert unknown.cause is UnknownArtifactCause.NOT_A_BUILD_ID
    assert unknown.artifact_directories == (tmp_path / "silver_build_id=not-a-uuid",)
    assert builds.find_by_ids_requests == [frozenset()]


def test_dataset_reconciliation_queries_only_requested_dataset_builds(tmp_path: Path) -> None:
    requested_build = make_build(
        silver_build_id="requested-build",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    other_build = make_build(
        silver_build_id="other-build",
        dataset_id="another.dataset",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    builds = FakeSilverBuildStore([requested_build, other_build])
    publications = FakePublicationStore()
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories = {
        "requested-build": (tmp_path / "silver_build_id=requested-build",),
        "other-build": (tmp_path / "silver_build_id=other-build",),
    }

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=builds,
        silver_store=store,
        assets=InMemoryPublicationAssetStore(publications=publications),
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert [orphan.silver_build_id for orphan in result.orphans] == ["requested-build"]
    assert builds.dataset_requests == [DATASET_ID]
    assert builds.find_by_ids_requests == []
    assert store.build_directory_requests == [frozenset({"requested-build"})]


def test_reconciliation_backfills_assets_from_valid_manifest(tmp_path: Path) -> None:
    publication = make_publication()
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    store = FakeSilverArtifactStore(tmp_path)
    store.manifests[publication.manifest_path] = make_manifest(publication=publication)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=store,
        assets=assets,
        backfill_assets=True,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert result.current_publication_id == publication.publication_id
    assert result.backfilled_publication_ids == (publication.publication_id,)
    assert result.manifest_failures == ()
    assert assets.register_calls == [publication.publication_id]
    assert len(assets.list_for_publication(publication_id=publication.publication_id)) == 1


def test_reconciliation_marks_both_projections_synchronized(tmp_path: Path) -> None:
    publication = make_publication()
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=publication.publication_id, assets=(make_asset_request(),))
    store = FakeSilverArtifactStore(tmp_path)
    store.manifests[publication.manifest_path] = make_manifest(publication=publication)
    projection_states = InMemoryPublicationProjectionStateStore()
    projection_states.mark_pending(
        dataset_id=DATASET_ID,
        current_publication_id=publication.publication_id,
        history_publication_id=publication.publication_id,
        changed_at=FIXED_NOW,
    )

    _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=store,
        assets=assets,
        projection_states=projection_states,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    states = tuple(projection_states.states.values())
    assert len(states) == 2
    assert all(state.status is PublicationProjectionStatus.SYNCHRONIZED for state in states)


def test_default_reconciliation_does_not_read_historical_manifests(tmp_path: Path) -> None:
    current = make_publication()
    historical = make_publication(
        publication_id="publication-old",
        version_period=date(2024, 1, 1),
        partition_value="2024",
        silver_build_id="build-old",
        manifest_path="data/files/silver/manifests/build-old.json",
        published_at=FIXED_NOW - timedelta(days=365),
        is_current=False,
    )
    publications = FakePublicationStore([historical, current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    store = FakeSilverArtifactStore(tmp_path)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=store,
        assets=assets,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert store.manifest_reads == []
    assert result.manifest_failures == ()
    assert result.current_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert result.history_projection.status is ProjectionReconciliationStatus.REPAIRED


def test_broken_historical_manifest_does_not_block_projection_repairs(tmp_path: Path) -> None:
    current = make_publication()
    historical = make_publication(
        publication_id="publication-old",
        version_period=date(2024, 1, 1),
        partition_value="2024",
        silver_build_id="build-old",
        manifest_path="data/files/silver/manifests/build-old.json",
        published_at=FIXED_NOW - timedelta(days=365),
        is_current=False,
    )
    publications = FakePublicationStore([historical, current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=current.publication_id, assets=(make_asset_request(),))
    store = FakeSilverArtifactStore(tmp_path)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=store,
        assets=assets,
        backfill_assets=True,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert result.current_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert result.history_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert result.backfilled_publication_ids == ()
    assert len(result.manifest_failures) == 1
    assert result.manifest_failures[0].publication_id == historical.publication_id
    assert result.manifest_failures[0].error_type == "KeyError"


def test_current_projection_failure_does_not_block_history_or_orphan_scan(tmp_path: Path) -> None:
    publication = make_publication()
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    projection_states = InMemoryPublicationProjectionStateStore()
    projection_states.mark_pending(
        dataset_id=DATASET_ID,
        current_publication_id=publication.publication_id,
        history_publication_id=publication.publication_id,
        changed_at=FIXED_NOW,
    )
    indexes = FakePublicationIndexService(
        current_result=_current_index_result(tmp_path=tmp_path, publication=publication),
        history_paths=(tmp_path / "history.sql",),
        current_error=FileNotFoundError("current manifest is missing"),
    )
    failed_build = make_build(
        silver_build_id="failed-build",
        status=SilverBuildStatus.FAILED,
        started_at=FIXED_NOW - timedelta(days=10),
        completed_at=FIXED_NOW - timedelta(days=9),
    )
    store = FakeSilverArtifactStore(tmp_path)
    store.build_directories["failed-build"] = (tmp_path / "silver_build_id=failed-build",)

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([failed_build]),
        silver_store=store,
        assets=assets,
        projection_states=projection_states,
        indexes=indexes,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert result.current_projection.status is ProjectionReconciliationStatus.FAILED
    assert result.current_projection.error_type == "FileNotFoundError"
    assert result.history_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert indexes.current_calls == 1
    assert indexes.history_calls == 1
    assert len(result.orphans) == 1

    current_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.CURRENT
    )
    history_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.HISTORY
    )
    assert current_state is not None
    assert current_state.status is PublicationProjectionStatus.STALE
    assert history_state is not None
    assert history_state.status is PublicationProjectionStatus.SYNCHRONIZED


def test_history_projection_failure_does_not_undo_current_repair(tmp_path: Path) -> None:
    publication = make_publication()
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    projection_states = InMemoryPublicationProjectionStateStore()
    projection_states.mark_pending(
        dataset_id=DATASET_ID,
        current_publication_id=publication.publication_id,
        history_publication_id=publication.publication_id,
        changed_at=FIXED_NOW,
    )
    indexes = FakePublicationIndexService(
        current_result=_current_index_result(tmp_path=tmp_path, publication=publication),
        history_error=RuntimeError("history projection write failed"),
    )

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
        projection_states=projection_states,
        indexes=indexes,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert result.current_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert result.history_projection.status is ProjectionReconciliationStatus.FAILED
    assert result.history_projection.error_type == "RuntimeError"

    current_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.CURRENT
    )
    history_state = projection_states.get(
        dataset_id=DATASET_ID, projection_kind=PublicationProjectionKind.HISTORY
    )
    assert current_state is not None
    assert current_state.status is PublicationProjectionStatus.SYNCHRONIZED
    assert history_state is not None
    assert history_state.status is PublicationProjectionStatus.STALE


def test_reconciliation_persists_corrupt_current_asset_without_blocking_repairs(
    tmp_path: Path,
) -> None:
    publication = make_publication()
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    asset = assets.register(
        publication_id=publication.publication_id, assets=(make_asset_request(),)
    )[0]
    failed_result = AssetIntegrityResult(
        file_path=asset.file_path,
        status=AssetIntegrityStatus.FAILED,
        expected_size_bytes=asset.size_bytes,
        actual_size_bytes=asset.size_bytes,
        expected_checksum=asset.checksum,
        actual_checksum="sha256:changed",
        failure_codes=(AssetIntegrityFailureCode.CHECKSUM_MISMATCH,),
    )
    batch = AssetIntegrityBatch(checked_at=FIXED_NOW, results=(failed_result,))
    integrity = MagicMock()
    integrity.inspect.return_value = batch
    verification_store = MagicMock()

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
        asset_integrity=integrity,
        asset_verifications=verification_store,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    expected_check = PublicationIntegrityCheck(
        publication_id=publication.publication_id,
        trigger=PublicationIntegrityTrigger.RECONCILIATION,
        batch=batch,
    )
    assert result.asset_verifications == (expected_check,)
    assert result.asset_verification_failures == ()
    assert result.asset_verifications[0].batch.failed_results == (failed_result,)
    verification_store.insert_check.assert_called_once_with(expected_check)
    assert result.current_projection.status is ProjectionReconciliationStatus.REPAIRED
    assert result.history_projection.status is ProjectionReconciliationStatus.REPAIRED


def test_superseded_asset_verification_is_opt_in(tmp_path: Path) -> None:
    current = make_publication()
    historical = make_publication(
        publication_id="publication-old",
        version_period=date(2024, 1, 1),
        partition_value="2024",
        silver_build_id="build-old",
        manifest_path="data/files/silver/manifests/build-old.json",
        published_at=FIXED_NOW - timedelta(days=365),
        is_active_revision=False,
        is_current=False,
    )
    publications = FakePublicationStore([historical, current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=current.publication_id, assets=(make_asset_request(),))
    assets.register(
        publication_id=historical.publication_id,
        assets=(
            make_asset_request(
                file_path=(
                    "data/files/silver/tables/adult-lead-county/"
                    "version_period=2024/silver_build_id=build-old/data.parquet"
                )
            ),
        ),
    )
    integrity = MagicMock()
    integrity.inspect.side_effect = _passed_asset_verification
    integrity_checks = MagicMock()

    _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
        asset_integrity=integrity,
        asset_verifications=integrity_checks,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert integrity.inspect.call_count == 1
    first_check = integrity_checks.insert_check.call_args.args[0]
    assert first_check.publication_id == current.publication_id

    integrity.reset_mock()
    integrity.inspect.side_effect = _passed_asset_verification
    integrity_checks.reset_mock()

    _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
        asset_integrity=integrity,
        asset_verifications=integrity_checks,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW, verify_history_assets=True)

    assert integrity.inspect.call_count == 2
    assert {
        call.args[0].publication_id for call in integrity_checks.insert_check.call_args_list
    } == {current.publication_id, historical.publication_id}


def test_all_active_dataset_versions_are_verified_by_default(tmp_path: Path) -> None:
    current = make_publication()
    older_active_version = make_publication(
        publication_id="publication-active-2024",
        version_period=date(2024, 1, 1),
        partition_value="2024",
        silver_build_id="build-active-2024",
        manifest_path="data/files/silver/manifests/build-active-2024.json",
        published_at=FIXED_NOW - timedelta(days=365),
        is_current=False,
    )
    publications = FakePublicationStore([older_active_version, current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=current.publication_id, assets=(make_asset_request(),))
    assets.register(
        publication_id=older_active_version.publication_id,
        assets=(
            make_asset_request(
                file_path=(
                    "data/files/silver/tables/adult-lead-county/"
                    "version_period=2024/silver_build_id=build-active-2024/data.parquet"
                )
            ),
        ),
    )
    integrity = MagicMock()
    integrity.inspect.side_effect = _passed_asset_verification
    integrity_checks = MagicMock()

    _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore(),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
        asset_integrity=integrity,
        asset_verifications=integrity_checks,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert {
        call.args[0].publication_id for call in integrity_checks.insert_check.call_args_list
    } == {current.publication_id, older_active_version.publication_id}


def test_reconciliation_verifies_recorded_manifest_hash(tmp_path: Path) -> None:
    manifest_path = "data/files/silver/manifests/build-1.json"
    manifest_file = tmp_path / manifest_path
    manifest_file.parent.mkdir(parents=True)
    content = b'{"artifact_type":"silver_build_manifest"}\n'
    manifest_file.write_bytes(content)
    publication = make_publication(manifest_path=manifest_path)
    build = make_build(
        status=SilverBuildStatus.SUCCEEDED,
        completed_at=FIXED_NOW,
        manifest_path=manifest_path,
        output_hash=hashlib.sha256(content).hexdigest(),
    )
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=publication.publication_id, assets=(make_asset_request(),))

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([build]),
        silver_store=FakeSilverArtifactStore(tmp_path),
        assets=assets,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert len(result.manifest_integrity_results) == 1
    assert not result.manifest_integrity_results[0].failed
    assert result.manifest_integrity_failures == ()


def test_reconciliation_verifies_contract_snapshot_after_manifest(tmp_path: Path) -> None:
    publication = make_publication()
    contract_path = "data/contracts/example/contract.yaml"
    contract_content = b"dataset:\n  id: example.records\n"
    contract_file = tmp_path / contract_path
    contract_file.parent.mkdir(parents=True)
    contract_file.write_bytes(contract_content)
    contract_checksum = hashlib.sha256(contract_content).hexdigest()
    manifest = make_manifest(
        publication=publication,
        contract_snapshot_path=contract_path,
        contract_checksum=f"sha256:{contract_checksum}",
    )
    manifest_content = json.dumps(manifest, indent=4, ensure_ascii=False).encode("utf-8")
    manifest_file = tmp_path / publication.manifest_path
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_bytes(manifest_content)
    build = make_build(
        status=SilverBuildStatus.SUCCEEDED,
        completed_at=FIXED_NOW,
        manifest_path=publication.manifest_path,
        output_hash=hashlib.sha256(manifest_content).hexdigest(),
    )
    publications = FakePublicationStore([publication])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=publication.publication_id, assets=(make_asset_request(),))
    store = FakeSilverArtifactStore(tmp_path)
    store.manifests[publication.manifest_path] = manifest

    result = _reconciler(
        tmp_path=tmp_path,
        publications=publications,
        builds=FakeSilverBuildStore([build]),
        silver_store=store,
        assets=assets,
    ).reconcile(dataset_id=DATASET_ID, now=FIXED_NOW)

    assert len(result.contract_snapshot_integrity_results) == 1
    assert not result.contract_snapshot_integrity_results[0].failed
    assert result.contract_snapshot_integrity_failures == ()
