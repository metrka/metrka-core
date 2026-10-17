from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from metrka_core.catalog.publication_asset_models import DatasetPublicationAssetRequest
from metrka_core.catalog.publication_candidate_models import (
    DatasetPublicationCandidate,
    DatasetPublicationCandidateStatus,
)
from metrka_core.pipeline.silver.approved_publication_unit_of_work import ApprovedPublicationCommand
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)
from metrka_core.pipeline.silver.postgres_approved_publication_unit_of_work import (
    PostgresApprovedPublicationUnitOfWork,
)
from metrka_core.pipeline.silver.publication_decision import SilverPublicationChangeKind
from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityFailureCode,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)
from metrka_core.quality.publication_integrity_models import PublicationIntegrityTrigger

from .fakes import (
    DATASET_ID,
    FIXED_NOW,
    LOGICAL_HASH,
    SCHEMA_HASH,
    make_asset,
    make_asset_request,
    make_build,
    make_manifest,
    make_publication,
)


def _session() -> MagicMock:
    session = MagicMock()
    session.transaction.return_value.__enter__.return_value = None
    session.cursor.return_value.__enter__.return_value = MagicMock()
    return session


def _candidate(
    *,
    baseline_publication_id: str | None = None,
    change_kind: SilverPublicationChangeKind = SilverPublicationChangeKind.INITIAL_PUBLICATION,
) -> DatasetPublicationCandidate:
    return DatasetPublicationCandidate(
        candidate_id="candidate-1",
        dataset_id=DATASET_ID,
        version_period=make_publication().version_period,
        partition_key="version_period",
        partition_value="2025",
        silver_build_id="build-1",
        baseline_publication_id=baseline_publication_id,
        change_kind=change_kind,
        status=DatasetPublicationCandidateStatus.APPROVED,
        fingerprint_version=1,
        logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
        schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
        logical_data_hash=LOGICAL_HASH,
        schema_hash=SCHEMA_HASH,
        requested_at=FIXED_NOW,
        approved_at=FIXED_NOW,
        approved_by="reviewer@example.test",
    )


def _passed_asset_verification(
    *, assets: tuple[DatasetPublicationAssetRequest, ...], checked_at: datetime
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


def _unit(
    *, candidate: DatasetPublicationCandidate, active_publication: object | None
) -> tuple[PostgresApprovedPublicationUnitOfWork, dict[str, MagicMock]]:
    session = _session()
    candidates = MagicMock()
    candidates.get_by_id_for_update.return_value = candidate
    builds = MagicMock()
    builds.get_by_id.return_value = make_build(
        status=SilverBuildStatus.SUCCEEDED, completed_at=FIXED_NOW
    )
    publication = make_publication(publication_id="publication-generated")
    publications = MagicMock()
    publications.find_active.return_value = active_publication
    publications.publish.return_value = publication
    publications.find_current.return_value = publication
    assets = MagicMock()
    assets.register.return_value = (make_asset(publication=publication),)
    asset_integrity = MagicMock()
    asset_integrity.inspect.side_effect = _passed_asset_verification
    integrity_batches = MagicMock()
    integrity_batches.insert_batch.return_value = 41
    publication_integrity = MagicMock()
    gate_evidence = MagicMock()
    projection_states = MagicMock()
    silver_store = MagicMock()
    silver_store.read_manifest.return_value = make_manifest(publication=publication)
    candidates.mark_published.return_value = replace(
        candidate,
        status=DatasetPublicationCandidateStatus.PUBLISHED,
        publication_id=publication.publication_id,
    )

    publication_ids = MagicMock()
    publication_ids.new_publication_id.return_value = "publication-generated"
    unit = PostgresApprovedPublicationUnitOfWork(
        session=session,
        candidates=candidates,
        silver_builds=builds,
        publications=publications,
        publication_assets=assets,
        publication_asset_integrity=asset_integrity,
        asset_integrity_batches=integrity_batches,
        publication_integrity=publication_integrity,
        publication_gate_evidence=gate_evidence,
        projection_states=projection_states,
        silver_store=silver_store,
        publication_ids=publication_ids,
    )
    return unit, {
        "session": session,
        "candidates": candidates,
        "publications": publications,
        "assets": assets,
        "asset_integrity": asset_integrity,
        "integrity_batches": integrity_batches,
        "publication_integrity": publication_integrity,
        "gate_evidence": gate_evidence,
        "projection_states": projection_states,
        "silver_store": silver_store,
        "publication_ids": publication_ids,
    }


def test_approved_initial_candidate_is_published_atomically() -> None:
    candidate = _candidate()
    unit, collaborators = _unit(candidate=candidate, active_publication=None)

    result = unit.commit(
        ApprovedPublicationCommand(candidate_id=candidate.candidate_id, published_at=FIXED_NOW)
    )
    publication_request = collaborators["publications"].publish.call_args.args[0]

    assert publication_request.publication_id == "publication-generated"
    collaborators["publication_ids"].new_publication_id.assert_called_once_with()

    assert result.candidate.status is DatasetPublicationCandidateStatus.PUBLISHED
    assert result.publication.publication_id == "publication-generated"
    assert result.current_publication == result.publication
    assert len(result.publication_assets) == 1
    collaborators["publications"].publish.assert_called_once()
    collaborators["assets"].register.assert_called_once()
    collaborators["asset_integrity"].inspect.assert_called_once()
    collaborators["gate_evidence"].insert_attempt.assert_called_once()
    collaborators["integrity_batches"].insert_batch.assert_called_once()
    collaborators["publication_integrity"].link_batch.assert_called_once()
    gate_attempt = collaborators["gate_evidence"].insert_attempt.call_args.args[0]
    integrity_link = collaborators["publication_integrity"].link_batch.call_args.args[0]
    assert gate_attempt.integrity_batch_id == 41
    assert integrity_link.publication_id == result.publication.publication_id
    assert integrity_link.integrity_batch_id == 41
    assert integrity_link.trigger is PublicationIntegrityTrigger.PUBLICATION_COMMIT
    collaborators["candidates"].mark_published.assert_called_once()
    collaborators["projection_states"].mark_pending.assert_called_once_with(
        dataset_id=publication_request.dataset_id,
        current_publication_id=result.current_publication.publication_id,
        history_publication_id=result.publication.publication_id,
        changed_at=FIXED_NOW,
    )
    collaborators["session"].transaction.assert_called_once()


def test_stale_approved_candidate_is_not_published() -> None:
    candidate = _candidate(
        baseline_publication_id="publication-old",
        change_kind=SilverPublicationChangeKind.LOGICAL_DATA_CHANGED,
    )
    unit, collaborators = _unit(
        candidate=candidate, active_publication=make_publication(publication_id="publication-new")
    )

    with pytest.raises(RuntimeError, match="candidate is stale"):
        unit.commit(
            ApprovedPublicationCommand(candidate_id=candidate.candidate_id, published_at=FIXED_NOW)
        )

    collaborators["publications"].publish.assert_not_called()
    collaborators["silver_store"].read_manifest.assert_not_called()
    collaborators["assets"].register.assert_not_called()
    collaborators["candidates"].mark_published.assert_not_called()
    collaborators["projection_states"].mark_pending.assert_not_called()


def test_failed_asset_integrity_prevents_publication() -> None:
    candidate = _candidate()
    unit, collaborators = _unit(candidate=candidate, active_publication=None)
    failed_result = AssetIntegrityResult(
        file_path=make_asset_request().file_path,
        status=AssetIntegrityStatus.FAILED,
        expected_size_bytes=12345,
        actual_size_bytes=12346,
        expected_checksum="sha256:expected",
        actual_checksum="sha256:actual",
        failure_codes=(AssetIntegrityFailureCode.SIZE_MISMATCH,),
    )
    collaborators["asset_integrity"].inspect.side_effect = None
    collaborators["asset_integrity"].inspect.return_value = AssetIntegrityBatch(
        checked_at=FIXED_NOW, results=(failed_result,)
    )

    with pytest.raises(RuntimeError, match="asset integrity verification failed"):
        unit.commit(
            ApprovedPublicationCommand(candidate_id=candidate.candidate_id, published_at=FIXED_NOW)
        )

    collaborators["publications"].publish.assert_not_called()
    collaborators["assets"].register.assert_not_called()
    collaborators["gate_evidence"].insert_attempt.assert_called_once()
    collaborators["integrity_batches"].insert_batch.assert_called_once()
    collaborators["publication_integrity"].link_batch.assert_not_called()
    collaborators["candidates"].mark_published.assert_not_called()

    gate_attempt = collaborators["gate_evidence"].insert_attempt.call_args.args[0]
    assert gate_attempt.candidate_id == candidate.candidate_id
    assert gate_attempt.silver_build_id == candidate.silver_build_id
    assert gate_attempt.pipeline_run_id == "pipeline-1"
    assert gate_attempt.integrity_batch_id == 41
    collaborators["publication_ids"].new_publication_id.assert_not_called()
    collaborators["session"].transaction.return_value.__exit__.assert_called_once_with(
        None, None, None
    )


def test_older_publication_keeps_current_projection_expectation() -> None:
    candidate = _candidate()
    unit, collaborators = _unit(candidate=candidate, active_publication=None)
    current_publication = make_publication(publication_id="publication-current")
    collaborators["publications"].find_current.return_value = current_publication

    result = unit.commit(
        ApprovedPublicationCommand(candidate_id=candidate.candidate_id, published_at=FIXED_NOW)
    )

    assert result.current_publication == current_publication
    collaborators["projection_states"].mark_pending.assert_called_once_with(
        dataset_id=result.publication.dataset_id,
        current_publication_id=current_publication.publication_id,
        history_publication_id=result.publication.publication_id,
        changed_at=FIXED_NOW,
    )
