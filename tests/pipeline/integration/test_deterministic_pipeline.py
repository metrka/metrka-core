"""Behavioural proof that a full pipeline reproduces one published dataset."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from metrka_core.catalog.postgres_publication_asset_store import (
    PostgresDatasetPublicationAssetStore,
)
from metrka_core.catalog.postgres_publication_candidate_store import (
    PostgresDatasetPublicationCandidateStore,
)
from metrka_core.catalog.postgres_publication_projection_store import (
    PostgresDatasetPublicationProjectionStateStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.bootstrap import open_pipeline_context
from metrka_core.pipeline.composition import runtime as runtime_composition
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.default_registry import create_core_registry
from metrka_core.pipeline.provenance import CodeProvenance
from metrka_core.pipeline.runner import execute_configured_pipeline
from metrka_core.pipeline.silver.approved_publication_unit_of_work import ApprovedPublicationCommand
from metrka_core.pipeline.silver.build_models import SilverBuild, SilverBuildStatus
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)
from metrka_core.pipeline.silver.postgres_approved_publication_unit_of_work import (
    PostgresApprovedPublicationUnitOfWork,
)
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_asset_integrity import (
    PublicationAssetIntegrityError,
    Sha256PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.publication_asset_mapping import publication_assets_from_manifest
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.postgres_publication_gate_evidence_store import (
    PostgresPublicationGateEvidenceStore,
)
from metrka_core.storage.silver_store import LocalSilverArtifactStore
from metrka_core.storage.workspace_layout import WorkspaceLayout

from .deterministic_pipeline_support import (
    PIPELINE_TIME,
    TARGET_DATE,
    DeterministicWorkspace,
    FixedPublicationIds,
    create_test_workspace,
    fixed_code_provenance,
    runtime_services,
)

TEST_DSN = os.environ.get("METRKA_MIGRATION_TEST_DSN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DSN, reason="METRKA_MIGRATION_TEST_DSN is not configured"),
]


@dataclass(frozen=True)
class CompletedRun:
    pipeline_run_id: str
    build: SilverBuild
    engine_hash: str


@dataclass(frozen=True)
class FixedWorkspaceLocationResolver:
    workspace_name: str
    workspace_root: Path

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        if workspace_name != self.workspace_name:
            raise KeyError(workspace_name)

        return WorkspaceLocation.portable(
            workspace_name=self.workspace_name, workspace_root=self.workspace_root
        )


def _test_dsn() -> str:
    if TEST_DSN is None:
        raise RuntimeError("Migration test DSN is missing")

    return TEST_DSN


def _run_pipeline(
    *, workspace: DeterministicWorkspace, token: str, run_label: str, force_rebuild: bool
) -> CompletedRun:
    services = runtime_services(
        token=token, run_label=run_label, source_capture_id=workspace.source_capture_id
    )
    pipeline_run_id = services.pipeline_run_ids.new_pipeline_run_id()
    silver_build_id = services.silver_build_ids.new_silver_build_id()

    with open_pipeline_context(
        workspace_name=workspace.name,
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
        services=services,
        workspace_location_resolver=FixedWorkspaceLocationResolver(
            workspace_name=workspace.name, workspace_root=workspace.root
        ),
        metadata_conninfo=_test_dsn(),
    ) as context:
        state = execute_configured_pipeline(
            context=context,
            registry=create_core_registry(),
            target_date=TARGET_DATE,
            source_capture_id=workspace.source_capture_id,
            action_option_overrides={
                "silver.process": {
                    "force_rebuild": force_rebuild,
                    "target_dataset_id": workspace.dataset_id,
                }
            },
        )

        assert [result.outcome.status for result in state.action_outcomes] == [
            "completed",
            "completed",
        ]

        build = context.silver.outputs.silver_build_store.get_by_id(silver_build_id)

        if build is None:
            raise AssertionError(f"Silver build was not persisted: {silver_build_id}")

        engine_hash = context.silver.engine.runtime.identity.engine_hash

    return CompletedRun(pipeline_run_id=pipeline_run_id, build=build, engine_hash=engine_hash)


def _publish_first_candidate(
    *, workspace: DeterministicWorkspace, token: str, candidate_id: str
) -> DatasetPublication:
    layout = WorkspaceLayout(
        location=WorkspaceLocation.portable(
            workspace_name=workspace.name, workspace_root=workspace.root
        )
    )
    silver_store = LocalSilverArtifactStore(
        workspace_root=layout.data_root,
        silver_root=layout.silver_dir,
        current_root=layout.current_dir,
    )

    with PostgresSession(_test_dsn()) as session:
        candidates = PostgresDatasetPublicationCandidateStore(session)
        approved = candidates.approve(
            candidate_id=candidate_id,
            approved_by="deterministic-integration-test",
            approved_at=PIPELINE_TIME,
        )
        integrity_evidence = PostgresAssetIntegrityEvidenceStore(session)

        publisher = PostgresApprovedPublicationUnitOfWork(
            session=session,
            candidates=candidates,
            silver_builds=PostgresSilverBuildStore(session),
            publications=PostgresDatasetPublicationStore(session),
            publication_assets=PostgresDatasetPublicationAssetStore(session),
            publication_asset_integrity=Sha256PublicationAssetIntegrityVerifier(silver_store),
            asset_integrity_batches=integrity_evidence,
            publication_integrity=integrity_evidence,
            publication_gate_evidence=PostgresPublicationGateEvidenceStore(session),
            projection_states=PostgresDatasetPublicationProjectionStateStore(session),
            silver_store=silver_store,
            publication_ids=FixedPublicationIds(f"publication_{token}"),
        )

        result = publisher.commit(
            ApprovedPublicationCommand(
                candidate_id=approved.candidate_id, published_at=PIPELINE_TIME
            )
        )

    return result.publication


def _pipeline_receipt(pipeline_run_id: str) -> dict[str, Any]:
    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                workspace_name,
                config_name,
                status,
                code_provenance,
                error,
                source_capture_id
            FROM logs.pipeline_runs
            WHERE pipeline_run_id = %s
            """,
            (pipeline_run_id,),
        )
        row = cursor.fetchone()

    if row is None:
        raise AssertionError(f"Pipeline run receipt was not persisted: {pipeline_run_id}")

    return dict(row)


def _publication_integrity_receipt(publication_id: str) -> tuple[int, set[str], set[str]]:
    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(result.file_path) AS verification_count,
                ARRAY_AGG(DISTINCT result.status) AS statuses,
                ARRAY_AGG(DISTINCT check_record.verification_trigger) AS triggers
            FROM quality.publication_integrity_checks AS check_record
            JOIN quality.asset_integrity_results AS result
              ON result.integrity_batch_id = check_record.integrity_batch_id
            WHERE check_record.publication_id = %s
            """,
            (publication_id,),
        )
        row = cursor.fetchone()

    if row is None:
        raise AssertionError(
            f"Publication asset verification receipt was not persisted: {publication_id}"
        )

    return (
        int(row["verification_count"]),
        {str(value) for value in row["statuses"] or ()},
        {str(value) for value in row["triggers"] or ()},
    )


def _publication_gate_receipt(candidate_id: str) -> tuple[str, int, set[str]]:
    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                CASE
                    WHEN BOOL_AND(result.status = 'passed') THEN 'passed'
                    ELSE 'failed'
                END AS status,
                COUNT(result.file_path) AS result_count,
                ARRAY_AGG(DISTINCT result.status) AS result_statuses
            FROM quality.publication_gate_attempts AS attempt
            JOIN quality.asset_integrity_results AS result
              ON result.integrity_batch_id = attempt.integrity_batch_id
            WHERE attempt.candidate_id = %s
            GROUP BY attempt.gate_attempt_id
            ORDER BY attempt.gate_attempt_id DESC
            LIMIT 1
            """,
            (candidate_id,),
        )
        row = cursor.fetchone()

    if row is None:
        raise AssertionError(f"Publication gate receipt was not persisted: {candidate_id}")

    return (
        str(row["status"]),
        int(row["result_count"]),
        {str(value) for value in row["result_statuses"] or ()},
    )


def _semantic_receipt(run: CompletedRun) -> bytes:
    """Serialize only reproducibility-relevant fields, excluding operational identity."""

    build = run.build

    if build.version_period is None:
        raise AssertionError("Successful build contains no version_period")

    if build.logical_data_hash is None or build.schema_hash is None:
        raise AssertionError("Successful build contains no logical fingerprint")

    payload = {
        "pipeline": _pipeline_receipt(run.pipeline_run_id),
        "materialization": {
            "dataset_file_id": build.dataset_file_id,
            "dataset_id": build.dataset_id,
            "version_period": build.version_period.isoformat(),
            "partition_key": build.partition_key,
            "partition_value": build.partition_value,
            "contract_hash": build.contract_hash,
            "engine_release_id": build.engine_release_id,
            "processing_config_hash": build.processing_config_hash,
            "quality_config_hash": build.quality_config_hash,
            "fingerprint_version": build.fingerprint_version,
            "logical_data_hash": build.logical_data_hash,
            "schema_hash": build.schema_hash,
            "status": build.status.value,
        },
    }

    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )


def _assert_no_failed_quality_checks(*pipeline_run_ids: str) -> None:
    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS failure_count
            FROM quality.quality_check_runs
            WHERE pipeline_run_id = ANY(%s)
              AND status IN ('failed', 'error')
            """,
            (list(pipeline_run_ids),),
        )
        row = cursor.fetchone()

    assert row is not None
    assert int(row["failure_count"]) == 0


def test_same_input_reproduces_publication_without_creating_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the real configured pipeline twice and prove semantic equivalence."""

    token = uuid4().hex[:12]
    workspace = create_test_workspace(base_dir=tmp_path, token=token)
    provenance = fixed_code_provenance()

    def collect_test_provenance(*, definition_path: Path) -> CodeProvenance:
        assert definition_path == workspace.root.resolve()
        return provenance

    monkeypatch.setattr(runtime_composition, "collect_code_provenance", collect_test_provenance)

    first = _run_pipeline(workspace=workspace, token=token, run_label="first", force_rebuild=False)

    first_candidate_id = f"publication_candidate_{token}_first"
    publication = _publish_first_candidate(
        workspace=workspace, token=token, candidate_id=first_candidate_id
    )

    second = _run_pipeline(workspace=workspace, token=token, run_label="second", force_rebuild=True)

    assert first.pipeline_run_id != second.pipeline_run_id
    assert first.build.silver_build_id != second.build.silver_build_id
    assert first.build.manifest_path != second.build.manifest_path
    assert first.engine_hash == second.engine_hash
    assert first.build.status is SilverBuildStatus.SUCCEEDED
    assert second.build.status is SilverBuildStatus.SUCCEEDED

    assert _semantic_receipt(first) == _semantic_receipt(second)

    assert publication.logical_data_hash == first.build.logical_data_hash
    assert publication.schema_hash == first.build.schema_hash
    assert publication.logical_hash_algorithm == LOGICAL_DATA_HASH_ALGORITHM
    assert publication.schema_hash_algorithm == SCHEMA_HASH_ALGORITHM
    assert publication.logical_data_hash == second.build.logical_data_hash
    assert publication.schema_hash == second.build.schema_hash

    asset_verification_receipt = _publication_integrity_receipt(publication.publication_id)
    verification_count, asset_statuses, asset_triggers = asset_verification_receipt
    assert verification_count > 0
    assert asset_statuses == {"passed"}
    assert asset_triggers == {"publication_commit"}

    gate_status, gate_result_count, gate_result_statuses = _publication_gate_receipt(
        first_candidate_id
    )
    assert gate_status == "passed"
    assert gate_result_count > 0
    assert gate_result_statuses == {"passed"}

    with PostgresSession(_test_dsn()) as session:
        publications = PostgresDatasetPublicationStore(session).list_all(
            dataset_id=workspace.dataset_id
        )
        with session.cursor() as cursor:
            cursor.execute(
                """
                SELECT candidate_id
                FROM catalog.dataset_publication_candidates
                WHERE silver_build_id = %s
                """,
                (second.build.silver_build_id,),
            )
            second_candidate = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    logical_hash_algorithm,
                    schema_hash_algorithm,
                    latest_silver_build_id,
                    verification_count
                FROM quality.silver_publication_verifications
                WHERE publication_id = %s
                  AND engine_hash = %s
                  AND logical_hash_algorithm = %s
                  AND schema_hash_algorithm = %s
                """,
                (
                    publication.publication_id,
                    second.engine_hash,
                    LOGICAL_DATA_HASH_ALGORITHM,
                    SCHEMA_HASH_ALGORITHM,
                ),
            )
            verification = cursor.fetchone()

    assert len(publications) == 1
    assert publications[0].publication_id == publication.publication_id
    assert second_candidate is None
    assert verification is not None
    assert verification["logical_hash_algorithm"] == LOGICAL_DATA_HASH_ALGORITHM
    assert verification["schema_hash_algorithm"] == SCHEMA_HASH_ALGORITHM
    assert str(verification["latest_silver_build_id"]) == second.build.silver_build_id
    assert int(verification["verification_count"]) == 1

    _assert_no_failed_quality_checks(first.pipeline_run_id, second.pipeline_run_id)


def test_failed_publication_gate_persists_evidence_without_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = uuid4().hex[:12]
    workspace = create_test_workspace(base_dir=tmp_path, token=token)
    provenance = fixed_code_provenance()

    def collect_test_provenance(*, definition_path: Path) -> CodeProvenance:
        assert definition_path == workspace.root.resolve()
        return provenance

    monkeypatch.setattr(runtime_composition, "collect_code_provenance", collect_test_provenance)

    completed = _run_pipeline(
        workspace=workspace, token=token, run_label="failed-gate", force_rebuild=False
    )

    if completed.build.manifest_path is None:
        raise AssertionError("Successful build contains no manifest path")

    layout = WorkspaceLayout(
        location=WorkspaceLocation.portable(
            workspace_name=workspace.name, workspace_root=workspace.root
        )
    )
    silver_store = LocalSilverArtifactStore(
        workspace_root=layout.data_root,
        silver_root=layout.silver_dir,
        current_root=layout.current_dir,
    )
    manifest = silver_store.read_manifest(path=completed.build.manifest_path)
    requested_assets = publication_assets_from_manifest(manifest)
    damaged_path = silver_store.resolve_publication_asset_path(requested_assets[0].file_path)
    damaged_path.write_bytes(damaged_path.read_bytes() + b"damaged-after-build")

    candidate_id = f"publication_candidate_{token}_failed-gate"

    with pytest.raises(PublicationAssetIntegrityError):
        _publish_first_candidate(workspace=workspace, token=token, candidate_id=candidate_id)

    gate_status, gate_result_count, gate_result_statuses = _publication_gate_receipt(candidate_id)
    assert gate_status == "failed"
    assert gate_result_count > 0
    assert "failed" in gate_result_statuses

    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT status, publication_id
            FROM catalog.dataset_publication_candidates
            WHERE candidate_id = %s
            """,
            (candidate_id,),
        )
        candidate = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*) AS publication_count
            FROM catalog.dataset_publications
            WHERE silver_build_id = %s
            """,
            (completed.build.silver_build_id,),
        )
        publication_count = cursor.fetchone()

    assert candidate is not None
    assert candidate["status"] == "approved"
    assert candidate["publication_id"] is None
    assert publication_count is not None
    assert int(publication_count["publication_count"]) == 0
