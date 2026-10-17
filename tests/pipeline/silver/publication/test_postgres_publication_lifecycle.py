from __future__ import annotations

import os
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest

from metrka_core.catalog.postgres_publication_projection_store import (
    PostgresDatasetPublicationProjectionStateStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.catalog.publication_models import DatasetPublicationRequest
from metrka_core.catalog.publication_projection_models import (
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)

TEST_DSN = os.environ.get("METRKA_MIGRATION_TEST_DSN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DSN, reason="METRKA_MIGRATION_TEST_DSN is not configured"),
]


class RollbackTestData(Exception):
    pass


def _test_dsn() -> str:
    if TEST_DSN is None:
        raise RuntimeError("Migration test DSN is missing")
    return TEST_DSN


def _request(
    *,
    publication_id: str,
    pipeline_run_id: str,
    dataset_id: str,
    build_id: str,
    engine_id: str,
    version_period: date,
    partition_value: str,
) -> DatasetPublicationRequest:
    return DatasetPublicationRequest(
        publication_id=publication_id,
        pipeline_run_id=pipeline_run_id,
        dataset_id=dataset_id,
        version_period=version_period,
        partition_key="version_period",
        partition_value=partition_value,
        silver_build_id=build_id,
        engine_release_id=engine_id,
        processing_config_hash="a" * 64,
        quality_config_hash="b" * 64,
        fingerprint_version=1,
        logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
        schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
        logical_data_hash="d" * 64,
        schema_hash="e" * 64,
        manifest_path=f"data/files/silver/manifests/{build_id}.json",
        published_at=datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
    )


def test_postgres_revision_and_current_lifecycle() -> None:
    token = uuid4().hex
    dataset_id = f"integration.publication.{token}"
    file_id = f"file-{token}"
    pipeline_id = f"pipeline-{token}"
    engine_id = f"engine-{token}"
    build_ids = [str(uuid4()) for _ in range(3)]

    with PostgresSession(_test_dsn()) as session:
        try:
            with session.transaction(), session.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO logs.pipeline_runs (
                        pipeline_run_id,
                        workspace_name,
                        config_name,
                        started_at,
                        finished_at,
                        status,
                        code_provenance
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        'success',
                        '{}'::jsonb
                    )
                    """,
                    (
                        pipeline_id,
                        f"integration-workspace-{token}",
                        "integration.yaml",
                        datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
                        datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO meta.silver_engine_releases (
                        engine_release_id,
                        release_hash,
                        engine_hash,
                        engine_fingerprint_version,
                        runtime_hash,
                        runtime_fingerprint_version,
                        component_hashes,
                        runtime_versions,
                        core_commit_sha,
                        status,
                        detected_at
                    )
                    VALUES (%s, %s, %s, 1, %s, 1, '{}'::jsonb, '{}'::jsonb, %s, 'candidate', %s)
                    """,
                    (
                        engine_id,
                        "1" * 64,
                        "2" * 64,
                        "3" * 64,
                        "commit-1",
                        datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO meta.marshaled_files (
                        dataset_file_id,
                        dataset_id,
                        artifact_role,
                        bronze_artifacts
                    )
                    VALUES (%s, %s, 'data', '[]'::jsonb)
                    """,
                    (file_id, dataset_id),
                )

                for build_id in build_ids:
                    cursor.execute(
                        """
                        INSERT INTO logs.silver_build_attempts (
                            silver_build_id,
                            pipeline_run_id,
                            silver_run_id,
                            dataset_file_id,
                            dataset_id,
                            contract_hash,
                            engine_release_id,
                            processing_config_hash,
                            quality_config_hash,
                            build_signature,
                            fingerprint_version,
                            status,
                            rebuild_mode,
                            rebuild_reasons,
                            started_at,
                            completed_at,
                            logical_hash_algorithm,
                            schema_hash_algorithm
                        )
                        VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            1, 'succeeded', 'automatic',
                            '["initial_build"]'::jsonb,
                            %s, %s, %s, %s
                        )
                        """,
                        (
                            build_id,
                            pipeline_id,
                            f"silver-{build_id}",
                            file_id,
                            dataset_id,
                            "c" * 64,
                            engine_id,
                            "a" * 64,
                            "b" * 64,
                            "f" * 64,
                            datetime(2026, 8, 13, 11, 0, tzinfo=UTC),
                            datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
                            LOGICAL_DATA_HASH_ALGORITHM,
                            SCHEMA_HASH_ALGORITHM,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO meta.silver_materializations (
                            silver_build_id,
                            manifest_path,
                            output_hash,
                            output_file_count,
                            output_byte_count,
                            logical_data_hash,
                            schema_hash,
                            materialized_at
                        )
                        VALUES (
                            %s, %s, NULL, 1, 1,
                            %s, %s, %s
                        )
                        """,
                        (
                            build_id,
                            f"data/files/silver/manifests/{build_id}.json",
                            "d" * 64,
                            "e" * 64,
                            datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
                        ),
                    )

                publications = PostgresDatasetPublicationStore(session)
                first = publications.publish(
                    _request(
                        publication_id=f"publication-{token}-1",
                        pipeline_run_id=pipeline_id,
                        dataset_id=dataset_id,
                        build_id=build_ids[0],
                        engine_id=engine_id,
                        version_period=date(2025, 1, 1),
                        partition_value="2025",
                    )
                )
                second_request = _request(
                    publication_id=f"publication-{token}-2",
                    pipeline_run_id=pipeline_id,
                    dataset_id=dataset_id,
                    build_id=build_ids[1],
                    engine_id=engine_id,
                    version_period=date(2025, 1, 1),
                    partition_value="2025",
                )
                second = publications.publish(second_request)
                older = publications.publish(
                    _request(
                        publication_id=f"publication-{token}-3",
                        pipeline_run_id=pipeline_id,
                        dataset_id=dataset_id,
                        build_id=build_ids[2],
                        engine_id=engine_id,
                        version_period=date(2024, 1, 1),
                        partition_value="2024",
                    )
                )

                assert first.revision == 1
                assert second.revision == 2
                assert second.supersedes_publication_id == first.publication_id
                assert second.is_current
                assert not older.is_current
                assert publications.find_current(dataset_id) == second
                assert publications.publish(second_request) == second

                projection_states = PostgresDatasetPublicationProjectionStateStore(session)
                pending_states = projection_states.mark_pending(
                    dataset_id=dataset_id,
                    current_publication_id=second.publication_id,
                    history_publication_id=older.publication_id,
                    changed_at=datetime(2026, 8, 13, 12, 1, tzinfo=UTC),
                )
                assert len(pending_states) == 2
                assert all(
                    state.status is PublicationProjectionStatus.PENDING for state in pending_states
                )

                synchronized = projection_states.mark_synchronized(
                    dataset_id=dataset_id,
                    projection_kind=PublicationProjectionKind.CURRENT,
                    publication_id=second.publication_id,
                    checked_at=datetime(2026, 8, 13, 12, 2, tzinfo=UTC),
                )
                stale = projection_states.mark_stale(
                    dataset_id=dataset_id,
                    projection_kind=PublicationProjectionKind.HISTORY,
                    expected_publication_id=older.publication_id,
                    checked_at=datetime(2026, 8, 13, 12, 3, tzinfo=UTC),
                    error={"type": "RuntimeError", "message": "history unavailable"},
                )

                assert synchronized.status is PublicationProjectionStatus.SYNCHRONIZED
                assert stale.status is PublicationProjectionStatus.STALE
                assert (
                    projection_states.get(
                        dataset_id=dataset_id, projection_kind=PublicationProjectionKind.HISTORY
                    )
                    == stale
                )

                resynchronized = projection_states.mark_synchronized(
                    dataset_id=dataset_id,
                    projection_kind=PublicationProjectionKind.HISTORY,
                    publication_id=older.publication_id,
                    checked_at=datetime(2026, 8, 13, 12, 4, tzinfo=UTC),
                )
                superseded_failure = projection_states.mark_stale(
                    dataset_id=dataset_id,
                    projection_kind=PublicationProjectionKind.HISTORY,
                    expected_publication_id=second.publication_id,
                    checked_at=datetime(2026, 8, 13, 12, 5, tzinfo=UTC),
                    error={"type": "RuntimeError", "message": "obsolete refresh failed"},
                )

                assert resynchronized.status is PublicationProjectionStatus.SYNCHRONIZED
                assert superseded_failure == resynchronized
                assert (
                    projection_states.get(
                        dataset_id=dataset_id, projection_kind=PublicationProjectionKind.HISTORY
                    )
                    == resynchronized
                )

                raise RollbackTestData
        except RollbackTestData:
            pass

        with session.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM catalog.dataset_publications
                WHERE dataset_id = %s
                """,
                (dataset_id,),
            )
            remaining = cursor.fetchone()

    assert remaining is not None
    assert int(remaining["count"]) == 0
