"""PostgreSQL persistence for dataset publications."""

from __future__ import annotations

from typing import Any

from metrka_core.catalog.publication_models import DatasetPublication, DatasetPublicationRequest
from metrka_core.metadata.postgres import PostgresSession

PUBLICATION_COLUMNS = """
    publication_id,
    pipeline_run_id,
    dataset_id,
    version_period,
    partition_key,
    partition_value,
    revision,
    silver_build_id,
    engine_release_id,
    processing_config_hash,
    quality_config_hash,
    fingerprint_version,
    logical_hash_algorithm,
    schema_hash_algorithm,
    logical_data_hash,
    schema_hash,
    manifest_path,
    published_at,
    is_active_revision,
    is_current,
    supersedes_publication_id
"""


def _row_to_publication(row: Any) -> DatasetPublication:
    """Convert one PostgreSQL row into a publication."""

    record = dict(row)

    supersedes_publication_id = record.get("supersedes_publication_id")

    return DatasetPublication(
        publication_id=str(record["publication_id"]),
        pipeline_run_id=str(record["pipeline_run_id"]),
        dataset_id=str(record["dataset_id"]),
        version_period=record["version_period"],
        partition_key=str(record["partition_key"]),
        partition_value=str(record["partition_value"]),
        revision=int(record["revision"]),
        silver_build_id=str(record["silver_build_id"]),
        engine_release_id=str(record["engine_release_id"]),
        processing_config_hash=str(record["processing_config_hash"]),
        quality_config_hash=str(record["quality_config_hash"]),
        fingerprint_version=int(record["fingerprint_version"]),
        logical_hash_algorithm=str(record["logical_hash_algorithm"]),
        schema_hash_algorithm=str(record["schema_hash_algorithm"]),
        logical_data_hash=str(record["logical_data_hash"]),
        schema_hash=str(record["schema_hash"]),
        manifest_path=str(record["manifest_path"]),
        published_at=record["published_at"],
        is_active_revision=bool(record["is_active_revision"]),
        is_current=bool(record["is_current"]),
        supersedes_publication_id=(
            str(supersedes_publication_id) if supersedes_publication_id is not None else None
        ),
    )


class PostgresDatasetPublicationStore:
    """Persist and query published dataset versions."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def publish(self, request: DatasetPublicationRequest) -> DatasetPublication:
        """
        Publish one successful Silver build.

        Publication revisions for one dataset are serialized
        with a PostgreSQL advisory transaction lock.
        """

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                    SELECT pg_advisory_xact_lock(
                        hashtextextended(%s, 0)
                    )
                    """,
                ((f"dataset-publication:{request.dataset_id}"),),
            )

            cursor.execute(
                f"""
                    SELECT
                        {PUBLICATION_COLUMNS}
                    FROM catalog.dataset_publications
                    WHERE silver_build_id = %s
                    """,
                (request.silver_build_id,),
            )

            existing_row = cursor.fetchone()

            if existing_row is not None:
                existing = _row_to_publication(existing_row)

                if (
                    existing.dataset_id != request.dataset_id
                    or existing.partition_value != request.partition_value
                ):
                    raise RuntimeError(
                        "Silver build is already published "
                        "for a different dataset version: "
                        f"{request.silver_build_id}"
                    )

                return existing

            cursor.execute(
                f"""
                    SELECT
                        {PUBLICATION_COLUMNS}
                    FROM catalog.dataset_publications
                    WHERE dataset_id = %s
                      AND is_current = true
                    FOR UPDATE
                    """,
                (request.dataset_id,),
            )

            current_row = cursor.fetchone()

            current_publication = (
                _row_to_publication(current_row) if current_row is not None else None
            )

            cursor.execute(
                f"""
                    SELECT
                        {PUBLICATION_COLUMNS}
                    FROM catalog.dataset_publications
                    WHERE dataset_id = %s
                      AND partition_value = %s
                      AND is_active_revision = true
                    FOR UPDATE
                    """,
                (request.dataset_id, request.partition_value),
            )

            previous_row = cursor.fetchone()

            previous_publication = (
                _row_to_publication(previous_row) if previous_row is not None else None
            )

            cursor.execute(
                """
                    SELECT
                        COALESCE(MAX(revision), 0) + 1
                            AS next_revision
                    FROM catalog.dataset_publications
                    WHERE dataset_id = %s
                      AND partition_value = %s
                    """,
                (request.dataset_id, request.partition_value),
            )

            revision_row = cursor.fetchone()

            if revision_row is None:
                raise RuntimeError("Could not calculate the next publication revision")

            revision = int(dict(revision_row)["next_revision"])

            is_current = current_publication is None or (
                request.version_period >= current_publication.version_period
            )

            supersedes_publication_id = (
                previous_publication.publication_id if previous_publication is not None else None
            )

            if previous_publication is not None:
                cursor.execute(
                    """
                        UPDATE catalog.dataset_publications
                        SET
                            is_active_revision = false,
                            is_current = false
                        WHERE publication_id = %s
                        """,
                    (previous_publication.publication_id,),
                )

            if is_current:
                cursor.execute(
                    """
                    UPDATE catalog.dataset_publications
                    SET is_current = false
                    WHERE dataset_id = %s
                      AND is_current = true
                    """,
                    (request.dataset_id,),
                )

            cursor.execute(
                f"""
                INSERT INTO catalog.dataset_publications (
                    publication_id,
                    pipeline_run_id,
                    dataset_id,
                    version_period,
                    partition_key,
                    partition_value,
                    revision,
                    silver_build_id,
                    engine_release_id,
                    processing_config_hash,
                    quality_config_hash,
                    fingerprint_version,
                    logical_hash_algorithm,
                    schema_hash_algorithm,
                    logical_data_hash,
                    schema_hash,
                    manifest_path,
                    published_at,
                    is_active_revision,
                    is_current,
                    supersedes_publication_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, true, %s, %s
                )
                RETURNING
                    {PUBLICATION_COLUMNS}
                """,
                (
                    request.publication_id,
                    request.pipeline_run_id,
                    request.dataset_id,
                    request.version_period,
                    request.partition_key,
                    request.partition_value,
                    revision,
                    request.silver_build_id,
                    request.engine_release_id,
                    request.processing_config_hash,
                    request.quality_config_hash,
                    request.fingerprint_version,
                    request.logical_hash_algorithm,
                    request.schema_hash_algorithm,
                    request.logical_data_hash,
                    request.schema_hash,
                    request.manifest_path,
                    request.published_at,
                    is_current,
                    supersedes_publication_id,
                ),
            )

            inserted_row = cursor.fetchone()

            if inserted_row is None:
                raise RuntimeError(
                    "PostgreSQL did not return the newly created dataset publication"
                )

        return _row_to_publication(inserted_row)

    def get_by_id(self, publication_id: str) -> DatasetPublication | None:
        """Return one publication by its identifier."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PUBLICATION_COLUMNS}
                FROM catalog.dataset_publications
                WHERE publication_id = %s
                """,
                (publication_id,),
            )

            row = cursor.fetchone()

        return _row_to_publication(row) if row is not None else None

    def find_current(self, dataset_id: str) -> DatasetPublication | None:
        """Return the current publication of a dataset."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PUBLICATION_COLUMNS}
                FROM catalog.dataset_publications
                WHERE dataset_id = %s
                  AND is_current = true
                LIMIT 1
                """,
                (dataset_id,),
            )

            row = cursor.fetchone()

        return _row_to_publication(row) if row is not None else None

    def find_active(self, *, dataset_id: str, partition_value: str) -> DatasetPublication | None:
        """Return the active revision of one dataset version."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PUBLICATION_COLUMNS}
                FROM catalog.dataset_publications
                WHERE dataset_id = %s
                  AND partition_value = %s
                  AND is_active_revision = true
                LIMIT 1
                """,
                (dataset_id, partition_value),
            )

            row = cursor.fetchone()

        return _row_to_publication(row) if row is not None else None

    def list_active(self, *, dataset_id: str) -> list[DatasetPublication]:
        """Return active revisions of all dataset versions."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PUBLICATION_COLUMNS}
                FROM catalog.dataset_publications
                WHERE dataset_id = %s
                  AND is_active_revision = true
                ORDER BY
                    version_period DESC,
                    revision DESC
                """,
                (dataset_id,),
            )

            rows = cursor.fetchall()

        return [_row_to_publication(row) for row in rows]

    def list_all(self, *, dataset_id: str) -> list[DatasetPublication]:
        """Return all publication revisions of a dataset."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PUBLICATION_COLUMNS}
                FROM catalog.dataset_publications
                WHERE dataset_id = %s
                ORDER BY
                    version_period DESC,
                    revision DESC
                """,
                (dataset_id,),
            )

            rows = cursor.fetchall()

        return [_row_to_publication(row) for row in rows]
