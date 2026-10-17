"""PostgreSQL persistence for Silver build attempts and materializations."""

from __future__ import annotations

from collections.abc import Collection
from datetime import date, datetime
from typing import Any

from psycopg.types.json import Jsonb

from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.silver.build_models import (
    RebuildMode,
    RebuildReason,
    SilverBuild,
    SilverBuildStatus,
)
from metrka_core.storage.checksums import parse_sha256_hex

SILVER_BUILD_COLUMNS = """
    attempt.silver_build_id,
    attempt.pipeline_run_id,
    attempt.silver_run_id,
    attempt.dataset_file_id,
    attempt.dataset_id,
    attempt.version_period,
    attempt.partition_key,
    attempt.partition_value,
    attempt.contract_hash,
    attempt.engine_release_id,
    attempt.processing_config_hash,
    attempt.quality_config_hash,
    attempt.build_signature,
    attempt.status,
    attempt.rebuild_mode,
    attempt.rebuild_reasons,
    attempt.fingerprint_version,
    attempt.logical_hash_algorithm,
    attempt.schema_hash_algorithm,
    materialization.logical_data_hash,
    materialization.schema_hash,
    materialization.manifest_path,
    materialization.output_hash,
    materialization.output_file_count,
    materialization.output_byte_count,
    attempt.started_at,
    attempt.completed_at,
    attempt.error_code,
    attempt.error_message
"""

SILVER_BUILD_FROM = """
    FROM logs.silver_build_attempts AS attempt
    LEFT JOIN meta.silver_materializations AS materialization
      ON materialization.silver_build_id = attempt.silver_build_id
"""


def _row_to_silver_build(row: Any) -> SilverBuild:
    """Convert one joined PostgreSQL row into a SilverBuild aggregate."""

    record = dict(row)
    logical_data_hash = record.get("logical_data_hash")
    schema_hash = record.get("schema_hash")

    return SilverBuild(
        silver_build_id=str(record["silver_build_id"]),
        pipeline_run_id=str(record["pipeline_run_id"]),
        silver_run_id=str(record["silver_run_id"]),
        dataset_file_id=str(record["dataset_file_id"]),
        dataset_id=str(record["dataset_id"]),
        version_period=record.get("version_period"),
        partition_key=record.get("partition_key"),
        partition_value=record.get("partition_value"),
        contract_hash=str(record["contract_hash"]),
        engine_release_id=str(record["engine_release_id"]),
        processing_config_hash=str(record["processing_config_hash"]),
        quality_config_hash=str(record["quality_config_hash"]),
        build_signature=str(record["build_signature"]),
        fingerprint_version=int(record["fingerprint_version"]),
        logical_hash_algorithm=str(record["logical_hash_algorithm"]),
        schema_hash_algorithm=str(record["schema_hash_algorithm"]),
        logical_data_hash=(str(logical_data_hash) if logical_data_hash is not None else None),
        schema_hash=(str(schema_hash) if schema_hash is not None else None),
        status=SilverBuildStatus(record["status"]),
        rebuild_mode=RebuildMode(record["rebuild_mode"]),
        rebuild_reasons=tuple(
            RebuildReason(reason) for reason in (record.get("rebuild_reasons") or [])
        ),
        manifest_path=record.get("manifest_path"),
        output_hash=record.get("output_hash"),
        output_file_count=record.get("output_file_count"),
        output_byte_count=record.get("output_byte_count"),
        started_at=record["started_at"],
        completed_at=record.get("completed_at"),
        error_code=record.get("error_code"),
        error_message=record.get("error_message"),
    )


class PostgresSilverBuildStore:
    """Persist attempts in logs and successful outputs in metadata."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def insert_started(self, build: SilverBuild) -> str:
        """Insert one running attempt without materialization fields."""

        if build.status is not SilverBuildStatus.RUNNING:
            raise ValueError("A newly inserted Silver build must have running status")

        with self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO logs.silver_build_attempts (
                    silver_build_id,
                    pipeline_run_id,
                    silver_run_id,
                    dataset_file_id,
                    dataset_id,
                    version_period,
                    partition_key,
                    partition_value,
                    contract_hash,
                    engine_release_id,
                    processing_config_hash,
                    quality_config_hash,
                    build_signature,
                    fingerprint_version,
                    logical_hash_algorithm,
                    schema_hash_algorithm,
                    status,
                    rebuild_mode,
                    rebuild_reasons,
                    started_at,
                    completed_at,
                    error_code,
                    error_message
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    build.silver_build_id,
                    build.pipeline_run_id,
                    build.silver_run_id,
                    build.dataset_file_id,
                    build.dataset_id,
                    build.version_period,
                    build.partition_key,
                    build.partition_value,
                    build.contract_hash,
                    build.engine_release_id,
                    build.processing_config_hash,
                    build.quality_config_hash,
                    build.build_signature,
                    build.fingerprint_version,
                    build.logical_hash_algorithm,
                    build.schema_hash_algorithm,
                    build.status.value,
                    build.rebuild_mode.value,
                    Jsonb([reason.value for reason in build.rebuild_reasons]),
                    build.started_at,
                    build.completed_at,
                    build.error_code,
                    build.error_message,
                ),
            )

        return build.silver_build_id

    def get_by_id(self, silver_build_id: str) -> SilverBuild | None:
        """Return one attempt with its optional materialization."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.silver_build_id = %s
                """,
                (silver_build_id,),
            )
            row = cursor.fetchone()

        return _row_to_silver_build(row) if row is not None else None

    def find_by_ids(self, silver_build_ids: Collection[str]) -> dict[str, SilverBuild]:
        """Return requested build aggregates in one database query."""

        build_ids = sorted(set(silver_build_ids))
        if not build_ids:
            return {}

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.silver_build_id = ANY(%s::uuid[])
                """,
                (build_ids,),
            )
            rows = cursor.fetchall()

        builds = (_row_to_silver_build(row) for row in rows)
        return {build.silver_build_id: build for build in builds}

    def list_for_dataset(self, *, dataset_id: str) -> tuple[SilverBuild, ...]:
        """Return all build attempts belonging to one dataset."""

        if not dataset_id.strip():
            raise ValueError("dataset_id must not be empty")

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.dataset_id = %s
                ORDER BY attempt.started_at, attempt.silver_build_id
                """,
                (dataset_id,),
            )
            rows = cursor.fetchall()

        return tuple(_row_to_silver_build(row) for row in rows)

    def find_successful_by_signatures(
        self, build_signatures: Collection[str]
    ) -> dict[str, SilverBuild]:
        """Return the newest materialized build for every requested signature."""

        signatures = sorted(set(build_signatures))
        if not signatures:
            return {}

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT DISTINCT ON (attempt.build_signature)
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.build_signature = ANY(%s)
                  AND attempt.status = %s
                  AND materialization.silver_build_id IS NOT NULL
                ORDER BY
                    attempt.build_signature,
                    attempt.completed_at DESC
                """,
                (signatures, SilverBuildStatus.SUCCEEDED.value),
            )
            rows = cursor.fetchall()

        builds = (_row_to_silver_build(row) for row in rows)
        return {build.build_signature: build for build in builds}

    def find_latest_successful_for_version(
        self, *, dataset_id: str, partition_value: str
    ) -> SilverBuild | None:
        """Return the newest materialized build for one dataset version."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.dataset_id = %s
                  AND attempt.partition_value = %s
                  AND attempt.status = %s
                  AND materialization.silver_build_id IS NOT NULL
                ORDER BY attempt.completed_at DESC
                LIMIT 1
                """,
                (dataset_id, partition_value, SilverBuildStatus.SUCCEEDED.value),
            )
            row = cursor.fetchone()

        return _row_to_silver_build(row) if row is not None else None

    def find_latest_attempt_for_version(
        self, *, dataset_id: str, partition_value: str
    ) -> SilverBuild | None:
        """Return the newest attempt for one dataset version."""

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {SILVER_BUILD_COLUMNS}
                {SILVER_BUILD_FROM}
                WHERE attempt.dataset_id = %s
                  AND attempt.partition_value = %s
                ORDER BY attempt.started_at DESC
                LIMIT 1
                """,
                (dataset_id, partition_value),
            )
            row = cursor.fetchone()

        return _row_to_silver_build(row) if row is not None else None

    def mark_succeeded(
        self,
        *,
        silver_build_id: str,
        version_period: date | None,
        partition_key: str | None,
        partition_value: str | None,
        manifest_path: str,
        output_hash: str | None,
        output_file_count: int,
        output_byte_count: int,
        fingerprint_version: int,
        logical_hash_algorithm: str,
        schema_hash_algorithm: str,
        logical_data_hash: str,
        schema_hash: str,
        completed_at: datetime,
    ) -> SilverBuild:
        """Atomically complete an attempt and persist its materialization."""

        self._validate_success(
            manifest_path=manifest_path,
            output_hash=output_hash,
            output_file_count=output_file_count,
            output_byte_count=output_byte_count,
            fingerprint_version=fingerprint_version,
            logical_hash_algorithm=logical_hash_algorithm,
            schema_hash_algorithm=schema_hash_algorithm,
            logical_data_hash=logical_data_hash,
            schema_hash=schema_hash,
            completed_at=completed_at,
        )

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                WITH updated_attempt AS (
                    UPDATE logs.silver_build_attempts
                    SET
                        status = %s,
                        version_period = %s,
                        partition_key = %s,
                        partition_value = %s,
                        fingerprint_version = %s,
                        logical_hash_algorithm = %s,
                        schema_hash_algorithm = %s,
                        completed_at = %s,
                        error_code = NULL,
                        error_message = NULL
                    WHERE silver_build_id = %s
                      AND status = %s
                    RETURNING *
                ),
                inserted_materialization AS (
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
                    SELECT
                        updated_attempt.silver_build_id,
                        %s, %s, %s, %s, %s, %s, %s
                    FROM updated_attempt
                    RETURNING *
                )
                SELECT
                    {SILVER_BUILD_COLUMNS}
                FROM updated_attempt AS attempt
                JOIN inserted_materialization AS materialization
                  ON materialization.silver_build_id =
                     attempt.silver_build_id
                """,
                (
                    SilverBuildStatus.SUCCEEDED.value,
                    version_period,
                    partition_key,
                    partition_value,
                    fingerprint_version,
                    logical_hash_algorithm,
                    schema_hash_algorithm,
                    completed_at,
                    silver_build_id,
                    SilverBuildStatus.RUNNING.value,
                    manifest_path,
                    output_hash,
                    output_file_count,
                    output_byte_count,
                    logical_data_hash,
                    schema_hash,
                    completed_at,
                ),
            )
            row = cursor.fetchone()

        if row is None:
            raise ValueError(
                f"Silver build does not exist or is no longer running: {silver_build_id}"
            )

        return _row_to_silver_build(row)

    def mark_failed(
        self, *, silver_build_id: str, completed_at: datetime, error_code: str, error_message: str
    ) -> SilverBuild:
        """Mark a running attempt as failed without a materialization."""

        if completed_at.tzinfo is None or completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        if not error_code.strip():
            raise ValueError("error_code must not be empty")
        if not error_message.strip():
            raise ValueError("error_message must not be empty")

        with self._session.cursor() as cursor:
            cursor.execute(
                """
                UPDATE logs.silver_build_attempts
                SET
                    status = %s,
                    completed_at = %s,
                    error_code = %s,
                    error_message = %s
                WHERE silver_build_id = %s
                  AND status = %s
                RETURNING silver_build_id
                """,
                (
                    SilverBuildStatus.FAILED.value,
                    completed_at,
                    error_code,
                    error_message,
                    silver_build_id,
                    SilverBuildStatus.RUNNING.value,
                ),
            )
            row = cursor.fetchone()

        if row is None:
            raise ValueError(
                f"Silver build does not exist or is no longer running: {silver_build_id}"
            )

        failed = self.get_by_id(silver_build_id)
        if failed is None:
            raise RuntimeError(f"Failed Silver build disappeared after update: {silver_build_id}")
        return failed

    @staticmethod
    def _validate_success(
        *,
        manifest_path: str,
        output_hash: str | None,
        output_file_count: int,
        output_byte_count: int,
        fingerprint_version: int,
        logical_hash_algorithm: str,
        schema_hash_algorithm: str,
        logical_data_hash: str,
        schema_hash: str,
        completed_at: datetime,
    ) -> None:
        if completed_at.tzinfo is None or completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        if not manifest_path.strip():
            raise ValueError("manifest_path must not be empty")
        if output_hash is not None:
            try:
                parse_sha256_hex(output_hash)
            except ValueError as error:
                raise ValueError("output_hash must be a SHA-256 hash") from error
        if output_file_count < 1:
            raise ValueError("A successful Silver build must produce at least one file")
        if output_byte_count < 0:
            raise ValueError("output_byte_count must not be negative")
        if fingerprint_version < 1:
            raise ValueError("fingerprint_version must be positive")
        if not logical_hash_algorithm.strip():
            raise ValueError("logical_hash_algorithm must not be empty")
        if not schema_hash_algorithm.strip():
            raise ValueError("schema_hash_algorithm must not be empty")

        for field_name, value in {
            "logical_data_hash": logical_data_hash,
            "schema_hash": schema_hash,
        }.items():
            if len(value) != 64:
                raise ValueError(f"{field_name} must be a SHA-256 hash")
