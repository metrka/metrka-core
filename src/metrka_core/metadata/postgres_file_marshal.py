"""PostgreSQL persistence for source-file lifecycle metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from metrka_core.metadata.artifact import ArtifactRole
from metrka_core.metadata.file_marshal_models import (
    BronzeArtifactDigest,
    MarshaledFile,
    MarshalEntry,
    MarshalEvent,
    SilverCandidateFile,
)
from metrka_core.metadata.postgres import PostgresSession, to_jsonb

MARSHAL_FILE_COLUMNS = """
    f.dataset_file_id,
    f.dataset_id,
    f.source_url,
    f.source_file_name,
    f.original_source_file_name,
    f.artifact_role,
    f.source_hash,
    f.file_size,
    f.ingestion_timestamp,
    f.source_last_modified,
    f.row_count_raw,
    f.column_count_raw,
    f.bronze_run_id,
    f.bronze_artifacts,
    f.silver_run_id,
    f.landing_path,
    f.manifest_path,
    f.partition_key,
    f.partition_value,
    f.is_promoted,
    f.version_period,
    f.promoted_at,
    f.superseded_by_file_id
"""


def _record_mapping(record: object, *, record_name: str) -> Mapping[str, object]:
    if not isinstance(record, Mapping):
        raise TypeError(f"{record_name} must be a mapping")

    normalized: dict[str, object] = {}

    for key, value in record.items():
        if not isinstance(key, str):
            raise TypeError(f"{record_name} contains a non-string column name")

        normalized[key] = value

    return normalized


def _column_value(row: Mapping[str, object], field_name: str) -> object:
    if field_name not in row:
        raise ValueError(f"PostgreSQL record is missing column {field_name!r}")

    return row[field_name]


def _required_text(row: Mapping[str, object], field_name: str) -> str:
    value = _column_value(row, field_name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"PostgreSQL column {field_name!r} must be a non-empty string")

    return value


def _optional_text(row: Mapping[str, object], field_name: str) -> str | None:
    value = _column_value(row, field_name)

    if value is None:
        return None

    if not isinstance(value, str):
        raise TypeError(f"PostgreSQL column {field_name!r} must be text or NULL")

    return value


def _required_int(row: Mapping[str, object], field_name: str) -> int:
    value = _column_value(row, field_name)

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"PostgreSQL column {field_name!r} must be an integer")

    return value


def _required_bool(row: Mapping[str, object], field_name: str) -> bool:
    value = _column_value(row, field_name)

    if not isinstance(value, bool):
        raise TypeError(f"PostgreSQL column {field_name!r} must be boolean")

    return value


def _required_datetime(row: Mapping[str, object], field_name: str) -> datetime:
    value = _column_value(row, field_name)

    if not isinstance(value, datetime):
        raise TypeError(f"PostgreSQL column {field_name!r} must be a datetime")

    return value


def _optional_datetime(row: Mapping[str, object], field_name: str) -> datetime | None:
    value = _column_value(row, field_name)

    if value is None:
        return None

    if not isinstance(value, datetime):
        raise TypeError(f"PostgreSQL column {field_name!r} must be a datetime or NULL")

    return value


def _optional_date(row: Mapping[str, object], field_name: str) -> date | None:
    value = _column_value(row, field_name)

    if value is None:
        return None

    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"PostgreSQL column {field_name!r} must be a date or NULL")

    return value


def _required_artifact_role(row: Mapping[str, object]) -> ArtifactRole:
    value = _required_text(row, "artifact_role")

    if value == "data":
        return "data"

    if value == "source_schema":
        return "source_schema"

    if value == "documentation":
        return "documentation"

    raise ValueError(f"Invalid artifact_role stored in PostgreSQL: {value!r}")


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"PostgreSQL JSONB field {field_name!r} must be a decoded object")

    result: dict[str, Any] = {}

    for key, item in value.items():
        if not isinstance(key, str):
            raise TypeError(f"PostgreSQL JSONB field {field_name!r} contains a non-string key")

        result[key] = item

    return result


def _bronze_artifacts(value: object) -> tuple[BronzeArtifactDigest, ...]:
    if not isinstance(value, list):
        raise TypeError("PostgreSQL column 'bronze_artifacts' must be a JSON array")

    artifacts: list[BronzeArtifactDigest] = []

    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise TypeError(f"bronze_artifacts[{index}] must be a JSON object")

        relative_path = item.get("relative_path")
        sha256 = item.get("sha256")
        size_bytes = item.get("size_bytes")

        if not isinstance(relative_path, str):
            raise TypeError(f"bronze_artifacts[{index}].relative_path must be text")

        if not isinstance(sha256, str):
            raise TypeError(f"bronze_artifacts[{index}].sha256 must be text")

        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError(f"bronze_artifacts[{index}].size_bytes must be an integer")

        artifacts.append(
            BronzeArtifactDigest(relative_path=relative_path, sha256=sha256, size_bytes=size_bytes)
        )

    ordered = tuple(sorted(artifacts, key=lambda artifact: artifact.relative_path))
    paths = [artifact.relative_path for artifact in ordered]

    if len(paths) != len(set(paths)):
        raise ValueError("PostgreSQL bronze_artifacts contains duplicate relative paths")

    return ordered


def _marshal_entry_from_record(record: object) -> MarshalEntry:
    if isinstance(record, MarshalEntry):
        return record

    row = _record_mapping(record, record_name="File Marshal record")

    file = MarshaledFile(
        dataset_file_id=_required_text(row, "dataset_file_id"),
        dataset_id=_required_text(row, "dataset_id"),
        source_url=_required_text(row, "source_url"),
        source_file_name=_required_text(row, "source_file_name"),
        original_source_file_name=_required_text(row, "original_source_file_name"),
        artifact_role=_required_artifact_role(row),
        source_hash=_required_text(row, "source_hash"),
        file_size=_required_int(row, "file_size"),
        ingestion_timestamp=_required_datetime(row, "ingestion_timestamp"),
        source_last_modified=_optional_datetime(row, "source_last_modified"),
        row_count_raw=_required_int(row, "row_count_raw"),
        column_count_raw=_required_int(row, "column_count_raw"),
    )

    return MarshalEntry(
        file=file,
        bronze_run_id=_optional_text(row, "bronze_run_id"),
        bronze_artifacts=_bronze_artifacts(_column_value(row, "bronze_artifacts")),
        silver_run_id=_optional_text(row, "silver_run_id"),
        landing_path=_optional_text(row, "landing_path"),
        manifest_path=_optional_text(row, "manifest_path"),
        partition_key=_optional_text(row, "partition_key"),
        partition_value=_optional_text(row, "partition_value"),
        is_promoted=_required_bool(row, "is_promoted"),
        version_period=_optional_date(row, "version_period"),
        superseded_by_file_id=_optional_text(row, "superseded_by_file_id"),
        promoted_at=_optional_datetime(row, "promoted_at"),
    )


def _silver_candidate_from_record(record: object) -> SilverCandidateFile:
    row = _record_mapping(record, record_name="Silver candidate record")

    return SilverCandidateFile(
        dataset_file_id=_required_text(row, "dataset_file_id"),
        dataset_id=_required_text(row, "dataset_id"),
        bronze_run_id=_required_text(row, "bronze_run_id"),
    )


class PostgresFileMarshalStore:
    """PostgreSQL implementation of PostgresFileMarshalStore."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def transaction(self) -> Any:
        return self._session.transaction()

    # ========================================================
    # FileMarshal Inserts / Upserts
    # ========================================================
    def upsert_marshaled_file(self, entry: MarshalEntry) -> None:
        """Insert or update a file lifecycle state row."""
        f = entry.file
        superseded = entry.superseded_by_file_id if entry.superseded_by_file_id else None

        with self._session.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.marshaled_files (
                    dataset_file_id,
                    dataset_id,
                    source_url,
                    source_file_name,
                    original_source_file_name,
                    artifact_role,
                    source_hash,
                    file_size,
                    ingestion_timestamp,
                    source_last_modified,
                    row_count_raw,
                    column_count_raw,
                    bronze_run_id,
                    bronze_artifacts,
                    silver_run_id,
                    landing_path,
                    manifest_path,
                    partition_key,
                    partition_value,
                    is_promoted,
                    version_period,
                    promoted_at,
                    superseded_by_file_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (dataset_file_id) DO UPDATE SET
                    dataset_id = EXCLUDED.dataset_id,
                    source_url = EXCLUDED.source_url,
                    source_file_name = EXCLUDED.source_file_name,
                    original_source_file_name = EXCLUDED.original_source_file_name,
                    artifact_role = EXCLUDED.artifact_role,
                    source_hash = EXCLUDED.source_hash,
                    file_size = EXCLUDED.file_size,
                    ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                    source_last_modified = EXCLUDED.source_last_modified,
                    row_count_raw = EXCLUDED.row_count_raw,
                    column_count_raw = EXCLUDED.column_count_raw,
                    bronze_run_id = EXCLUDED.bronze_run_id,
                    bronze_artifacts = EXCLUDED.bronze_artifacts,
                    silver_run_id = EXCLUDED.silver_run_id,
                    landing_path = EXCLUDED.landing_path,
                    manifest_path = EXCLUDED.manifest_path,
                    partition_key = EXCLUDED.partition_key,
                    partition_value = EXCLUDED.partition_value,
                    is_promoted = EXCLUDED.is_promoted,
                    version_period = EXCLUDED.version_period,
                    promoted_at = EXCLUDED.promoted_at,
                    superseded_by_file_id = EXCLUDED.superseded_by_file_id
                """,
                (
                    f.dataset_file_id,
                    f.dataset_id,
                    f.source_url,
                    f.source_file_name,
                    f.original_source_file_name,
                    f.artifact_role,
                    f.source_hash,
                    f.file_size,
                    f.ingestion_timestamp,
                    f.source_last_modified,
                    f.row_count_raw,
                    f.column_count_raw,
                    entry.bronze_run_id,
                    to_jsonb(
                        [
                            {
                                "relative_path": artifact.relative_path,
                                "sha256": artifact.sha256,
                                "size_bytes": artifact.size_bytes,
                            }
                            for artifact in entry.bronze_artifacts
                        ]
                    ),
                    entry.silver_run_id,
                    entry.landing_path,
                    entry.manifest_path,
                    entry.partition_key,
                    entry.partition_value,
                    entry.is_promoted,
                    entry.version_period,
                    entry.promoted_at,
                    superseded,
                ),
            )

    def insert_marshal_event(self, event: MarshalEvent) -> None:
        """Insert a file lifecycle audit event."""
        old_entry = json.loads(json.dumps(event.old, default=str)) if event.old else None
        new_entry = json.loads(json.dumps(event.new, default=str))

        with self._session.cursor() as cur:
            cur.execute(
                """
                INSERT INTO logs.marshal_events (
                    event_ts,
                    event_type,
                    file_id,
                    reason,
                    stage,
                    bronze_run_id,
                    silver_run_id,
                    manifest_path,
                    landing_path,
                    partition_value,
                    old_entry,
                    new_entry,
                    meta
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.event_ts,
                    event.event_type,
                    event.file_id,
                    event.reason,
                    event.meta.get("stage"),
                    event.meta.get("bronze_run_id"),
                    event.meta.get("silver_run_id"),
                    event.meta.get("manifest_path"),
                    event.meta.get("landing_path"),
                    event.meta.get("partition_value"),
                    to_jsonb(old_entry),
                    to_jsonb(new_entry),
                    to_jsonb(event.meta),
                ),
            )

    def get_promoted_fingerprint(self, dataset_id: str) -> dict[str, Any] | None:
        """
        Return the current promoted file payload fingerprint for a dataset.
        """
        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT e.meta -> 'payload_fingerprint' AS fingerprint
                FROM meta.marshaled_files f
                JOIN logs.marshal_events e ON f.dataset_file_id = e.file_id
                WHERE f.dataset_id = %s
                  AND f.is_promoted = TRUE
                  AND e.reason = 'register'
                ORDER BY e.event_ts DESC
                LIMIT 1
                """,
                (dataset_id,),
            )
            result = cur.fetchone()

        if result is None:
            return None

        row = _record_mapping(result, record_name="Promoted fingerprint record")
        fingerprint = _column_value(row, "fingerprint")

        if fingerprint is None:
            return None

        return _json_object(fingerprint, field_name="fingerprint")

    def check_hash_exists(self, dataset_id: str, source_hash: str) -> bool:
        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM meta.marshaled_files
                WHERE dataset_id = %s AND source_hash = %s
                LIMIT 1
                """,
                (dataset_id, source_hash),
            )
            return cur.fetchone() is not None

    def get_marshaled_file_by_hash(self, dataset_id: str, source_hash: str) -> MarshalEntry | None:
        query = f"""
            SELECT
                {MARSHAL_FILE_COLUMNS}
            FROM meta.marshaled_files f
            WHERE f.dataset_id = %s
            AND f.source_hash = %s
            ORDER BY f.ingestion_timestamp DESC
            LIMIT 1
        """

        with self._session.cursor() as cur:
            cur.execute(query, (dataset_id, source_hash))
            record = cur.fetchone()
            return _marshal_entry_from_record(record) if record else None

    def get_marshaled_file(self, dataset_file_id: str) -> MarshalEntry | None:
        """Return one persisted FileMarshal entry by file ID."""
        query = f"""
            SELECT
                {MARSHAL_FILE_COLUMNS}
            FROM meta.marshaled_files f
            WHERE f.dataset_file_id = %s
        """

        with self._session.cursor() as cur:
            cur.execute(query, (dataset_file_id,))
            record = cur.fetchone()
            return _marshal_entry_from_record(record) if record else None

    def get_promoted_for_version_period(
        self, dataset_id: str, version_period: date
    ) -> MarshalEntry | None:
        """Return the promoted file for one dataset version period."""

        query = f"""
            SELECT
                {MARSHAL_FILE_COLUMNS}
            FROM meta.marshaled_files f
            WHERE f.dataset_id = %s
            AND f.version_period = %s
            AND f.is_promoted = TRUE
            ORDER BY
                f.promoted_at DESC NULLS LAST,
                f.ingestion_timestamp DESC
            LIMIT 1
            FOR UPDATE
        """

        with self._session.cursor() as cur:
            cur.execute(query, (dataset_id, version_period))
            record = cur.fetchone()

            return _marshal_entry_from_record(record) if record else None

    def get_silver_candidate_files(
        self, *, dataset_id: str | None = None
    ) -> tuple[SilverCandidateFile, ...]:
        """
        Return active Bronze data assets that may require a Silver build.

        This includes both newly registered files ready processed files.
        The Silver build registry decides whether each candidate is rebuilt
        or skipped.
        """

        query = """
            SELECT
                f.dataset_file_id,
                f.dataset_id,
                f.bronze_run_id
            FROM meta.marshaled_files f
            WHERE f.bronze_run_id IS NOT NULL
              AND jsonb_array_length(f.bronze_artifacts) > 0
              AND f.artifact_role = 'data'
              AND (
                    f.is_promoted IS TRUE
                    OR f.superseded_by_file_id IS NULL
              )
        """

        params: list[Any] = []

        if dataset_id is not None:
            query += " AND f.dataset_id = %s"
            params.append(dataset_id)

        query += """
            ORDER BY
                f.ingestion_timestamp ASC,
                f.dataset_file_id ASC
        """

        with self._session.cursor() as cursor:
            cursor.execute(query, params)
            return tuple(_silver_candidate_from_record(record) for record in cursor.fetchall())
