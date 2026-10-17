"""PostgreSQL persistence for source-schema metadata."""

from __future__ import annotations

import json
from typing import Any

from metrka_core.metadata.postgres import PostgresSession, to_jsonb
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.metadata.source_schema import ParsedSourceSchema, SourceSchemaField
from metrka_core.pipeline.runtime_services import SourceSchemaSnapshotIdGenerator


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)

    return value


class PostgresSourceSchemaStore:
    """PostgreSQL implementation of SourceSchemaStore."""

    def __init__(
        self,
        session: PostgresSession,
        file_marshal_store: PostgresFileMarshalStore,
        source_schema_ids: SourceSchemaSnapshotIdGenerator,
    ) -> None:
        self._session = session
        self._file_marshal_store = file_marshal_store
        self._source_schema_ids = source_schema_ids

    # ========================================================
    # Source schema catalog
    # ========================================================

    def get_source_schema_snapshot_for_file(
        self,
        source_schema_file_id: str,
        parser_name: str,
        parser_version: str,
        schema_hash_algorithm: str,
    ) -> dict[str, Any] | None:
        """Find an existing parse of one source-schema file."""

        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT
                    schema_snapshot_id,
                    source_schema_file_id,
                    dataset_id,
                    schema_hash_algorithm,
                    schema_hash,
                    source_format,
                    field_binding,
                    parser_name,
                    parser_version,
                    table_count,
                    field_count,
                    parsed_at,
                    meta
                FROM meta.source_schema_snapshots
                WHERE source_schema_file_id = %s
                  AND parser_name = %s
                  AND parser_version = %s
                  AND schema_hash_algorithm = %s
                LIMIT 1
                """,
                (source_schema_file_id, parser_name, parser_version, schema_hash_algorithm),
            )

            record = cur.fetchone()
            return dict(record) if record else None

    def get_source_schema_fields(self, schema_snapshot_id: str) -> list[SourceSchemaField]:
        """Return normalized fields belonging to one schema snapshot."""

        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT
                    table_name,
                    field_name,
                    ordinal_position,
                    source_type,
                    source_length,
                    nullable,
                    attributes
                FROM meta.source_schema_fields
                WHERE schema_snapshot_id = %s
                ORDER BY table_name, ordinal_position
                """,
                (schema_snapshot_id,),
            )

            return [
                SourceSchemaField(
                    table_name=record["table_name"],
                    field_name=record["field_name"],
                    ordinal_position=record["ordinal_position"],
                    source_type=record["source_type"],
                    source_length=record["source_length"],
                    nullable=record["nullable"],
                    attributes=_decode_json(record["attributes"]) or {},
                )
                for record in cur.fetchall()
            ]

    def register_source_schema_snapshot(self, parsed_schema: ParsedSourceSchema) -> str:
        """Store one schema snapshot and all its fields atomically."""

        source_entry = self._file_marshal_store.get_marshaled_file(
            parsed_schema.source_schema_file_id
        )

        if source_entry is None:
            raise ValueError(
                "Source schema file is not registered in FileMarshal: "
                f"{parsed_schema.source_schema_file_id}"
            )

        if source_entry.file.artifact_role != "source_schema":
            raise ValueError(
                "FileMarshal entry is not a source_schema artifact: "
                f"{parsed_schema.source_schema_file_id}"
            )

        if source_entry.file.dataset_id != parsed_schema.dataset_id:
            raise ValueError("Parsed schema dataset_id does not match its FileMarshal entry")

        existing = self.get_source_schema_snapshot_for_file(
            source_schema_file_id=parsed_schema.source_schema_file_id,
            parser_name=parsed_schema.parser_name,
            parser_version=parsed_schema.parser_version,
            schema_hash_algorithm=parsed_schema.schema_hash_algorithm,
        )

        if existing is not None:
            if existing["schema_hash"] != parsed_schema.schema_hash:
                raise RuntimeError(
                    "The same file and parser version produced a different "
                    "schema hash. Increase parser_version."
                )

            return str(existing["schema_snapshot_id"])

        schema_snapshot_id = self._source_schema_ids.new_source_schema_snapshot_id()

        if not schema_snapshot_id.strip():
            raise ValueError("SourceSchemaSnapshotIdGenerator returned an empty snapshot ID")

        with self._session.transaction(), self._session.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO meta.source_schema_snapshots (
                        schema_snapshot_id,
                        source_schema_file_id,
                        dataset_id,
                        schema_hash_algorithm,
                        schema_hash,
                        source_format,
                        field_binding,
                        parser_name,
                        parser_version,
                        table_count,
                        field_count,
                        meta
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                (
                    schema_snapshot_id,
                    parsed_schema.source_schema_file_id,
                    parsed_schema.dataset_id,
                    parsed_schema.schema_hash_algorithm,
                    parsed_schema.schema_hash,
                    parsed_schema.source_format,
                    parsed_schema.field_binding.value,
                    parsed_schema.parser_name,
                    parsed_schema.parser_version,
                    parsed_schema.table_count,
                    parsed_schema.field_count,
                    to_jsonb(parsed_schema.meta),
                ),
            )

            cur.executemany(
                """
                    INSERT INTO meta.source_schema_fields (
                        schema_snapshot_id,
                        table_name,
                        field_name,
                        ordinal_position,
                        source_type,
                        source_length,
                        nullable,
                        attributes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                [
                    (
                        schema_snapshot_id,
                        schema_field.table_name,
                        schema_field.field_name,
                        schema_field.ordinal_position,
                        schema_field.source_type,
                        schema_field.source_length,
                        schema_field.nullable,
                        to_jsonb(schema_field.attributes),
                    )
                    for schema_field in parsed_schema.fields
                ],
            )

        return schema_snapshot_id

    def bind_source_schema_snapshot(
        self, schema_snapshot_id: str, data_file_id: str, meta: dict[str, Any] | None = None
    ) -> None:
        """Bind one parsed source schema snapshot to one data file."""

        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM meta.source_schema_snapshots
                WHERE schema_snapshot_id = %s
                LIMIT 1
                """,
                (schema_snapshot_id,),
            )

            if cur.fetchone() is None:
                raise ValueError(f"Unknown source schema snapshot: {schema_snapshot_id}")

        data_entry = self._file_marshal_store.get_marshaled_file(data_file_id)

        if data_entry is None:
            raise ValueError(f"Data file is not registered in FileMarshal: {data_file_id}")

        if data_entry.file.artifact_role != "data":
            raise ValueError(f"FileMarshal entry is not a data artifact: {data_file_id}")

        with self._session.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.source_schema_bindings (
                    schema_snapshot_id,
                    data_file_id,
                    meta
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (schema_snapshot_id, data_file_id)
                DO UPDATE SET meta = EXCLUDED.meta
                """,
                (schema_snapshot_id, data_file_id, to_jsonb(meta or {})),
            )

    def get_source_schema_snapshot(self, schema_snapshot_id: str) -> dict[str, Any] | None:
        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT
                    schema_snapshot_id,
                    source_schema_file_id,
                    dataset_id,
                    schema_hash_algorithm,
                    schema_hash,
                    source_format,
                    field_binding,
                    parser_name,
                    parser_version,
                    table_count,
                    field_count,
                    parsed_at,
                    meta
                FROM meta.source_schema_snapshots
                WHERE schema_snapshot_id = %s
                LIMIT 1
                """,
                (schema_snapshot_id,),
            )

            record = cur.fetchone()
            return dict(record) if record else None

    def get_previous_source_schema_snapshot(self, schema_snapshot_id: str) -> dict[str, Any] | None:
        current = self.get_source_schema_snapshot(schema_snapshot_id)

        if current is None:
            raise ValueError(f"Unknown source schema snapshot: {schema_snapshot_id}")

        with self._session.cursor() as cur:
            cur.execute(
                """
                SELECT
                    schema_snapshot_id,
                    source_schema_file_id,
                    dataset_id,
                    schema_hash_algorithm,
                    schema_hash,
                    source_format,
                    field_binding,
                    parser_name,
                    parser_version,
                    table_count,
                    field_count,
                    parsed_at,
                    meta
                FROM meta.source_schema_snapshots
                WHERE dataset_id = %s
                  AND parser_name = %s
                  AND schema_snapshot_id <> %s
                  AND parsed_at <= %s
                ORDER BY parsed_at DESC, schema_snapshot_id DESC
                LIMIT 1
                """,
                (
                    current["dataset_id"],
                    current["parser_name"],
                    schema_snapshot_id,
                    current["parsed_at"],
                ),
            )

            record = cur.fetchone()
            return dict(record) if record else None
