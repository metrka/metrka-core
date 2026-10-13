"""Persistence contract for source-schema metadata."""

from __future__ import annotations

from typing import Any, Protocol

from metrka_core.metadata.source_schema import ParsedSourceSchema, SourceSchemaField


class SourceSchemaStore(Protocol):
    """Persist and query parsed source-schema snapshots."""

    def get_source_schema_snapshot_for_file(
        self,
        source_schema_file_id: str,
        parser_name: str,
        parser_version: str,
        schema_hash_algorithm: str,
    ) -> dict[str, Any] | None: ...

    def get_source_schema_fields(self, schema_snapshot_id: str) -> list[SourceSchemaField]: ...

    def register_source_schema_snapshot(self, parsed_schema: ParsedSourceSchema) -> str: ...

    def bind_source_schema_snapshot(
        self, schema_snapshot_id: str, data_file_id: str, meta: dict[str, Any] | None = None
    ) -> None: ...

    def get_source_schema_snapshot(self, schema_snapshot_id: str) -> dict[str, Any] | None: ...

    def get_previous_source_schema_snapshot(
        self, schema_snapshot_id: str
    ) -> dict[str, Any] | None: ...
