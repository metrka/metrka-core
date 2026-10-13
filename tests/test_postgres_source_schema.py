from __future__ import annotations

from unittest.mock import MagicMock

from metrka_core.metadata.postgres_source_schema import PostgresSourceSchemaStore
from metrka_core.metadata.source_schema import (
    SOURCE_SCHEMA_HASH_ALGORITHM,
    ParsedSourceSchema,
    SourceSchemaField,
    SourceSchemaFieldBinding,
)


def _parsed_schema() -> ParsedSourceSchema:
    return ParsedSourceSchema(
        source_schema_file_id="source-schema-file-1",
        dataset_id="example.dataset",
        source_format="xlsx",
        parser_name="example-parser",
        parser_version="1",
        fields=(
            SourceSchemaField(
                table_name="people",
                field_name="person_id",
                ordinal_position=1,
                source_type="integer",
                nullable=False,
            ),
        ),
    )


def _source_entry() -> MagicMock:
    entry = MagicMock()
    entry.file.artifact_role = "source_schema"
    entry.file.dataset_id = "example.dataset"
    return entry


def _session() -> tuple[MagicMock, MagicMock]:
    session = MagicMock()
    cursor = MagicMock()

    session.cursor.return_value.__enter__.return_value = cursor
    session.transaction.return_value.__enter__.return_value = None

    return session, cursor


def test_new_source_schema_uses_injected_snapshot_id() -> None:
    session, cursor = _session()
    cursor.fetchone.return_value = None

    file_marshal_store = MagicMock()
    file_marshal_store.get_marshaled_file.return_value = _source_entry()

    source_schema_ids = MagicMock()
    source_schema_ids.new_source_schema_snapshot_id.return_value = (
        "11111111-1111-4111-8111-111111111111"
    )

    store = PostgresSourceSchemaStore(
        session=session, file_marshal_store=file_marshal_store, source_schema_ids=source_schema_ids
    )

    snapshot_id = store.register_source_schema_snapshot(_parsed_schema())

    assert snapshot_id == ("11111111-1111-4111-8111-111111111111")

    source_schema_ids.new_source_schema_snapshot_id.assert_called_once_with()
    session.transaction.assert_called_once()
    cursor.executemany.assert_called_once()

    insert_call = cursor.execute.call_args_list[1]
    insert_parameters = insert_call.args[1]

    assert insert_parameters[0] == snapshot_id
    assert insert_parameters[3] == SOURCE_SCHEMA_HASH_ALGORITHM
    assert insert_parameters[4] == _parsed_schema().schema_hash
    assert insert_parameters[6] == SourceSchemaFieldBinding.BY_NAME.value

    lookup_call = cursor.execute.call_args_list[0]
    assert lookup_call.args[1] == (
        "source-schema-file-1",
        "example-parser",
        "1",
        SOURCE_SCHEMA_HASH_ALGORITHM,
    )


def test_existing_source_schema_does_not_generate_new_id() -> None:
    session, cursor = _session()
    cursor.fetchone.return_value = {
        "schema_snapshot_id": ("22222222-2222-4222-8222-222222222222"),
        "schema_hash_algorithm": SOURCE_SCHEMA_HASH_ALGORITHM,
        "schema_hash": _parsed_schema().schema_hash,
        "field_binding": SourceSchemaFieldBinding.BY_NAME.value,
    }

    file_marshal_store = MagicMock()
    file_marshal_store.get_marshaled_file.return_value = _source_entry()

    source_schema_ids = MagicMock()

    store = PostgresSourceSchemaStore(
        session=session, file_marshal_store=file_marshal_store, source_schema_ids=source_schema_ids
    )

    snapshot_id = store.register_source_schema_snapshot(_parsed_schema())

    assert snapshot_id == ("22222222-2222-4222-8222-222222222222")
    source_schema_ids.new_source_schema_snapshot_id.assert_not_called()
    session.transaction.assert_not_called()
    cursor.executemany.assert_not_called()
