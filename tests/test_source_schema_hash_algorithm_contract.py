from __future__ import annotations

from metrka_core.metadata.source_schema import (
    SOURCE_SCHEMA_HASH_ALGORITHM,
    ParsedSourceSchema,
    SourceSchemaField,
    SourceSchemaFieldBinding,
)


def _pinned_schema() -> ParsedSourceSchema:
    return ParsedSourceSchema(
        source_schema_file_id="source-schema-1",
        dataset_id="example.dataset",
        source_format="xlsx",
        parser_name="example-parser",
        parser_version="1",
        fields=(
            SourceSchemaField(
                table_name="people",
                field_name="café_name",
                ordinal_position=2,
                source_type="VARCHAR",
                source_length=100,
                nullable=True,
                attributes={"label": "Málaga", "code_page": "utf-8"},
            ),
            SourceSchemaField(
                table_name="people",
                field_name="person_id",
                ordinal_position=1,
                source_type="INTEGER",
                nullable=False,
            ),
        ),
    )


def test_source_schema_hash_is_pinned_for_binding_aware_fields_v1() -> None:
    parsed_schema = _pinned_schema()

    assert SOURCE_SCHEMA_HASH_ALGORITHM == ("metrka.source-schema.sha256.binding-aware-fields.v1")
    assert parsed_schema.schema_hash_algorithm == SOURCE_SCHEMA_HASH_ALGORITHM
    assert parsed_schema.field_binding is SourceSchemaFieldBinding.BY_NAME
    assert parsed_schema.schema_hash == (
        "82e9e7a960ac6b842f86d767b507a06c6bceb3d62c821dcabcfcf9dbcb02e8f9"
    )


def test_source_schema_hash_does_not_depend_on_input_field_order() -> None:
    parsed_schema = _pinned_schema()
    reordered_schema = ParsedSourceSchema(
        source_schema_file_id=parsed_schema.source_schema_file_id,
        dataset_id=parsed_schema.dataset_id,
        source_format=parsed_schema.source_format,
        parser_name=parsed_schema.parser_name,
        parser_version=parsed_schema.parser_version,
        fields=tuple(reversed(parsed_schema.fields)),
    )

    assert reordered_schema.schema_hash == parsed_schema.schema_hash
