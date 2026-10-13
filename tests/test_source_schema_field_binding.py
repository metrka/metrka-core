from __future__ import annotations

from dataclasses import replace

import pytest

from metrka_core.metadata.source_schema import (
    ParsedSourceSchema,
    SourceSchemaField,
    SourceSchemaFieldBinding,
    compare_source_schema_fields,
)


def _field(*, name: str, position: int) -> SourceSchemaField:
    return SourceSchemaField(
        table_name="people",
        field_name=name,
        ordinal_position=position,
        source_type="string",
        nullable=False,
    )


def _reordered_fields() -> tuple[tuple[SourceSchemaField, ...], tuple[SourceSchemaField, ...]]:
    previous = (_field(name="person_id", position=1), _field(name="person_name", position=2))
    current = (_field(name="person_id", position=2), _field(name="person_name", position=1))
    return previous, current


def test_reordering_named_fields_is_non_breaking() -> None:
    previous, current = _reordered_fields()

    changes = compare_source_schema_fields(
        previous, current, field_binding=SourceSchemaFieldBinding.BY_NAME
    )

    assert len(changes) == 2
    assert {change.impact for change in changes} == {"non_breaking"}
    assert all("ordinal_position" in change.details for change in changes)


def test_reordering_positional_fields_is_breaking() -> None:
    previous, current = _reordered_fields()

    changes = compare_source_schema_fields(
        previous, current, field_binding=SourceSchemaFieldBinding.BY_POSITION
    )

    assert len(changes) == 2
    assert {change.impact for change in changes} == {"breaking"}


def test_field_binding_is_part_of_source_schema_hash() -> None:
    parsed_schema = ParsedSourceSchema(
        source_schema_file_id="source-schema-1",
        dataset_id="example.dataset",
        source_format="csv",
        parser_name="example-parser",
        parser_version="1",
        fields=(_field(name="person_id", position=1),),
    )
    positional_schema = replace(parsed_schema, field_binding=SourceSchemaFieldBinding.BY_POSITION)

    assert parsed_schema.schema_hash != positional_schema.schema_hash


def test_comparison_rejects_untyped_field_binding() -> None:
    previous, current = _reordered_fields()

    with pytest.raises(TypeError, match="field_binding must be a SourceSchemaFieldBinding"):
        compare_source_schema_fields(
            previous,
            current,
            field_binding="by_position",  # type: ignore[arg-type]
        )
