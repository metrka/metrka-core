"""Normalized source-schema models, hashes, and compatibility comparisons.

Source-specific parsers construct :class:`ParsedSourceSchema` values from
external schema artifacts. Metrka hashes their canonical representation with a
versioned algorithm and compares successive field sets. Supported extension
contracts are re-exported from :mod:`metrka_core.extensions.source_schema`;
other names in this implementation module remain internal.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

SOURCE_SCHEMA_HASH_ALGORITHM = "metrka.source-schema.sha256.binding-aware-fields.v1"


class SourceSchemaFieldBinding(StrEnum):
    """How source records associate values with declared fields."""

    BY_NAME = "by_name"
    BY_POSITION = "by_position"


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


@dataclass(frozen=True)
class SourceSchemaField:
    """One normalized field declared by a source schema."""

    table_name: str
    field_name: str
    ordinal_position: int
    source_type: str
    source_length: int | None = None
    nullable: bool | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.table_name, "table_name")
        _require_text(self.field_name, "field_name")
        _require_text(self.source_type, "source_type")

        if self.ordinal_position <= 0:
            raise ValueError("ordinal_position must be greater than zero")

        if self.source_length is not None and self.source_length <= 0:
            raise ValueError("source_length must be greater than zero")

    def canonical_dict(self) -> dict[str, Any]:
        """Return the structural representation used for hashing."""

        return {
            "table_name": self.table_name,
            "field_name": self.field_name,
            "ordinal_position": self.ordinal_position,
            "source_type": self.source_type,
            "source_length": self.source_length,
            "nullable": self.nullable,
            "attributes": self.attributes,
        }


@dataclass(frozen=True)
class ParsedSourceSchema:
    """Normalized result produced by a source-specific schema parser."""

    source_schema_file_id: str
    dataset_id: str
    source_format: str
    parser_name: str
    parser_version: str
    fields: tuple[SourceSchemaField, ...]
    field_binding: SourceSchemaFieldBinding = SourceSchemaFieldBinding.BY_NAME
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.source_schema_file_id, "source_schema_file_id")
        _require_text(self.dataset_id, "dataset_id")
        _require_text(self.source_format, "source_format")
        _require_text(self.parser_name, "parser_name")
        _require_text(self.parser_version, "parser_version")

        if not isinstance(self.field_binding, SourceSchemaFieldBinding):
            raise TypeError("field_binding must be a SourceSchemaFieldBinding")

        if not self.fields:
            raise ValueError("Parsed source schema must contain at least one field")

        identities: set[tuple[str, str]] = set()
        ordinals: set[tuple[str, int]] = set()

        for schema_field in self.fields:
            identity = (schema_field.table_name, schema_field.field_name)
            ordinal = (schema_field.table_name, schema_field.ordinal_position)

            if identity in identities:
                raise ValueError(
                    "Duplicate source schema field: "
                    f"{schema_field.table_name}.{schema_field.field_name}"
                )

            if ordinal in ordinals:
                raise ValueError(
                    "Duplicate ordinal position: "
                    f"{schema_field.table_name} "
                    f"position {schema_field.ordinal_position}"
                )

            identities.add(identity)
            ordinals.add(ordinal)

    @property
    def table_count(self) -> int:
        """Return the number of distinct source tables."""

        return len({schema_field.table_name for schema_field in self.fields})

    @property
    def field_count(self) -> int:
        """Return the total number of normalized source fields."""

        return len(self.fields)

    @property
    def schema_hash_algorithm(self) -> str:
        """Return the identifier of the canonical source-schema hash algorithm."""

        return SOURCE_SCHEMA_HASH_ALGORITHM

    @property
    def schema_hash(self) -> str:
        """Return the deterministic hash of binding mode and canonical fields."""

        ordered_fields = sorted(
            self.fields,
            key=lambda schema_field: (
                schema_field.table_name,
                schema_field.ordinal_position,
                schema_field.field_name,
            ),
        )

        payload = {
            "algorithm": SOURCE_SCHEMA_HASH_ALGORITHM,
            "field_binding": self.field_binding.value,
            "fields": [schema_field.canonical_dict() for schema_field in ordered_fields],
        }

        canonical_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


SourceSchemaChangeKind = Literal[
    "table_added", "table_removed", "field_added", "field_removed", "field_changed"
]

SourceSchemaChangeImpact = Literal["breaking", "non_breaking", "informational"]


@dataclass(frozen=True)
class SourceSchemaChange:
    """One classified difference between successive normalized source schemas."""

    kind: SourceSchemaChangeKind
    table_name: str
    field_name: str | None = None
    impact: SourceSchemaChangeImpact = "informational"
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    details: dict[str, Any] = field(default_factory=dict)


def compare_source_schema_fields(
    previous_fields: Sequence[SourceSchemaField],
    current_fields: Sequence[SourceSchemaField],
    *,
    field_binding: SourceSchemaFieldBinding = SourceSchemaFieldBinding.BY_NAME,
) -> tuple[SourceSchemaChange, ...]:
    """Compare normalized fields and return deterministic classified changes.

    Added tables and fields are non-breaking; removals are breaking. Type
    changes, length reductions, and nullable-to-required changes are breaking.
    Ordinal movement is breaking for ``BY_POSITION`` sources and non-breaking
    for ``BY_NAME`` sources. Other recorded field changes are informational.

    ``field_binding`` must be a :class:`SourceSchemaFieldBinding`; passing an
    arbitrary string raises ``TypeError`` instead of guessing source semantics.
    """

    if not isinstance(field_binding, SourceSchemaFieldBinding):
        raise TypeError("field_binding must be a SourceSchemaFieldBinding")

    previous_by_key = {
        (schema_field.table_name, schema_field.field_name): schema_field
        for schema_field in previous_fields
    }
    current_by_key = {
        (schema_field.table_name, schema_field.field_name): schema_field
        for schema_field in current_fields
    }

    previous_tables = {schema_field.table_name for schema_field in previous_fields}
    current_tables = {schema_field.table_name for schema_field in current_fields}

    changes: list[SourceSchemaChange] = []

    for table_name in sorted(current_tables - previous_tables):
        changes.append(
            SourceSchemaChange(kind="table_added", table_name=table_name, impact="non_breaking")
        )

    for table_name in sorted(previous_tables - current_tables):
        changes.append(
            SourceSchemaChange(kind="table_removed", table_name=table_name, impact="breaking")
        )

    for key in sorted(current_by_key.keys() - previous_by_key.keys()):
        schema_field = current_by_key[key]
        changes.append(
            SourceSchemaChange(
                kind="field_added",
                table_name=schema_field.table_name,
                field_name=schema_field.field_name,
                impact="non_breaking",
                after=schema_field.canonical_dict(),
            )
        )

    for key in sorted(previous_by_key.keys() - current_by_key.keys()):
        schema_field = previous_by_key[key]
        changes.append(
            SourceSchemaChange(
                kind="field_removed",
                table_name=schema_field.table_name,
                field_name=schema_field.field_name,
                impact="breaking",
                before=schema_field.canonical_dict(),
            )
        )

    for key in sorted(previous_by_key.keys() & current_by_key.keys()):
        previous = previous_by_key[key]
        current = current_by_key[key]
        details = _source_schema_field_change_details(previous, current)

        if not details:
            continue

        changes.append(
            SourceSchemaChange(
                kind="field_changed",
                table_name=current.table_name,
                field_name=current.field_name,
                impact=_source_schema_field_change_impact(
                    previous, current, field_binding=field_binding
                ),
                before=previous.canonical_dict(),
                after=current.canonical_dict(),
                details=details,
            )
        )

    return tuple(changes)


def _source_schema_field_change_details(
    previous: SourceSchemaField, current: SourceSchemaField
) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}

    for attribute in ("ordinal_position", "source_type", "source_length", "nullable", "attributes"):
        previous_value = getattr(previous, attribute)
        current_value = getattr(current, attribute)

        if previous_value != current_value:
            details[attribute] = {"before": previous_value, "after": current_value}

    return details


def _source_schema_field_change_impact(
    previous: SourceSchemaField,
    current: SourceSchemaField,
    *,
    field_binding: SourceSchemaFieldBinding,
) -> SourceSchemaChangeImpact:
    if previous.source_type != current.source_type:
        return "breaking"

    if (
        previous.source_length is not None
        and current.source_length is not None
        and current.source_length < previous.source_length
    ):
        return "breaking"

    if previous.nullable is True and current.nullable is False:
        return "breaking"

    if previous.ordinal_position != current.ordinal_position:
        if field_binding is SourceSchemaFieldBinding.BY_POSITION:
            return "breaking"

        return "non_breaking"

    return "informational"
