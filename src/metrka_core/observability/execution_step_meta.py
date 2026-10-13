"""Typed metadata carried by execution-step audit events."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import cast

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

_STRING_FIELDS = (
    "dataset_id",
    "dataset_file_id",
    "source_capture_id",
    "source_file_name",
    "original_source_file_name",
    "table_key",
    "bronze_run_id",
    "silver_run_id",
    "silver_build_id",
    "partition_key",
    "partition_value",
    "version_period",
    "contract_hash",
    "contract_name",
    "contract_path",
    "contract_version",
    "contract_snapshot_yaml_path",
    "contract_snapshot_json_path",
    "manifest_path",
)

_COUNT_FIELDS = (
    "input_row_count",
    "output_row_count",
    "input_column_count",
    "output_column_count",
    "input_file_count",
    "output_file_count",
    "input_byte_count",
    "output_byte_count",
)

_RESERVED_FIELDS = frozenset((*_STRING_FIELDS, *_COUNT_FIELDS))


def _validate_json_value(value: object, *, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or infinity")
        return

    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return

    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise TypeError(f"{path} must contain non-empty string keys")
            _validate_json_value(item, path=f"{path}.{key}")
        return

    raise TypeError(f"{path} contains a non-JSON value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class ExecutionStepMeta:
    """Canonical queryable fields plus explicitly non-queryable JSON evidence."""

    dataset_id: str | None = None
    dataset_file_id: str | None = None
    source_capture_id: str | None = None
    source_file_name: str | None = None
    original_source_file_name: str | None = None
    table_key: str | None = None
    bronze_run_id: str | None = None
    silver_run_id: str | None = None
    silver_build_id: str | None = None
    partition_key: str | None = None
    partition_value: str | None = None
    version_period: str | None = None
    contract_hash: str | None = None
    contract_name: str | None = None
    contract_path: str | None = None
    contract_version: str | None = None
    contract_snapshot_yaml_path: str | None = None
    contract_snapshot_json_path: str | None = None
    input_row_count: int | None = None
    output_row_count: int | None = None
    input_column_count: int | None = None
    output_column_count: int | None = None
    input_file_count: int | None = None
    output_file_count: int | None = None
    input_byte_count: int | None = None
    output_byte_count: int | None = None
    manifest_path: str | None = None
    # Extra evidence is deliberately open-ended at the type boundary. Its
    # complete JSON shape is checked recursively in __post_init__.
    extra: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in _STRING_FIELDS:
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None")
            if value == "" and field_name != "contract_version":
                raise ValueError(f"{field_name} must not be empty")

        for field_name in _COUNT_FIELDS:
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise TypeError(f"{field_name} must be an integer or None")
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must not be negative")

        normalized_extra = dict(self.extra)
        conflicts = _RESERVED_FIELDS.intersection(normalized_extra)
        if conflicts:
            raise ValueError(
                f"Execution step extra metadata uses reserved fields: {sorted(conflicts)}"
            )

        _validate_json_value(normalized_extra, path="extra")
        object.__setattr__(self, "extra", MappingProxyType(normalized_extra))

    def to_dict(self) -> dict[str, JsonValue]:
        """Return the canonical JSON object stored with an execution event."""

        result = {key: cast(JsonValue, value) for key, value in self.extra.items()}
        for field_name in (*_STRING_FIELDS, *_COUNT_FIELDS):
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = cast(JsonValue, value)
        return result

    def merged_with(self, update: ExecutionStepMeta | None) -> ExecutionStepMeta:
        """Overlay finish metadata on start metadata without losing evidence."""

        if update is None:
            return self

        return self.from_mapping({**self.to_dict(), **update.to_dict()})

    @classmethod
    def from_mapping(cls, value: Mapping[str, object] | None) -> ExecutionStepMeta:
        """Validate a serialized metadata object at the persistence boundary."""

        if value is None:
            return cls()

        raw = dict(value)
        extra = {key: item for key, item in raw.items() if key not in _RESERVED_FIELDS}

        return cls(
            dataset_id=cast(str | None, raw.get("dataset_id")),
            dataset_file_id=cast(str | None, raw.get("dataset_file_id")),
            source_capture_id=cast(str | None, raw.get("source_capture_id")),
            source_file_name=cast(str | None, raw.get("source_file_name")),
            original_source_file_name=cast(str | None, raw.get("original_source_file_name")),
            table_key=cast(str | None, raw.get("table_key")),
            bronze_run_id=cast(str | None, raw.get("bronze_run_id")),
            silver_run_id=cast(str | None, raw.get("silver_run_id")),
            silver_build_id=cast(str | None, raw.get("silver_build_id")),
            partition_key=cast(str | None, raw.get("partition_key")),
            partition_value=cast(str | None, raw.get("partition_value")),
            version_period=cast(str | None, raw.get("version_period")),
            contract_hash=cast(str | None, raw.get("contract_hash")),
            contract_name=cast(str | None, raw.get("contract_name")),
            contract_path=cast(str | None, raw.get("contract_path")),
            contract_version=cast(str | None, raw.get("contract_version")),
            contract_snapshot_yaml_path=cast(str | None, raw.get("contract_snapshot_yaml_path")),
            contract_snapshot_json_path=cast(str | None, raw.get("contract_snapshot_json_path")),
            input_row_count=cast(int | None, raw.get("input_row_count")),
            output_row_count=cast(int | None, raw.get("output_row_count")),
            input_column_count=cast(int | None, raw.get("input_column_count")),
            output_column_count=cast(int | None, raw.get("output_column_count")),
            input_file_count=cast(int | None, raw.get("input_file_count")),
            output_file_count=cast(int | None, raw.get("output_file_count")),
            input_byte_count=cast(int | None, raw.get("input_byte_count")),
            output_byte_count=cast(int | None, raw.get("output_byte_count")),
            manifest_path=cast(str | None, raw.get("manifest_path")),
            extra=extra,
        )
