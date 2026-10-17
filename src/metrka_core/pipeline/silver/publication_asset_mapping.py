"""Map immutable Silver manifests to publication assets."""

from __future__ import annotations

from typing import Any

from metrka_core.catalog.publication_asset_models import DatasetPublicationAssetRequest


def publication_assets_from_manifest(
    manifest: dict[str, Any],
) -> tuple[DatasetPublicationAssetRequest, ...]:
    raw_tables = manifest.get("tables")

    if not isinstance(raw_tables, list):
        raise ValueError("Silver manifest tables must be a list")

    assets: list[DatasetPublicationAssetRequest] = []

    for index, raw_entry in enumerate(raw_tables):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"Silver manifest table entry {index} must be a mapping")

        raw_columns = raw_entry.get("columns")

        if not isinstance(raw_columns, list):
            raise ValueError(f"Silver manifest table entry {index} columns must be a list")

        assets.append(
            DatasetPublicationAssetRequest(
                table_key=_required_string(raw_entry, "table_key", index),
                file_path=_required_string(raw_entry, "path", index),
                file_format=_required_string(raw_entry, "format", index),
                row_count=_required_non_negative_integer(raw_entry, "row_count", index),
                column_count=_required_non_negative_integer(raw_entry, "column_count", index),
                columns=tuple(str(column) for column in raw_columns),
                size_bytes=_required_non_negative_integer(raw_entry, "size_bytes", index),
                checksum=_required_string(raw_entry, "checksum", index),
            )
        )

    if not assets:
        raise ValueError("Silver publication manifest contains no assets")

    return tuple(assets)


def _required_string(entry: dict[str, Any], field_name: str, index: int) -> str:
    value = entry.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Silver manifest table entry {index} field {field_name!r} must be a non-empty string"
        )

    return value.strip()


def _required_non_negative_integer(entry: dict[str, Any], field_name: str, index: int) -> int:
    value = entry.get(field_name)

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"Silver manifest table entry {index} field {field_name!r} must be an integer"
        )

    if value < 0:
        raise ValueError(
            f"Silver manifest table entry {index} field {field_name!r} must not be negative"
        )

    return value
