from __future__ import annotations

from typing import Any

import pytest

from metrka_core.pipeline.silver.publication_asset_mapping import publication_assets_from_manifest


def _manifest(**overrides: object) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "table_key": "people",
        "path": "people/data.parquet",
        "format": "parquet",
        "row_count": 2,
        "column_count": 1,
        "columns": ["person_id"],
        "size_bytes": 128,
        "checksum": f"sha256:{'a' * 64}",
    }
    entry.update(overrides)
    return {"tables": [entry]}


@pytest.mark.parametrize("field_name", ["row_count", "column_count", "size_bytes"])
def test_manifest_rejects_negative_asset_measure(field_name: str) -> None:
    with pytest.raises(ValueError, match=rf"field '{field_name}' must not be negative"):
        publication_assets_from_manifest(_manifest(**{field_name: -1}))


def test_manifest_accepts_zero_asset_measures() -> None:
    assets = publication_assets_from_manifest(
        _manifest(row_count=0, column_count=0, columns=[], size_bytes=0)
    )

    assert len(assets) == 1
    assert assets[0].row_count == 0
    assert assets[0].column_count == 0
    assert assets[0].size_bytes == 0
