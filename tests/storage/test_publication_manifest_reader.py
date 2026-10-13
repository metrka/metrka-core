"""Regression tests for the shared Silver manifest reader."""

from __future__ import annotations

import json
from pathlib import Path

from metrka_core.storage.silver_store import LocalSilverArtifactStore


def test_silver_store_reuses_reader_with_its_configured_manifest_root(tmp_path: Path) -> None:
    silver_root = tmp_path / "custom" / "silver"
    manifest_path = silver_root / "manifests" / "demo" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({"dataset_id": "demo.main"}), encoding="utf-8")
    store = LocalSilverArtifactStore(
        workspace_root=tmp_path,
        silver_root=silver_root,
        current_root=tmp_path / "custom" / "current",
    )

    manifest = store.read_manifest(path=manifest_path.relative_to(tmp_path).as_posix())

    assert manifest == {"dataset_id": "demo.main"}
