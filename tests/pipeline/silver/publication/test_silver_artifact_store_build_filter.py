"""Tests for scoped discovery of local Silver build artifacts."""

from __future__ import annotations

from pathlib import Path

import pytest

import metrka_core.storage.silver_store as silver_store_module
from metrka_core.pipeline.silver.artifact_models import SilverBuildArtifactQuery
from metrka_core.storage.silver_store import LocalSilverArtifactStore


def test_list_build_artifact_directories_returns_only_requested_builds(tmp_path: Path) -> None:
    silver_root = tmp_path / "data" / "files" / "silver"
    current_root = tmp_path / "data" / "current"
    requested = silver_root / "tables" / "people" / "year=2026" / "silver_build_id=build-a"
    unrelated = silver_root / "tables" / "people" / "year=2025" / "silver_build_id=build-b"
    requested.mkdir(parents=True)
    unrelated.mkdir(parents=True)

    store = LocalSilverArtifactStore(
        workspace_root=tmp_path, silver_root=silver_root, current_root=current_root
    )

    result = store.list_build_artifact_directories(
        builds=(
            SilverBuildArtifactQuery(
                dataset_id="people",
                silver_build_id="build-a",
                partition_key="year",
                partition_value="2026",
            ),
        )
    )

    assert result == {"build-a": (requested.resolve(),)}


def test_list_build_artifact_directories_skips_scan_for_empty_request(tmp_path: Path) -> None:
    silver_root = tmp_path / "data" / "files" / "silver"
    current_root = tmp_path / "data" / "current"
    unrelated = silver_root / "tables" / "people" / "year=2025" / "silver_build_id=build-b"
    unrelated.mkdir(parents=True)

    store = LocalSilverArtifactStore(
        workspace_root=tmp_path, silver_root=silver_root, current_root=current_root
    )

    assert store.list_build_artifact_directories(builds=()) == {}


def test_delete_build_artifacts_returns_partial_failure_without_stopping(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    silver_root = tmp_path / "data" / "files" / "silver"
    current_root = tmp_path / "data" / "current"
    build_directory_name = "silver_build_id=build-a"
    blocked = silver_root / "tables" / "people" / "year=2026" / build_directory_name
    removable = silver_root / "manifests" / "people" / "year=2026" / build_directory_name
    blocked.mkdir(parents=True)
    removable.mkdir(parents=True)
    store = LocalSilverArtifactStore(
        workspace_root=tmp_path, silver_root=silver_root, current_root=current_root
    )
    original_rmtree = silver_store_module.shutil.rmtree

    def controlled_rmtree(path: Path) -> None:
        if path == blocked.resolve():
            raise PermissionError("access denied")
        original_rmtree(path)

    monkeypatch.setattr(silver_store_module.shutil, "rmtree", controlled_rmtree)

    result = store.delete_build_artifact_directories(
        silver_build_id="build-a", artifact_directories=(blocked, removable)
    )

    assert not result.deleted
    assert result.deleted_directories == (removable.resolve(),)
    assert len(result.errors) == 1
    assert result.errors[0].artifact_directory == blocked.resolve()
    assert result.errors[0].error_type == "PermissionError"
    assert blocked.is_dir()
    assert not removable.exists()
