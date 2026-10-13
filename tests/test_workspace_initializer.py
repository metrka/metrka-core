"""Tests for local dataset workspace initialization."""

from __future__ import annotations

from pathlib import Path

from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.storage.workspace_initializer import LocalWorkspaceInitializer
from metrka_core.storage.workspace_layout import WorkspaceLayout


def test_initializer_creates_complete_workspace_tree(tmp_path: Path) -> None:
    layout = WorkspaceLayout(
        location=WorkspaceLocation.portable(
            workspace_name="dataset", workspace_root=tmp_path / "dataset"
        )
    )
    initializer = LocalWorkspaceInitializer(layout=layout)

    initializer.ensure_structure()
    initializer.ensure_structure()

    required = initializer.required_directories()

    assert layout.silver_transformation_impacts_dir in required
    assert required
    assert all(path.is_dir() for path in required)
    assert len(required) == len(set(required))
    assert not (layout.files_dir / "gold").exists()
    assert not (layout.current_latest_dir / "gold").exists()
