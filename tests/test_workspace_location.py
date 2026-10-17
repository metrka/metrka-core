"""Tests for portable and managed workspace placement."""

from __future__ import annotations

from pathlib import Path

import pytest

from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement


def test_portable_location_keeps_definition_and_data_under_one_root(tmp_path: Path) -> None:
    root = tmp_path / "customer_workspace"

    location = WorkspaceLocation.portable(workspace_name="customer", workspace_root=root)

    assert location.workspace_root == root.resolve()
    assert location.definition_root == root.resolve()
    assert location.data_root == (root / "data").resolve()
    assert location.is_portable is True
    assert location.placement is WorkspacePlacement.PORTABLE


def test_managed_location_allows_independent_roots(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions" / "customer"
    data_root = tmp_path / "mounted-data" / "customer"

    location = WorkspaceLocation.managed(
        workspace_name="customer", definition_root=definition_root, data_root=data_root
    )

    assert location.workspace_root is None
    assert location.definition_root == definition_root.resolve()
    assert location.data_root == data_root.resolve()
    assert location.is_portable is False
    assert location.placement is WorkspacePlacement.MANAGED


def test_portable_location_rejects_a_root_outside_workspace_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="data_root must be inside workspace_root"):
        WorkspaceLocation(
            workspace_name="customer",
            workspace_root=tmp_path / "workspace",
            definition_root=tmp_path / "workspace" / "definition",
            data_root=tmp_path / "outside",
        )


def test_location_rejects_identical_definition_and_data_roots(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be different directories"):
        WorkspaceLocation.managed(
            workspace_name="customer",
            definition_root=tmp_path / "same",
            data_root=tmp_path / "same",
        )


def test_managed_location_rejects_nested_roots(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not contain one another"):
        WorkspaceLocation.managed(
            workspace_name="customer",
            definition_root=tmp_path / "workspace",
            data_root=tmp_path / "workspace" / "data",
        )
