"""Tests for the dual-root workspace directory layout."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.storage.workspace_layout import WorkspaceLayout


@pytest.fixture
def layout(tmp_path: Path) -> WorkspaceLayout:
    location = WorkspaceLocation.managed(
        workspace_name="orders",
        definition_root=tmp_path / "definitions" / "orders",
        data_root=tmp_path / "data" / "orders",
    )
    return WorkspaceLayout(location=location)


def test_init_exposes_resolved_definition_and_data_roots(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions" / ".." / "definitions" / "orders"
    data_root = tmp_path / "data" / ".." / "data" / "orders"

    layout = WorkspaceLayout(
        location=WorkspaceLocation.managed(
            workspace_name="orders", definition_root=definition_root, data_root=data_root
        )
    )

    assert layout.definition_root == definition_root.resolve()
    assert layout.data_root == data_root.resolve()


def test_init_rejects_non_location() -> None:
    with pytest.raises(TypeError, match="location must be a WorkspaceLocation"):
        WorkspaceLayout(location="not-a-location")  # type: ignore[arg-type]


def test_top_level_directories(layout: WorkspaceLayout) -> None:
    assert layout.conf_dir == layout.definition_root / "conf"
    assert layout.logs_dir == layout.data_root / "logs"
    assert layout.files_dir == layout.data_root / "files"


def test_bronze_directories(layout: WorkspaceLayout) -> None:
    assert layout.bronze_dir == layout.files_dir / "bronze"
    assert layout.bronze_landing_dir == layout.bronze_dir / "landing"
    assert layout.bronze_runs_dir == layout.bronze_dir / "runs"
    assert layout.bronze_run_dir("bronze-run-1") == layout.bronze_runs_dir / "bronze-run-1"


def test_bronze_run_dir_rejects_empty_id(layout: WorkspaceLayout) -> None:
    with pytest.raises(ValueError, match="run_id is required"):
        layout.bronze_run_dir("")


def test_silver_directories(layout: WorkspaceLayout) -> None:
    assert layout.silver_dir == layout.files_dir / "silver"
    assert layout.silver_tables_dir == layout.silver_dir / "tables"
    assert layout.silver_manifests_dir == layout.silver_dir / "manifests"
    assert layout.silver_views_dir == layout.silver_dir / "views"
    assert layout.silver_transformation_impacts_dir == layout.silver_dir / "transformation_impacts"


def test_receipt_and_contract_directories(layout: WorkspaceLayout) -> None:
    assert layout.receipts_dir == layout.data_root / "receipts"
    assert layout.executions_dir == layout.receipts_dir / "executions"
    assert layout.contract_snapshots_dir == layout.data_root / "contracts"


def test_current_state_directories(layout: WorkspaceLayout) -> None:
    assert layout.current_dir == layout.data_root / "current"
    assert layout.current_latest_dir == layout.current_dir / "latest"
    assert layout.current_checks_dir == layout.current_dir / "checks"
    assert layout.bronze_latest_dir == layout.current_latest_dir / "bronze"
    assert layout.silver_latest_dir == layout.current_latest_dir / "silver"


def test_current_marker_paths(layout: WorkspaceLayout) -> None:
    assert layout.bronze_execution_marker_path == layout.bronze_latest_dir / "_run_in_progress.json"
    assert layout.silver_execution_marker_path == layout.silver_latest_dir / "_run_in_progress.json"


def test_config_paths(layout: WorkspaceLayout) -> None:
    assert layout.config_path() == layout.conf_dir / "config.yaml"
    assert layout.config_path("pipeline.yaml") == layout.conf_dir / "pipeline.yaml"


@pytest.mark.parametrize("operation", [lambda layout: layout.config_path("")])
def test_required_path_segments_reject_empty_values(
    layout: WorkspaceLayout, operation: Callable[[WorkspaceLayout], Path]
) -> None:
    with pytest.raises(ValueError, match="is required"):
        operation(layout)
