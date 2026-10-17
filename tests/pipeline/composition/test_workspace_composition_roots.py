"""Behavioural tests for definition/data workspace composition."""

from __future__ import annotations

from pathlib import Path

import yaml

from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.composition.workspace import build_workspace_composition


class FixedWorkspaceLocationResolver:
    def __init__(self, location: WorkspaceLocation) -> None:
        self._location = location

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        if workspace_name != self._location.workspace_name:
            raise KeyError(workspace_name)

        return self._location


def test_composition_reads_definitions_and_writes_only_below_data_root(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions" / "example"
    data_root = tmp_path / "persistent-data" / "example"
    config_root = definition_root / "conf"
    config_root.mkdir(parents=True)
    _write_yaml(config_root / "main.yaml", _source_config())
    _write_yaml(config_root / "quality.yaml", _quality_config())
    location = WorkspaceLocation.managed(
        workspace_name="example", definition_root=definition_root, data_root=data_root
    )
    services = RuntimeServices()

    composition = build_workspace_composition(
        workspace_name="example",
        config_name="main.yaml",
        workspace_locations=FixedWorkspaceLocationResolver(location),
        clock=services.clock,
        source_capture_ids=services.source_capture_ids,
    )

    assert composition.layout.definition_root == definition_root.resolve()
    assert composition.layout.data_root == data_root.resolve()
    assert composition.config_store.path(name="main.yaml") == config_root / "main.yaml"
    assert composition.bronze_store.workspace_root == data_root.resolve()
    assert composition.silver_store.workspace_root == data_root.resolve()
    assert composition.contract_store.definition_root == definition_root.resolve()
    assert composition.contract_store.data_root == data_root.resolve()
    assert composition.quality_config.tables == {}
    contract_path = composition.contract_store.definition_relative_path(config_root / "main.yaml")
    snapshots_path = composition.contract_store.snapshot_relative_path(
        composition.layout.contract_snapshots_dir
    )
    assert contract_path == "conf/main.yaml"
    assert snapshots_path == "contracts"
    assert composition.landing_store.root == data_root.resolve() / "files" / "bronze" / "landing"
    assert data_root.is_dir()
    assert not (definition_root / "data").exists()
    assert not (definition_root / "logs").exists()


def _write_yaml(path: Path, value: object) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


def _source_config() -> dict[str, object]:
    return {
        "workspace_name": "example",
        "source": {
            "name": "Example",
            "system": "Example",
            "url": "https://example.org/source.csv",
            "update_frequency": "unknown",
        },
        "streams": {
            "data": {
                "display_name": "Example data",
                "description": "Example data",
                "official_filename": "source.csv",
                "download_url": "https://example.org/source.csv",
            }
        },
        "pipeline": {
            "quality": {"config": "quality.yaml"},
            "acquisition": {"extractor": "http.files"},
            "steps": [{"action": "bronze.ingest"}],
        },
    }


def _quality_config() -> dict[str, object]:
    return {"version": 1}
