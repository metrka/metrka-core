"""Sources that keep each dataset in its own folder (``layout: dataset_folders``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from metrka_core.datasets.scaffolding import initialize_workspace
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.datasets.yaml_workspace_resolver import (
    YamlWorkspaceLocationResolver,
    load_workspace_locations,
)
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.composition.workspace import build_workspace_composition
from metrka_core.pipeline.workspace_validation import validate_workspace


def _registry(tmp_path: Path, workspaces: dict[str, Any]) -> Path:
    path = tmp_path / "datasets.local.yaml"
    path.write_text(
        yaml.safe_dump({"schema_version": 1, "workspaces": workspaces}), encoding="utf-8"
    )
    return path


def _source_registry(tmp_path: Path) -> Path:
    return _registry(
        tmp_path,
        {
            "fl": {
                "placement": "portable",
                "workspace_root": str(tmp_path / "fl"),
                "layout": "dataset_folders",
            }
        },
    )


def _managed_source_registry(tmp_path: Path) -> Path:
    """Definitions in ``fl/<dataset>``, data in ``store/fl/<dataset>``."""

    (tmp_path / "store" / "fl").mkdir(parents=True, exist_ok=True)
    return _registry(
        tmp_path,
        {
            "fl": {
                "placement": "managed",
                "definition_root": str(tmp_path / "fl"),
                "data_root": str(tmp_path / "store" / "fl"),
                "layout": "dataset_folders",
            }
        },
    )


def _dataset_folder(
    tmp_path: Path, *, dataset: str = "beds", workspace_name: str = "fl", stream: str | None = None
) -> Path:
    """Scaffold a valid workspace, then turn it into dataset folder ``fl/<dataset>``."""

    scaffold = initialize_workspace(
        "scaffold",
        download_url="https://example.org/source.csv",
        workspaces_config_path=tmp_path / "scaffold.yaml",
        workspace_root=tmp_path / "scaffold",
    )
    conf = tmp_path / "fl" / dataset / "conf"
    conf.mkdir(parents=True)

    for file in scaffold.main_config_path.parent.iterdir():
        (conf / file.name).write_bytes(file.read_bytes())

    main = yaml.safe_load((conf / "main.yaml").read_text(encoding="utf-8"))
    main["workspace_name"] = workspace_name
    main["streams"] = {stream or dataset: main["streams"]["data"]}
    (conf / "main.yaml").write_text(yaml.safe_dump(main, sort_keys=False), encoding="utf-8")

    return conf.parent


def test_registry_marks_a_source_that_keeps_datasets_in_folders(tmp_path: Path) -> None:
    locations = load_workspace_locations(_source_registry(tmp_path))

    assert locations["fl"].dataset_folders
    assert locations["fl"].workspace_root == (tmp_path / "fl").resolve()


def test_registry_rejects_an_unknown_layout(tmp_path: Path) -> None:
    registry = _registry(
        tmp_path,
        {"fl": {"placement": "portable", "workspace_root": str(tmp_path), "layout": "nested"}},
    )

    with pytest.raises(ValueError, match="unsupported layout 'nested'"):
        load_workspace_locations(registry)


def test_registry_marks_a_managed_source_that_keeps_datasets_in_folders(tmp_path: Path) -> None:
    locations = load_workspace_locations(_managed_source_registry(tmp_path))

    assert locations["fl"].dataset_folders
    assert not locations["fl"].is_portable
    assert locations["fl"].definition_root == (tmp_path / "fl").resolve()
    assert locations["fl"].data_root == (tmp_path / "store" / "fl").resolve()


def test_registry_rejects_an_unknown_managed_layout(tmp_path: Path) -> None:
    registry = _registry(
        tmp_path,
        {
            "fl": {
                "placement": "managed",
                "definition_root": str(tmp_path / "definitions"),
                "data_root": str(tmp_path / "data"),
                "layout": "nested",
            }
        },
    )

    with pytest.raises(ValueError, match="unsupported layout 'nested'"):
        load_workspace_locations(registry)


def test_resolve_finds_the_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)
    resolver = YamlWorkspaceLocationResolver.from_config_path(_source_registry(tmp_path))

    location = resolver.resolve("fl.beds")

    assert location.name == "fl.beds"
    assert location.workspace_name == "fl"
    assert location.dataset_name == "beds"
    assert location.definition_root == folder.resolve()
    assert location.data_root == (folder / "data").resolve()
    assert not location.dataset_folders


def test_resolve_asks_for_a_dataset_when_given_the_source(tmp_path: Path) -> None:
    resolver = YamlWorkspaceLocationResolver.from_config_path(_source_registry(tmp_path))

    with pytest.raises(ValueError, match=r"name one dataset, for example fl\.<dataset>"):
        resolver.resolve("fl")


def test_resolve_rejects_a_missing_dataset_folder(tmp_path: Path) -> None:
    _dataset_folder(tmp_path)
    resolver = YamlWorkspaceLocationResolver.from_config_path(_source_registry(tmp_path))

    with pytest.raises(RuntimeError, match="Dataset folder 'cars' does not exist in source 'fl'"):
        resolver.resolve("fl.cars")


@pytest.mark.parametrize("dataset_name", ["bad name", "..", "-beds"])
def test_dataset_folder_names_are_plain_identifiers(dataset_name: str) -> None:
    source = WorkspaceLocation.portable(
        workspace_name="fl", workspace_root=Path("fl"), dataset_folders=True
    )

    with pytest.raises(ValueError, match="must use only letters, digits"):
        source.dataset_folder(dataset_name)


def test_a_managed_dataset_folder_keeps_its_data_under_the_source_data_root(tmp_path: Path) -> None:
    source = WorkspaceLocation.managed(
        workspace_name="fl",
        definition_root=tmp_path / "definitions",
        data_root=tmp_path / "store",
        dataset_folders=True,
    )

    folder = source.dataset_folder("beds")

    assert folder.definition_root == (tmp_path / "definitions" / "beds").resolve()
    assert folder.data_root == (tmp_path / "store" / "beds").resolve()
    assert folder.name == "fl.beds"
    assert not folder.is_portable
    assert not folder.dataset_folders


def test_resolve_finds_a_managed_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)
    resolver = YamlWorkspaceLocationResolver.from_config_path(_managed_source_registry(tmp_path))

    location = resolver.resolve("fl.beds")

    assert location.name == "fl.beds"
    assert location.definition_root == folder.resolve()
    assert location.data_root == (tmp_path / "store" / "fl" / "beds").resolve()
    assert not location.is_portable
    assert resolver.resolve_dataset("fl.beds") == location


def test_resolve_requires_the_managed_source_data_root(tmp_path: Path) -> None:
    _dataset_folder(tmp_path)
    registry = _managed_source_registry(tmp_path)
    (tmp_path / "store" / "fl").rmdir()
    resolver = YamlWorkspaceLocationResolver.from_config_path(registry)

    with pytest.raises(RuntimeError, match="data_root is not available"):
        resolver.resolve("fl.beds")


def test_resolve_dataset_handles_both_layouts(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)
    flat_root = tmp_path / "flat"
    flat_root.mkdir()
    registry = _registry(
        tmp_path,
        {
            "flat": {"placement": "portable", "workspace_root": str(flat_root)},
            "fl": {
                "placement": "portable",
                "workspace_root": str(tmp_path / "fl"),
                "layout": "dataset_folders",
            },
        },
    )
    resolver = YamlWorkspaceLocationResolver.from_config_path(registry)

    assert resolver.resolve_dataset("flat.inmate_active").name == "flat"
    assert resolver.resolve_dataset("fl.beds").definition_root == folder.resolve()

    with pytest.raises(ValueError, match="does not contain a stream name"):
        resolver.resolve_dataset("no_stream")


def test_validate_workspace_accepts_a_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)

    result = validate_workspace("fl.beds", workspaces_config_path=_source_registry(tmp_path))

    assert result.workspace_name == "fl.beds"
    assert result.stream_names == ("beds",)
    assert result.definition_root == folder.resolve()


def test_validate_workspace_requires_the_stream_to_match_the_folder(tmp_path: Path) -> None:
    _dataset_folder(tmp_path, stream="other")

    with pytest.raises(RuntimeError, match="exactly one stream named 'beds'"):
        validate_workspace("fl.beds", workspaces_config_path=_source_registry(tmp_path))


def test_validate_workspace_requires_the_source_workspace_name(tmp_path: Path) -> None:
    _dataset_folder(tmp_path, workspace_name="fl_beds")

    with pytest.raises(RuntimeError, match="Unexpected workspace_name"):
        validate_workspace("fl.beds", workspaces_config_path=_source_registry(tmp_path))


def test_runtime_composition_runs_inside_the_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)
    services = RuntimeServices()

    composition = build_workspace_composition(
        workspace_name="fl.beds",
        config_name="main.yaml",
        workspace_locations=YamlWorkspaceLocationResolver.from_config_path(
            _source_registry(tmp_path)
        ),
        clock=services.clock,
        source_capture_ids=services.source_capture_ids,
    )

    assert composition.run_name == "fl.beds"
    assert composition.layout.data_root == (folder / "data").resolve()
    assert composition.source_config.dataset_id("beds") == "fl.beds"


def test_validate_workspace_accepts_a_managed_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)

    result = validate_workspace(
        "fl.beds", workspaces_config_path=_managed_source_registry(tmp_path)
    )

    assert result.workspace_name == "fl.beds"
    assert result.definition_root == folder.resolve()
    assert result.data_root == (tmp_path / "store" / "fl" / "beds").resolve()


def test_runtime_composition_keeps_managed_data_out_of_the_dataset_folder(tmp_path: Path) -> None:
    folder = _dataset_folder(tmp_path)
    services = RuntimeServices()

    composition = build_workspace_composition(
        workspace_name="fl.beds",
        config_name="main.yaml",
        workspace_locations=YamlWorkspaceLocationResolver.from_config_path(
            _managed_source_registry(tmp_path)
        ),
        clock=services.clock,
        source_capture_ids=services.source_capture_ids,
    )

    data_root = (tmp_path / "store" / "fl" / "beds").resolve()
    assert composition.run_name == "fl.beds"
    assert composition.layout.data_root == data_root
    assert data_root.is_dir()
    assert not (folder / "data").exists()
