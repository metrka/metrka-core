"""Behavioural tests for public dataset workspace initialization."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
import yaml

from metrka_core.datasets.scaffolding import WorkspaceInitializationResult, initialize_workspace
from metrka_core.datasets.source_config import load_source_config
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement
from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver
from metrka_core.pipeline.config import RuntimeConfigError
from metrka_core.pipeline.models import parse_pipeline_spec
from metrka_core.quality.config import load_quality_config
from metrka_core.storage.workspace_initializer import LocalWorkspaceInitializer
from metrka_core.storage.workspace_layout import WorkspaceLayout


def test_initialize_workspace_creates_and_registers_portable_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"

    result = initialize_workspace(
        "example_dataset",
        download_url="https://example.org/files/source.csv",
        workspaces_config_path=config_path,
        workspace_root="datasets/example_dataset",
        source_name="Example Open Data",
    )

    workspace_root = (tmp_path / "datasets" / "example_dataset").resolve()
    assert result.workspace_name == "example_dataset"
    assert result.placement is WorkspacePlacement.PORTABLE
    assert result.workspace_root == workspace_root
    assert result.definition_root == workspace_root
    assert result.data_root == workspace_root / "data"
    assert result.workspaces_config_path == config_path.resolve()
    assert result.main_config_path == workspace_root / "conf" / "main.yaml"
    assert result.quality_config_path == workspace_root / "conf" / "quality.yaml"
    assert _read_yaml(config_path) == {
        "schema_version": 1,
        "workspaces": {
            "example_dataset": {
                "placement": "portable",
                "workspace_root": "datasets/example_dataset",
            }
        },
    }

    location = YamlWorkspaceLocationResolver.from_config_path(config_path).resolve(
        "example_dataset"
    )
    assert location == _result_location(result)

    initializer = LocalWorkspaceInitializer(layout=WorkspaceLayout(location=location))
    assert all(path.is_dir() for path in initializer.required_directories())

    source_config = load_source_config(result.main_config_path, expected_ws_name="example_dataset")
    assert set(source_config.streams) == {"data"}
    assert source_config.streams["data"].official_filename == "source.csv"
    assert source_config.streams["data"].extra == {
        "download_url": "https://example.org/files/source.csv"
    }
    main_config = yaml.safe_load(result.main_config_path.read_text(encoding="utf-8"))
    assert "source" not in main_config

    pipeline = parse_pipeline_spec(source_config.pipeline)
    assert pipeline.acquisition.extractor == "http.files"
    assert [step.action for step in pipeline.steps] == ["bronze.ingest"]

    quality = load_quality_config(result.quality_config_path)
    assert quality.tables == {}
    assert (workspace_root / ".gitignore").read_text(encoding="utf-8") == "data/\n"
    readme = (workspace_root / "README.md").read_text(encoding="utf-8")
    assert "Bronze" in readme
    assert "Source: Example Open Data." in readme
    assert "`meta` block" in readme


def test_initialize_workspace_uses_environment_workspace_config_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "configuration" / "workspaces.yaml"
    monkeypatch.setenv("METRKA_WORKSPACES_CONFIG_PATH", str(config_path))

    result = initialize_workspace(
        "environment_dataset",
        download_url="https://example.org/source.csv",
        workspace_root="workspaces/environment_dataset",
    )

    assert result.workspaces_config_path == config_path.resolve()
    assert (
        result.workspace_root
        == (config_path.parent / "workspaces" / "environment_dataset").resolve()
    )
    assert config_path.is_file()


def test_initialize_workspace_serializes_concurrent_registry_updates(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    start = Barrier(2)

    def initialize(workspace_name: str) -> WorkspaceInitializationResult:
        start.wait(timeout=5)
        return initialize_workspace(
            workspace_name,
            download_url=f"https://example.org/{workspace_name}.csv",
            workspaces_config_path=config_path,
            workspace_root=f"datasets/{workspace_name}",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(initialize, "first_dataset")
        second = executor.submit(initialize, "second_dataset")
        results = (first.result(timeout=10), second.result(timeout=10))

    assert {result.workspace_name for result in results} == {"first_dataset", "second_dataset"}
    assert set(_read_yaml(config_path)["workspaces"]) == {"first_dataset", "second_dataset"}


def test_initialize_workspace_rejects_implicit_production_config_before_writing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METRKA_ENV", "production")
    monkeypatch.delenv("METRKA_WORKSPACES_CONFIG_PATH", raising=False)

    with pytest.raises(RuntimeConfigError, match="Production workspace configuration"):
        initialize_workspace(
            "production_dataset",
            download_url="https://example.org/source.csv",
            workspace_root="workspaces/production_dataset",
        )

    assert not (tmp_path / "workspaces.local.yaml").exists()
    assert not (tmp_path / "workspaces" / "production_dataset").exists()


def test_initialize_workspace_creates_and_registers_managed_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"

    result = initialize_workspace(
        "managed_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=config_path,
        placement=WorkspacePlacement.MANAGED,
        definition_root="definitions/managed_dataset",
        data_root="runtime/managed_dataset",
    )

    assert result.placement is WorkspacePlacement.MANAGED
    assert result.workspace_root is None
    assert result.definition_root == (tmp_path / "definitions" / "managed_dataset").resolve()
    assert result.data_root == (tmp_path / "runtime" / "managed_dataset").resolve()
    assert result.main_config_path.is_file()
    assert result.quality_config_path.is_file()
    assert (result.definition_root / "README.md").is_file()
    assert not (result.definition_root / ".gitignore").exists()
    assert result.data_root.is_dir()
    assert _read_yaml(config_path) == {
        "schema_version": 1,
        "workspaces": {
            "managed_dataset": {
                "placement": "managed",
                "definition_root": "definitions/managed_dataset",
                "data_root": "runtime/managed_dataset",
            }
        },
    }


def test_initialize_workspace_preserves_existing_config_and_appends_workspace(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    existing_root = tmp_path / "datasets" / "existing_dataset"
    (existing_root / "data").mkdir(parents=True)
    config_path.write_text(
        "# Workspaces available in this checkout.\n"
        "schema_version: 1\n"
        "workspaces:\n"
        "  existing_dataset:\n"
        "    placement: portable\n"
        "    workspace_root: datasets/existing_dataset\n",
        encoding="utf-8",
    )

    result = initialize_workspace(
        "example_dataset",
        download_url="https://example.org/archive.csv?version=1",
        workspaces_config_path=config_path,
        workspace_root="datasets/health/example_dataset",
        stream_name="county",
    )

    assert result.workspace_root == (tmp_path / "datasets" / "health" / "example_dataset").resolve()
    assert _read_yaml(config_path) == {
        "schema_version": 1,
        "workspaces": {
            "existing_dataset": {
                "placement": "portable",
                "workspace_root": "datasets/existing_dataset",
            },
            "example_dataset": {
                "placement": "portable",
                "workspace_root": "datasets/health/example_dataset",
            },
        },
    }
    assert config_path.read_text(encoding="utf-8").startswith(
        "# Workspaces available in this checkout.\n"
    )
    source_config = load_source_config(result.main_config_path)
    assert source_config.streams["county"].official_filename == "archive.csv"


@pytest.mark.parametrize(
    ("workspace_name", "download_url", "kwargs", "message"),
    [
        ("Example", "https://example.org/source.csv", {}, "workspace_name"),
        ("example", "file:///tmp/source.csv", {}, "download_url"),
        ("example", "https://example.org/source.csv", {}, "workspace_root is required"),
        (
            "example",
            "https://example.org/source.csv",
            {"placement": "managed", "definition_root": "definitions/example"},
            "definition_root and data_root are required",
        ),
    ],
)
def test_initialize_workspace_rejects_invalid_input_without_writing_roots(
    tmp_path: Path, workspace_name: str, download_url: str, kwargs: dict[str, object], message: str
) -> None:
    config_path = tmp_path / "workspaces.local.yaml"

    with pytest.raises(ValueError, match=message):
        initialize_workspace(
            workspace_name, download_url=download_url, workspaces_config_path=config_path, **kwargs
        )

    assert not config_path.exists()
    assert not (tmp_path / "definitions").exists()
    assert not (tmp_path / "runtime").exists()


def test_initialize_workspace_rejects_duplicate_registration(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    initialize_workspace(
        "example_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=config_path,
        workspace_root="datasets/example_dataset",
    )

    with pytest.raises(ValueError, match="already registered"):
        initialize_workspace(
            "example_dataset",
            download_url="https://example.org/source.csv",
            workspaces_config_path=config_path,
            workspace_root="datasets/another",
        )


def test_initialize_workspace_rejects_root_owned_by_another_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    initialize_workspace(
        "first_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=config_path,
        workspace_root="datasets/first",
    )

    with pytest.raises(ValueError, match="reuses roots assigned"):
        initialize_workspace(
            "second_dataset",
            download_url="https://example.org/source.csv",
            workspaces_config_path=config_path,
            placement="managed",
            definition_root="datasets/first",
            data_root="runtime/second",
        )

    assert not (tmp_path / "runtime" / "second").exists()


def test_initialize_workspace_validates_config_format_before_creating_workspace(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    config_path.write_text(
        "schema_version: 1\n"
        "workspaces: {existing: {placement: portable, workspace_root: existing}}\n",
        encoding="utf-8",
    )
    (tmp_path / "existing" / "data").mkdir(parents=True)

    with pytest.raises(ValueError, match="block-style"):
        initialize_workspace(
            "example_dataset",
            download_url="https://example.org/source.csv",
            workspaces_config_path=config_path,
            workspace_root="datasets/example_dataset",
        )

    assert not (tmp_path / "datasets" / "example_dataset").exists()


def _result_location(result: WorkspaceInitializationResult) -> WorkspaceLocation:
    if result.workspace_root is not None:
        return WorkspaceLocation.portable(
            workspace_name=result.workspace_name, workspace_root=result.workspace_root
        )

    return WorkspaceLocation.managed(
        workspace_name=result.workspace_name,
        definition_root=result.definition_root,
        data_root=result.data_root,
    )


def _read_yaml(path: Path) -> object:
    return yaml.safe_load(path.read_text(encoding="utf-8"))
