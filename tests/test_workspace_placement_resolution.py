from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.datasets.workspace_location import WorkspacePlacement
from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver
from metrka_core.pipeline.composition.workspace_locations import (
    build_workspace_location_resolver,
    resolve_workspaces_config_path,
    select_workspaces_config_path,
)
from metrka_core.pipeline.config import RuntimeConfigError, RuntimeEnvironment


def _write_yaml(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


def _portable_config(tmp_path: Path) -> tuple[Path, Path]:
    application_root = tmp_path / "application"
    workspace_root = tmp_path / "definitions" / "example_workspace"
    (workspace_root / "data").mkdir(parents=True)
    config_path = application_root / "workspaces.local.yaml"
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {
                "example": {
                    "placement": "portable",
                    "workspace_root": "../definitions/example_workspace",
                }
            },
        },
    )
    return config_path, workspace_root


def test_yaml_resolver_resolves_portable_workspace_relative_to_config(tmp_path: Path) -> None:
    config_path, workspace_root = _portable_config(tmp_path)

    location = YamlWorkspaceLocationResolver.from_config_path(config_path).resolve("example")

    assert location.placement is WorkspacePlacement.PORTABLE
    assert location.workspace_root == workspace_root.resolve()
    assert location.definition_root == workspace_root.resolve()
    assert location.data_root == (workspace_root / "data").resolve()


def test_yaml_resolver_resolves_managed_workspace_relative_to_config(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration" / "workspaces.local.yaml"
    definition_root = tmp_path / "definitions" / "example"
    data_root = tmp_path / "runtime" / "example"
    definition_root.mkdir(parents=True)
    data_root.mkdir(parents=True)
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {
                "example": {
                    "placement": "managed",
                    "definition_root": "../definitions/example",
                    "data_root": "../runtime/example",
                }
            },
        },
    )

    location = YamlWorkspaceLocationResolver.from_config_path(config_path).resolve("example")

    assert location.placement is WorkspacePlacement.MANAGED
    assert location.workspace_root is None
    assert location.definition_root == definition_root.resolve()
    assert location.data_root == data_root.resolve()


def test_yaml_resolver_rejects_unknown_workspace(tmp_path: Path) -> None:
    config_path, _workspace_root = _portable_config(tmp_path)
    resolver = YamlWorkspaceLocationResolver.from_config_path(config_path)

    with pytest.raises(KeyError, match="Unknown workspace"):
        resolver.resolve("unknown")


@pytest.mark.parametrize(
    "entry",
    [
        {"placement": "portable", "definition_root": "definitions"},
        {
            "placement": "managed",
            "workspace_root": "workspace",
            "definition_root": "definitions",
            "data_root": "data",
        },
        {"placement": "external", "workspace_root": "workspace"},
    ],
)
def test_yaml_resolver_rejects_invalid_placement_fields(
    tmp_path: Path, entry: dict[str, str]
) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    _write_yaml(config_path, {"schema_version": 1, "workspaces": {"example": entry}})

    with pytest.raises(ValueError, match="placement"):
        YamlWorkspaceLocationResolver.from_config_path(config_path)


def test_yaml_resolver_rejects_reused_data_root(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {
                "first": {
                    "placement": "managed",
                    "definition_root": "definitions/first",
                    "data_root": "runtime/shared",
                },
                "second": {
                    "placement": "managed",
                    "definition_root": "definitions/second",
                    "data_root": "runtime/shared",
                },
            },
        },
    )

    with pytest.raises(ValueError, match="already assigned"):
        YamlWorkspaceLocationResolver.from_config_path(config_path)


def test_yaml_resolver_rejects_missing_configured_root(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {
                "missing": {"placement": "portable", "workspace_root": "missing_workspace"}
            },
        },
    )
    resolver = YamlWorkspaceLocationResolver.from_config_path(config_path)

    with pytest.raises(RuntimeError, match="definition_root is not a directory"):
        resolver.resolve("missing")


def test_portable_resolver_allows_data_root_to_be_created_by_first_run(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    workspace_root = tmp_path / "portable"
    workspace_root.mkdir()
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {"portable": {"placement": "portable", "workspace_root": "portable"}},
        },
    )

    location = YamlWorkspaceLocationResolver.from_config_path(config_path).resolve("portable")

    assert location.data_root == workspace_root / "data"
    assert not location.data_root.exists()


def test_managed_resolver_requires_data_root_mount_to_exist(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    (tmp_path / "definitions").mkdir()
    _write_yaml(
        config_path,
        {
            "schema_version": 1,
            "workspaces": {
                "managed": {
                    "placement": "managed",
                    "definition_root": "definitions",
                    "data_root": "unavailable-data",
                }
            },
        },
    )
    resolver = YamlWorkspaceLocationResolver.from_config_path(config_path)

    with pytest.raises(RuntimeError, match="data_root is not available"):
        resolver.resolve("managed")


def test_yaml_resolver_rejects_unknown_schema_version(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    _write_yaml(config_path, {"schema_version": 2, "workspaces": {}})

    with pytest.raises(ValueError, match="Unsupported workspace configuration schema_version"):
        YamlWorkspaceLocationResolver.from_config_path(config_path)


def test_explicit_config_path_has_highest_priority(tmp_path: Path) -> None:
    explicit_path = tmp_path / "explicit.yaml"
    environment_path = tmp_path / "environment.yaml"
    explicit_path.touch()
    environment_path.touch()

    resolved = resolve_workspaces_config_path(
        explicit_config_path=explicit_path,
        environment_config_path=str(environment_path),
        runtime_environment=RuntimeEnvironment.PRODUCTION,
        working_directory=tmp_path,
    )

    assert resolved == explicit_path.resolve()


def test_selection_allows_a_new_explicit_config_file(tmp_path: Path) -> None:
    config_path = tmp_path / "new" / "workspaces.yaml"

    selected = select_workspaces_config_path(
        explicit_config_path=config_path,
        environment_config_path=None,
        runtime_environment=RuntimeEnvironment.PRODUCTION,
        working_directory=tmp_path,
    )

    assert selected == config_path.resolve()
    assert not selected.exists()


def test_resolution_requires_the_selected_config_file_to_exist(tmp_path: Path) -> None:
    config_path = tmp_path / "new" / "workspaces.yaml"

    with pytest.raises(RuntimeConfigError, match="Workspace configuration file not found"):
        resolve_workspaces_config_path(
            explicit_config_path=config_path,
            environment_config_path=None,
            runtime_environment=RuntimeEnvironment.PRODUCTION,
            working_directory=tmp_path,
        )


def test_environment_config_path_is_used_when_explicit_path_is_absent(tmp_path: Path) -> None:
    config_path = tmp_path / "environment.yaml"
    config_path.touch()

    resolved = resolve_workspaces_config_path(
        explicit_config_path=None,
        environment_config_path="environment.yaml",
        runtime_environment=RuntimeEnvironment.PRODUCTION,
        working_directory=tmp_path,
    )

    assert resolved == config_path.resolve()


def test_development_uses_working_directory_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    config_path.touch()

    resolved = resolve_workspaces_config_path(
        explicit_config_path=None,
        environment_config_path=None,
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
        working_directory=tmp_path,
    )

    assert resolved == config_path.resolve()


def test_production_requires_explicit_or_environment_configuration(tmp_path: Path) -> None:
    with pytest.raises(RuntimeConfigError, match="Production workspace configuration"):
        resolve_workspaces_config_path(
            explicit_config_path=None,
            environment_config_path=None,
            runtime_environment=RuntimeEnvironment.PRODUCTION,
            working_directory=tmp_path,
        )


def test_blank_environment_configuration_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(RuntimeConfigError, match="must not be blank"):
        resolve_workspaces_config_path(
            explicit_config_path=None,
            environment_config_path="   ",
            runtime_environment=RuntimeEnvironment.DEVELOPMENT,
            working_directory=tmp_path,
        )


def test_builder_returns_configured_yaml_adapter(tmp_path: Path) -> None:
    config_path, workspace_root = _portable_config(tmp_path)

    resolver = build_workspace_location_resolver(
        explicit_config_path=config_path,
        environment_config_path=None,
        runtime_environment=RuntimeEnvironment.PRODUCTION,
        working_directory=tmp_path,
    )

    location = resolver.resolve("example")
    assert location.definition_root == workspace_root.resolve()
    assert location.data_root == (workspace_root / "data").resolve()
