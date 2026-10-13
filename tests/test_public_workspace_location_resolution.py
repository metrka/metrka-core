"""Behavioral tests for public workspace-location resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.api import (
    RuntimeEnvironment,
    WorkspacePlacement,
    create_workspace_location_resolver,
)
from metrka_core.pipeline.config import RuntimeConfigError


def _write_config(path: Path, workspaces: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({"schema_version": 1, "workspaces": workspaces}, sort_keys=False),
        encoding="utf-8",
    )


def _write_portable_config(
    path: Path, *, workspace_name: str = "example", workspace_root: str = "workspace"
) -> Path:
    resolved_workspace_root = (path.parent / workspace_root).resolve()
    resolved_workspace_root.mkdir(parents=True, exist_ok=True)
    _write_config(
        path, {workspace_name: {"placement": "portable", "workspace_root": workspace_root}}
    )
    return resolved_workspace_root


def test_public_factory_prefers_explicit_config_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    explicit_path = tmp_path / "explicit" / "workspaces.yaml"
    environment_path = tmp_path / "environment" / "workspaces.yaml"
    explicit_root = _write_portable_config(explicit_path, workspace_root="explicit-workspace")
    _write_portable_config(
        environment_path, workspace_name="environment", workspace_root="environment-workspace"
    )
    monkeypatch.setenv("METRKA_WORKSPACES_CONFIG_PATH", str(environment_path))

    resolver = create_workspace_location_resolver(
        workspaces_config_path=explicit_path, runtime_environment=RuntimeEnvironment.PRODUCTION
    )

    assert resolver.resolve("example").definition_root == explicit_root


def test_public_factory_uses_environment_config_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "environment" / "workspaces.yaml"
    workspace_root = _write_portable_config(config_path)
    monkeypatch.setenv("METRKA_WORKSPACES_CONFIG_PATH", str(config_path))

    resolver = create_workspace_location_resolver(runtime_environment=RuntimeEnvironment.PRODUCTION)

    assert resolver.resolve("example").definition_root == workspace_root


def test_public_factory_resolves_portable_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.yaml"
    workspace_root = _write_portable_config(config_path)

    location = create_workspace_location_resolver(
        workspaces_config_path=config_path, runtime_environment=RuntimeEnvironment.PRODUCTION
    ).resolve("example")

    assert location.placement is WorkspacePlacement.PORTABLE
    assert location.workspace_root == workspace_root
    assert location.definition_root == workspace_root
    assert location.data_root == workspace_root / "data"


def test_public_factory_resolves_managed_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "configuration" / "workspaces.yaml"
    definition_root = tmp_path / "definitions" / "example"
    data_root = tmp_path / "data" / "example"
    definition_root.mkdir(parents=True)
    data_root.mkdir(parents=True)
    _write_config(
        config_path,
        {
            "example": {
                "placement": "managed",
                "definition_root": "../definitions/example",
                "data_root": "../data/example",
            }
        },
    )

    location = create_workspace_location_resolver(
        workspaces_config_path=config_path, runtime_environment=RuntimeEnvironment.PRODUCTION
    ).resolve("example")

    assert location.placement is WorkspacePlacement.MANAGED
    assert location.workspace_root is None
    assert location.definition_root == definition_root.resolve()
    assert location.data_root == data_root.resolve()


def test_public_factory_resolver_rejects_unknown_workspace(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.yaml"
    _write_portable_config(config_path)
    resolver = create_workspace_location_resolver(
        workspaces_config_path=config_path, runtime_environment=RuntimeEnvironment.PRODUCTION
    )

    with pytest.raises(KeyError, match="Unknown workspace"):
        resolver.resolve("unknown")


def test_public_factory_rejects_missing_configuration(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.yaml"

    with pytest.raises(RuntimeConfigError, match="Workspace configuration file not found"):
        create_workspace_location_resolver(
            workspaces_config_path=missing_path, runtime_environment=RuntimeEnvironment.DEVELOPMENT
        )


def test_public_factory_rejects_production_without_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("METRKA_WORKSPACES_CONFIG_PATH", raising=False)

    with pytest.raises(RuntimeConfigError, match="Production workspace configuration"):
        create_workspace_location_resolver(runtime_environment=RuntimeEnvironment.PRODUCTION)


def test_public_factory_uses_development_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "workspaces.local.yaml"
    workspace_root = _write_portable_config(config_path)
    monkeypatch.delenv("METRKA_WORKSPACES_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)

    location = create_workspace_location_resolver(
        runtime_environment=RuntimeEnvironment.DEVELOPMENT
    ).resolve("example")

    assert location.definition_root == workspace_root
