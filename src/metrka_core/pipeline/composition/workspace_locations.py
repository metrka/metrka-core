"""Resolve and compose the configured workspace-location adapter."""

from __future__ import annotations

import os
from pathlib import Path

from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver
from metrka_core.pipeline.config import (
    RuntimeConfigError,
    RuntimeEnvironment,
    resolve_runtime_environment,
)

WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE = "METRKA_WORKSPACES_CONFIG_PATH"


def _absolute_path(raw_path: str | Path, *, working_directory: Path) -> Path:
    path = Path(raw_path).expanduser()

    if not path.is_absolute():
        path = working_directory / path

    return path.resolve()


def select_workspaces_config_path(
    *,
    explicit_config_path: str | Path | None,
    environment_config_path: str | None,
    runtime_environment: RuntimeEnvironment,
    working_directory: Path | None = None,
) -> Path:
    """Select workspace placement configuration without requiring it to exist."""

    resolved_working_directory = (
        working_directory if working_directory is not None else Path.cwd()
    ).resolve()

    if explicit_config_path is not None:
        if isinstance(explicit_config_path, str) and not explicit_config_path.strip():
            raise RuntimeConfigError("workspaces_config_path must not be blank")

        candidate = _absolute_path(
            explicit_config_path, working_directory=resolved_working_directory
        )
    elif environment_config_path is not None:
        if not environment_config_path.strip():
            raise RuntimeConfigError(f"{WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE} must not be blank")

        candidate = _absolute_path(
            environment_config_path, working_directory=resolved_working_directory
        )
    elif runtime_environment is RuntimeEnvironment.PRODUCTION:
        raise RuntimeConfigError(
            "Production workspace configuration is missing. Pass "
            "workspaces_config_path or set METRKA_WORKSPACES_CONFIG_PATH."
        )
    else:
        candidate = resolved_working_directory / "workspaces.local.yaml"

    return candidate


def resolve_workspaces_config_path(
    *,
    explicit_config_path: str | Path | None,
    environment_config_path: str | None,
    runtime_environment: RuntimeEnvironment,
    working_directory: Path | None = None,
) -> Path:
    """Select workspace placement configuration and require an existing file."""

    candidate = select_workspaces_config_path(
        explicit_config_path=explicit_config_path,
        environment_config_path=environment_config_path,
        runtime_environment=runtime_environment,
        working_directory=working_directory,
    )

    if not candidate.is_file():
        raise RuntimeConfigError(f"Workspace configuration file not found: {candidate}")

    return candidate


def build_workspace_location_resolver(
    *,
    explicit_config_path: str | Path | None,
    environment_config_path: str | None,
    runtime_environment: RuntimeEnvironment,
    working_directory: Path | None = None,
) -> WorkspaceLocationResolver:
    """Build the YAML adapter selected by runtime configuration."""

    config_path = resolve_workspaces_config_path(
        explicit_config_path=explicit_config_path,
        environment_config_path=environment_config_path,
        runtime_environment=runtime_environment,
        working_directory=working_directory,
    )
    return YamlWorkspaceLocationResolver.from_config_path(config_path)


def create_workspace_location_resolver(
    *,
    workspaces_config_path: str | Path | None = None,
    runtime_environment: RuntimeEnvironment | None = None,
) -> WorkspaceLocationResolver:
    """Create the configured public workspace-location resolver port."""

    resolved_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )
    return build_workspace_location_resolver(
        explicit_config_path=workspaces_config_path,
        environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
        runtime_environment=resolved_environment,
    )
