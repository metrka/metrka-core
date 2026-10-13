"""Shared rendering and commit rules for the workspace placement registry."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final

import yaml
from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement
from metrka_core.datasets.yaml_workspace_resolver import (
    WORKSPACE_CONFIG_SCHEMA_VERSION,
    load_workspace_locations,
)
from metrka_core.storage.atomic_writes import atomic_write_text

_WORKSPACE_REGISTRY_LOCK_TIMEOUT_SECONDS: Final = 30.0


def validate_workspace_registration(*, config_path: Path, location: WorkspaceLocation) -> None:
    """Fail early for a registration that conflicts with the current registry snapshot."""

    render_workspace_registration(config_path=config_path, location=location)


@contextmanager
def workspace_registration_transaction(
    *, config_path: Path, location: WorkspaceLocation
) -> Iterator[None]:
    """Serialize the final filesystem commit with a fresh registry read and atomic write."""

    resolved_config_path = config_path.expanduser().resolve()
    resolved_config_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = resolved_config_path.with_name(f"{resolved_config_path.name}.lock")
    lock = FileLock(str(lock_path))

    try:
        lock.acquire(timeout=_WORKSPACE_REGISTRY_LOCK_TIMEOUT_SECONDS)
    except FileLockTimeout as error:
        raise TimeoutError(
            f"Timed out waiting to update the workspace placement registry: {resolved_config_path}"
        ) from error

    try:
        registration_text = render_workspace_registration(
            config_path=resolved_config_path, location=location
        )
        yield
        atomic_write_text(resolved_config_path, registration_text)
    finally:
        lock.release()


def render_workspace_registration(*, config_path: Path, location: WorkspaceLocation) -> str:
    """Return registry content with one new, non-overlapping workspace appended."""

    configured_locations = load_workspace_locations(config_path) if config_path.exists() else {}
    if location.workspace_name in configured_locations:
        raise ValueError(
            f"Workspace {location.workspace_name!r} is already registered in {config_path}"
        )

    _reject_reused_roots(location=location, configured_locations=configured_locations)
    entry = yaml.safe_dump(
        {location.workspace_name: _workspace_entry(location, config_path=config_path)},
        sort_keys=False,
        allow_unicode=True,
    ).rstrip()
    indented_entry = "\n".join(f"  {line}" for line in entry.splitlines())

    if not config_path.exists():
        return f"schema_version: {WORKSPACE_CONFIG_SCHEMA_VERSION}\nworkspaces:\n{indented_entry}\n"

    current = config_path.read_text(encoding="utf-8")
    top_level_statements = [
        line.strip()
        for line in current.splitlines()
        if line.strip() and not line.lstrip().startswith("#") and not line[0].isspace()
    ]
    if top_level_statements != [
        f"schema_version: {WORKSPACE_CONFIG_SCHEMA_VERSION}",
        "workspaces:",
    ]:
        raise ValueError(
            f"{config_path} must use block-style 'schema_version' and 'workspaces:' entries"
        )

    return f"{current.rstrip()}\n{indented_entry}\n"


def _reject_reused_roots(
    *, location: WorkspaceLocation, configured_locations: dict[str, WorkspaceLocation]
) -> None:
    requested_roots = {location.definition_root, location.data_root}

    for name, configured in configured_locations.items():
        configured_roots = {configured.definition_root, configured.data_root}
        reused = requested_roots & configured_roots
        if reused:
            raise ValueError(
                f"Workspace {location.workspace_name!r} reuses roots assigned to {name!r}: "
                f"{sorted(str(path) for path in reused)}"
            )


def _workspace_entry(location: WorkspaceLocation, *, config_path: Path) -> dict[str, str]:
    if location.workspace_root is not None:
        return {
            "placement": WorkspacePlacement.PORTABLE.value,
            "workspace_root": _configured_path(location.workspace_root, config_path=config_path),
        }

    return {
        "placement": WorkspacePlacement.MANAGED.value,
        "definition_root": _configured_path(location.definition_root, config_path=config_path),
        "data_root": _configured_path(location.data_root, config_path=config_path),
    }


def _configured_path(path: Path, *, config_path: Path) -> str:
    try:
        relative = os.path.relpath(path, start=config_path.parent)
    except ValueError:
        return path.as_posix()

    return Path(relative).as_posix()
