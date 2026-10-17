"""Reusable test helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def write_yaml(path: Path, data: Any) -> None:
    """Write YAML."""
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def write_portable_workspaces_config(
    repo_root: Path, *, workspace_name: str, workspace_root: Path
) -> None:
    write_yaml(
        repo_root / "workspaces.local.yaml",
        {
            "schema_version": 1,
            "workspaces": {
                workspace_name: {"placement": "portable", "workspace_root": str(workspace_root)}
            },
        },
    )
