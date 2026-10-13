"""YAML-backed adapter for resolving explicit workspace placements."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

import yaml

from metrka_core.datasets.dataset_identity import parse_dataset_id
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement

WORKSPACE_CONFIG_SCHEMA_VERSION: Final = 1
DATASET_FOLDERS_LAYOUT: Final = "dataset_folders"


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise RuntimeError(f"Missing workspace configuration file: {path}")

    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as error:
        raise RuntimeError(f"Invalid YAML in {path}") from error

    if not isinstance(loaded, dict):
        raise ValueError(f"Workspace configuration root must be a mapping: {path}")

    if not all(isinstance(key, str) for key in loaded):
        raise ValueError(f"Workspace configuration keys must be strings: {path}")

    return dict(loaded)


def _resolve_path(raw_path: object, *, field_name: str, config_path: Path) -> Path:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError(
            f"Workspace field {field_name!r} in {config_path} must be a non-empty path string"
        )

    candidate = Path(raw_path.strip()).expanduser()

    if not candidate.is_absolute():
        candidate = config_path.parent / candidate

    return candidate.resolve()


def _require_exact_keys(
    entry: Mapping[object, object],
    *,
    expected: set[str],
    workspace_name: str,
    config_path: Path,
    optional: frozenset[str] = frozenset(),
) -> None:
    if not all(isinstance(key, str) for key in entry):
        raise ValueError(
            f"Workspace {workspace_name!r} in {config_path} contains a non-string field name"
        )

    actual = {str(key) for key in entry}
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected - optional)

    if not missing and not unexpected:
        return

    details: list[str] = []

    if missing:
        details.append(f"missing={missing}")

    if unexpected:
        details.append(f"unexpected={unexpected}")

    raise ValueError(
        f"Workspace {workspace_name!r} has invalid placement fields: {', '.join(details)}"
    )


def load_workspace_locations(config_path: str | Path) -> dict[str, WorkspaceLocation]:
    """Parse and validate every placement declared in one configuration file."""

    resolved_config_path = Path(config_path).expanduser().resolve()
    config = _load_yaml_mapping(resolved_config_path)

    if set(config) != {"schema_version", "workspaces"}:
        raise ValueError(
            f"{resolved_config_path} must contain only 'schema_version' and 'workspaces'"
        )

    schema_version = config["schema_version"]

    if isinstance(schema_version, bool) or schema_version != WORKSPACE_CONFIG_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported workspace configuration schema_version {schema_version!r}; "
            f"expected {WORKSPACE_CONFIG_SCHEMA_VERSION}"
        )

    raw_workspaces = config["workspaces"]

    if not isinstance(raw_workspaces, dict):
        raise ValueError(f"{resolved_config_path} field 'workspaces' must be a mapping")

    locations: dict[str, WorkspaceLocation] = {}
    root_owners: dict[Path, tuple[str, str]] = {}

    for raw_name, raw_entry in raw_workspaces.items():
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ValueError(f"Workspace names in {resolved_config_path} must be non-empty strings")

        workspace_name = raw_name.strip()

        if workspace_name != raw_name:
            raise ValueError(f"Workspace name {raw_name!r} must not contain surrounding spaces")

        if not isinstance(raw_entry, dict):
            raise ValueError(
                f"Workspace {workspace_name!r} in {resolved_config_path} must be a mapping"
            )

        raw_placement = raw_entry.get("placement")

        if not isinstance(raw_placement, str):
            raise ValueError(f"Workspace {workspace_name!r} placement must be a string")

        try:
            placement = WorkspacePlacement(raw_placement)
        except ValueError as error:
            supported = [item.value for item in WorkspacePlacement]
            raise ValueError(
                f"Workspace {workspace_name!r} has unsupported placement {raw_placement!r}; "
                f"supported={supported}"
            ) from error

        if placement is WorkspacePlacement.PORTABLE:
            _require_exact_keys(
                raw_entry,
                expected={"placement", "workspace_root"},
                optional=frozenset({"layout"}),
                workspace_name=workspace_name,
                config_path=resolved_config_path,
            )
            layout = raw_entry.get("layout")

            if layout is not None and layout != DATASET_FOLDERS_LAYOUT:
                raise ValueError(
                    f"Workspace {workspace_name!r} has unsupported layout {layout!r}; "
                    f"supported={[DATASET_FOLDERS_LAYOUT]}"
                )

            location = WorkspaceLocation.portable(
                workspace_name=workspace_name,
                workspace_root=_resolve_path(
                    raw_entry["workspace_root"],
                    field_name="workspace_root",
                    config_path=resolved_config_path,
                ),
                dataset_folders=layout == DATASET_FOLDERS_LAYOUT,
            )
        else:
            _require_exact_keys(
                raw_entry,
                expected={"placement", "definition_root", "data_root"},
                workspace_name=workspace_name,
                config_path=resolved_config_path,
            )
            location = WorkspaceLocation.managed(
                workspace_name=workspace_name,
                definition_root=_resolve_path(
                    raw_entry["definition_root"],
                    field_name="definition_root",
                    config_path=resolved_config_path,
                ),
                data_root=_resolve_path(
                    raw_entry["data_root"], field_name="data_root", config_path=resolved_config_path
                ),
            )

        for root, field_name in (
            (location.definition_root, "definition_root"),
            (location.data_root, "data_root"),
        ):
            owner = root_owners.get(root)

            if owner is not None:
                owner_name, owner_field = owner
                raise ValueError(
                    f"Workspace {workspace_name!r} reuses {field_name} {root} "
                    f"already assigned to {owner_name!r} as {owner_field}"
                )

            root_owners[root] = (workspace_name, field_name)

        locations[workspace_name] = location

    return locations


@dataclass(frozen=True, slots=True)
class YamlWorkspaceLocationResolver:
    """Resolve portable and managed locations from explicit YAML configuration."""

    config_path: Path
    locations: Mapping[str, WorkspaceLocation]

    @classmethod
    def from_config_path(cls, config_path: str | Path) -> YamlWorkspaceLocationResolver:
        resolved_config_path = Path(config_path).expanduser().resolve()
        locations = load_workspace_locations(resolved_config_path)
        return cls(config_path=resolved_config_path, locations=MappingProxyType(locations))

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        if not isinstance(workspace_name, str) or not workspace_name.strip():
            raise ValueError("workspace_name must be a non-empty string")

        normalized_name = workspace_name.strip()
        location = self.locations.get(normalized_name)

        if location is None and "." in normalized_name:
            identity = parse_dataset_id(normalized_name)
            source_name, dataset_name = identity.workspace_name, identity.stream_name
            source = self.locations.get(source_name)

            if source is not None and source.dataset_folders:
                location = source.dataset_folder(dataset_name)

                if not location.definition_root.is_dir():
                    raise RuntimeError(
                        f"Dataset folder {dataset_name!r} does not exist in source "
                        f"{source_name!r}: {location.definition_root}"
                    )

        if location is None:
            raise KeyError(f"Unknown workspace {normalized_name!r} in {self.config_path}")

        if location.dataset_folders:
            raise ValueError(
                f"Workspace {normalized_name!r} keeps each dataset in its own folder; "
                f"name one dataset, for example {normalized_name}.<dataset>"
            )

        if not location.definition_root.is_dir():
            raise RuntimeError(
                f"Workspace {normalized_name!r} definition_root is not a directory: "
                f"{location.definition_root}"
            )

        if location.data_root.exists() and not location.data_root.is_dir():
            raise RuntimeError(
                f"Workspace {normalized_name!r} data_root is not a directory: {location.data_root}"
            )

        if not location.is_portable and not location.data_root.is_dir():
            raise RuntimeError(
                f"Managed workspace {normalized_name!r} data_root is not available: "
                f"{location.data_root}"
            )

        return location

    def resolve_dataset(self, dataset_id: str) -> WorkspaceLocation:
        """Resolve the folder that defines one published dataset (``workspace.stream``)."""

        identity = parse_dataset_id(dataset_id)
        source = self.locations.get(identity.workspace_name)

        if source is not None and source.dataset_folders:
            return self.resolve(identity.dataset_id)

        return self.resolve(identity.workspace_name)
