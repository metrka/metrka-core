"""Resolved filesystem locations for one logical Metrka workspace."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from metrka_core.datasets.dataset_identity import RunName, WorkspaceName

_DATASET_FOLDER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


class WorkspacePlacement(StrEnum):
    """Supported physical placements for one logical workspace."""

    PORTABLE = "portable"
    MANAGED = "managed"


@dataclass(frozen=True, slots=True)
class WorkspaceLocation:
    """Bind one workspace definition to its persistent data storage."""

    workspace_name: WorkspaceName
    definition_root: Path
    data_root: Path
    workspace_root: Path | None = None
    dataset_folders: bool = False
    dataset_name: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.workspace_name, str) or not self.workspace_name.strip():
            raise ValueError("workspace_name must be a non-empty string")

        object.__setattr__(self, "workspace_name", self.workspace_name.strip())

        if self.dataset_folders and self.workspace_root is None:
            raise ValueError("Only a portable workspace can keep its datasets in folders")

        if self.dataset_name is not None:
            if self.dataset_folders:
                raise ValueError("A dataset folder cannot itself contain dataset folders")

            require_dataset_folder_name(self.dataset_name)

        for field_name in ("definition_root", "data_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            object.__setattr__(self, field_name, value.expanduser().resolve())

        if self.definition_root == self.data_root:
            raise ValueError("definition_root and data_root must be different directories")

        if self.workspace_root is None:
            for child, parent in (
                (self.definition_root, self.data_root),
                (self.data_root, self.definition_root),
            ):
                try:
                    child.relative_to(parent)
                except ValueError:
                    continue

                raise ValueError(
                    "Managed definition_root and data_root must not contain one another"
                )

            return

        if not isinstance(self.workspace_root, Path):
            raise TypeError("workspace_root must be a pathlib.Path or None")

        normalized_workspace_root = self.workspace_root.expanduser().resolve()
        object.__setattr__(self, "workspace_root", normalized_workspace_root)

        for field_name in ("definition_root", "data_root"):
            try:
                getattr(self, field_name).relative_to(normalized_workspace_root)
            except ValueError as error:
                raise ValueError(
                    f"{field_name} must be inside workspace_root for a portable workspace"
                ) from error

    @classmethod
    def portable(
        cls, *, workspace_name: str, workspace_root: Path, dataset_folders: bool = False
    ) -> WorkspaceLocation:
        """Create the conventional all-in-one layout used by existing workspaces.

        With ``dataset_folders``, the workspace is a source whose datasets each live in
        their own subfolder with the same all-in-one layout.
        """

        normalized_root = workspace_root.expanduser().resolve()
        return cls(
            workspace_name=WorkspaceName(workspace_name),
            workspace_root=normalized_root,
            definition_root=normalized_root,
            data_root=normalized_root / "data",
            dataset_folders=dataset_folders,
        )

    def dataset_folder(self, dataset_name: str) -> WorkspaceLocation:
        """Return the location of one dataset folder inside this source."""

        if not self.dataset_folders or self.workspace_root is None:
            raise ValueError(f"Workspace {self.workspace_name!r} does not keep datasets in folders")

        folder = self.workspace_root / require_dataset_folder_name(dataset_name)
        return WorkspaceLocation(
            workspace_name=self.workspace_name,
            workspace_root=folder,
            definition_root=folder,
            data_root=folder / "data",
            dataset_name=dataset_name,
        )

    @property
    def name(self) -> RunName:
        """The name used to run or validate this location: ``workspace`` or ``workspace.dataset``."""

        if self.dataset_name is None:
            return RunName(self.workspace_name)

        return RunName(f"{self.workspace_name}.{self.dataset_name}")

    @property
    def glossary_path(self) -> Path:
        """The source's ``glossary.yaml``: beside its dataset folders, or beside ``conf``."""

        if self.dataset_name is not None and self.workspace_root is not None:
            return self.workspace_root.parent / "glossary.yaml"

        return self.definition_root / "glossary.yaml"

    @classmethod
    def managed(
        cls, *, workspace_name: str, definition_root: Path, data_root: Path
    ) -> WorkspaceLocation:
        """Create a layout whose definitions and data may live in separate stores."""

        return cls(
            workspace_name=WorkspaceName(workspace_name),
            definition_root=definition_root,
            data_root=data_root,
        )

    @property
    def is_portable(self) -> bool:
        """Return whether both roots belong to one transferable workspace directory."""

        return self.workspace_root is not None

    @property
    def placement(self) -> WorkspacePlacement:
        """Return the configured physical placement."""

        if self.is_portable:
            return WorkspacePlacement.PORTABLE

        return WorkspacePlacement.MANAGED


def require_dataset_folder_name(dataset_name: str) -> str:
    """A dataset folder name is also the dataset's stream name: letters, digits, ``_`` or ``-``."""

    if not isinstance(dataset_name, str) or _DATASET_FOLDER_NAME.fullmatch(dataset_name) is None:
        raise ValueError(
            f"Dataset folder name {dataset_name!r} must use only letters, digits, '_' or '-'"
        )

    return dataset_name
