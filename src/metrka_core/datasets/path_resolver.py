"""Port for resolving configured workspace locations."""

from __future__ import annotations

from typing import Protocol

from metrka_core.datasets.workspace_location import WorkspaceLocation


class WorkspaceLocationResolver(Protocol):
    """Resolve one configured workspace to its definition and data roots."""

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        """Resolve a workspace name, or ``source.dataset`` for a dataset folder."""
        ...

    def resolve_dataset(self, dataset_id: str) -> WorkspaceLocation:
        """Resolve the folder that defines one published dataset (``workspace.stream``).

        A malformed ``dataset_id`` raises ``DatasetIdentityError``.
        """
        ...
