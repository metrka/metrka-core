"""The one rule that splits a ``dataset_id`` into its workspace and stream names."""

from __future__ import annotations

from dataclasses import dataclass
from typing import NewType

WorkspaceName = NewType("WorkspaceName", str)
"""The source workspace: ``workspace_name`` in main.yaml, the part of a dataset_id before the dot."""

RunName = NewType("RunName", str)
"""What a run or validation names: a workspace, or ``workspace.dataset`` for a dataset folder."""


class DatasetIdentityError(ValueError):
    """A dataset identifier does not follow the ``workspace.stream`` identity rule."""

    def __init__(self, *, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class DatasetIdentity:
    """A canonical dataset identifier and its workspace and stream parts."""

    dataset_id: str
    workspace_name: WorkspaceName
    stream_name: str


def parse_dataset_id(value: object) -> DatasetIdentity:
    """Split ``workspace.stream`` at the last dot; stream names never contain one."""

    if not isinstance(value, str) or not value.strip():
        raise DatasetIdentityError(reason="dataset_id must be a non-empty string")
    if value != value.strip():
        raise DatasetIdentityError(reason="dataset_id must not contain surrounding whitespace")
    if "." not in value:
        raise DatasetIdentityError(reason="dataset_id does not contain a stream name")

    workspace_name, stream_name = value.rsplit(".", 1)
    if not workspace_name or not stream_name:
        raise DatasetIdentityError(reason="dataset_id does not contain both identifiers")
    if any(separator in value for separator in ("/", "\\")):
        raise DatasetIdentityError(reason="dataset_id contains a path separator")

    return DatasetIdentity(
        dataset_id=value, workspace_name=WorkspaceName(workspace_name), stream_name=stream_name
    )
