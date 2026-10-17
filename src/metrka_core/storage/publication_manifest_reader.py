"""Local-filesystem adapter for immutable publication manifests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from metrka_core.catalog.publication_manifest_reader import (
    PublicationManifestFailure,
    PublicationManifestReader,
    PublicationManifestReadError,
)
from metrka_core.storage.json_object_reader import JsonObjectReadError, LocalJsonObjectReader

_SILVER_MANIFESTS_RELATIVE_ROOT = PurePosixPath("files/silver/manifests")


@dataclass(frozen=True, slots=True)
class LocalPublicationManifestReader:
    """Read publication manifests below one resolved workspace data root."""

    data_root: Path
    manifests_root: Path

    def __post_init__(self) -> None:
        for field_name in ("data_root", "manifests_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            object.__setattr__(self, field_name, value.expanduser().resolve())

    def read_manifest(self, *, path: str) -> dict[str, Any]:
        """Read one manifest without allowing escape from Silver manifest storage."""

        reader = LocalJsonObjectReader(
            data_root=self.data_root,
            storage_root=self.manifests_root,
            artifact_label="Publication manifest",
            storage_label="Silver manifest storage",
        )
        try:
            return reader.read(path=path)
        except JsonObjectReadError as error:
            raise PublicationManifestReadError(
                reason=PublicationManifestFailure(error.reason.value),
                path=error.path,
                message=str(error),
            ) from error


def create_publication_manifest_reader(*, data_root: Path) -> PublicationManifestReader:
    """Create a reader scoped to one workspace's Silver manifest storage."""

    if not isinstance(data_root, Path):
        raise TypeError("data_root must be a pathlib.Path")

    return LocalPublicationManifestReader(
        data_root=data_root,
        manifests_root=data_root.joinpath(*_SILVER_MANIFESTS_RELATIVE_ROOT.parts),
    )
