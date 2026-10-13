"""The one storage port for Silver artifacts, so another store can replace the local disk."""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import Any, Protocol

from metrka_core.catalog.publication_manifest_reader import PublicationManifestReader
from metrka_core.pipeline.silver.artifact_models import (
    SilverArtifactRef,
    SilverBuildArtifactDeletionResult,
    SilverBuildArtifactQuery,
    SilverBuildRef,
)


class SilverArtifactStore(PublicationManifestReader, Protocol):
    """Every Silver artifact operation; ``LocalSilverArtifactStore`` keeps them on local disk."""

    def relative_path(self, path: str | Path) -> str: ...

    # Building one table.
    def staging_file_stem(self, *, run_id: str, artifact: SilverArtifactRef) -> Path: ...

    def transformation_details_path(
        self, *, artifact: SilverArtifactRef, transformation_impact_id: str
    ) -> Path: ...

    # Manifests and making a staged build durable.
    @property
    def tables_root(self) -> Path: ...

    def write_manifest(self, *, build: SilverBuildRef, payload: dict[str, Any]) -> Path: ...

    def table_relative_path(self, path: str | Path) -> Path: ...

    def commit_staged_files(
        self, *, run_id: str, dataset_id: str, staged_files: list[Path]
    ) -> list[Path]: ...

    def cleanup_staging(self, *, run_id: str, dataset_id: str) -> None: ...

    # Publication views and indexes.
    def write_latest_view(self, *, table_key: str, publication_id: str, content: str) -> Path: ...

    def write_history_view(self, *, table_key: str, content: str) -> Path: ...

    def write_latest_pointer(self, *, dataset_id: str, payload: dict[str, Any]) -> Path: ...

    def resolve_publication_asset_path(self, file_path: str) -> Path: ...

    # Reconciling build artifacts with their builds.
    def list_build_artifact_directories(
        self, *, builds: Collection[SilverBuildArtifactQuery] | None = None
    ) -> dict[str, tuple[Path, ...]]: ...

    def delete_build_artifact_directories(
        self, *, silver_build_id: str, artifact_directories: Collection[Path] | None = None
    ) -> SilverBuildArtifactDeletionResult: ...
