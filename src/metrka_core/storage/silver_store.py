"""Storage boundary for Silver artifacts."""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metrka_core.pipeline.silver.artifact_models import (
    SilverArtifactDeletionError,
    SilverArtifactRef,
    SilverBuildArtifactDeletionResult,
    SilverBuildArtifactQuery,
    SilverBuildRef,
)
from metrka_core.storage.atomic_writes import atomic_copy_file, atomic_write_text
from metrka_core.storage.naming import pointer_file_name
from metrka_core.storage.path_segments import require_path_segment
from metrka_core.storage.publication_manifest_reader import LocalPublicationManifestReader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalSilverArtifactStore:
    """Store Silver artifacts on a local filesystem."""

    workspace_root: Path
    silver_root: Path
    current_root: Path

    def __post_init__(self) -> None:
        normalized_roots: dict[str, Path] = {}

        for field_name in ("workspace_root", "silver_root", "current_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            normalized_roots[field_name] = value.expanduser().resolve()

        workspace_root = normalized_roots["workspace_root"]

        for field_name in ("silver_root", "current_root"):
            try:
                normalized_roots[field_name].relative_to(workspace_root)
            except ValueError as error:
                raise ValueError(f"{field_name} must be inside workspace_root") from error

        for field_name, value in normalized_roots.items():
            object.__setattr__(self, field_name, value)

    def staging_file_stem(self, *, run_id: str, artifact: SilverArtifactRef) -> Path:
        """Resolve one immutable Silver staging file stem."""

        normalized_run_id = require_path_segment(run_id, "run_id")

        return (
            self.silver_root
            / "staging"
            / normalized_run_id
            / artifact.dataset_id
            / artifact.table_key
            / (f"{artifact.partition_key}={artifact.partition_value}")
            / (f"silver_build_id={artifact.silver_build_id}")
            / "data"
        )

    def transformation_details_path(
        self, *, artifact: SilverArtifactRef, transformation_impact_id: str
    ) -> Path:
        """Resolve detailed transformation evidence path."""

        normalized_impact_id = require_path_segment(
            transformation_impact_id, "transformation_impact_id"
        )

        return (
            self.silver_root
            / "transformation_impacts"
            / artifact.table_key
            / (f"{artifact.partition_key}={artifact.partition_value}")
            / (f"silver_build_id={artifact.silver_build_id}")
            / f"{normalized_impact_id}.parquet"
        )

    def commit_staged_files(
        self, *, run_id: str, dataset_id: str, staged_files: list[Path]
    ) -> list[Path]:
        """Copy staged files into immutable Silver table storage."""

        staging_dataset_dir = (
            self.silver_root
            / "staging"
            / require_path_segment(run_id, "run_id")
            / require_path_segment(dataset_id, "dataset_id")
        ).resolve()

        tables_root = (self.silver_root / "tables").resolve()

        committed_files: list[Path] = []

        for staged_file in staged_files:
            source_path = staged_file.resolve()

            if not source_path.is_file():
                raise FileNotFoundError(f"Staged Silver file does not exist: {source_path}")

            try:
                relative_table_path = source_path.relative_to(staging_dataset_dir)
            except ValueError as error:
                raise ValueError(
                    "Staged Silver file is outside the "
                    "expected dataset staging directory: "
                    f"{source_path}"
                ) from error

            target_path = (tables_root / relative_table_path).resolve()

            try:
                target_path.relative_to(tables_root)
            except ValueError as error:
                raise ValueError(
                    f"Resolved Silver target is outside table storage: {target_path}"
                ) from error

            target_path.parent.mkdir(parents=True, exist_ok=True)

            atomic_copy_file(source_path, target_path)

            committed_files.append(target_path)

        return committed_files

    def cleanup_staging(self, *, run_id: str, dataset_id: str) -> None:
        """Remove successful dataset staging files."""

        staging_dataset_dir = (
            self.silver_root
            / "staging"
            / require_path_segment(run_id, "run_id")
            / require_path_segment(dataset_id, "dataset_id")
        )

        if not staging_dataset_dir.exists():
            return

        try:
            shutil.rmtree(staging_dataset_dir)
        except OSError as error:
            logger.warning(
                "Could not remove Silver staging directory %s: %s", staging_dataset_dir, error
            )

    def write_latest_pointer(self, *, dataset_id: str, payload: dict[str, Any]) -> Path:
        """Atomically write one Silver latest pointer."""

        normalized_dataset_id = require_path_segment(dataset_id, "dataset_id")

        if not isinstance(payload, dict):
            raise TypeError("Silver latest pointer payload must be a dictionary")

        if payload.get("dataset_id") != normalized_dataset_id:
            raise ValueError(
                "Silver latest pointer payload dataset_id does not match the requested dataset_id"
            )

        pointer_path = self._latest_pointer_path(normalized_dataset_id)

        atomic_write_text(pointer_path, json.dumps(payload, indent=4, ensure_ascii=False))

        return pointer_path

    def _latest_pointer_path(self, dataset_id: str) -> Path:
        return self.current_root / "latest" / "silver" / pointer_file_name(dataset_id)

    @property
    def tables_root(self) -> Path:
        """Return local durable Silver table storage."""

        return self.silver_root / "tables"

    def write_manifest(self, *, build: SilverBuildRef, payload: dict[str, Any]) -> Path:
        """Write one Silver build manifest."""

        if payload.get("dataset_id") != build.dataset_id:
            raise ValueError("Manifest dataset_id does not match SilverBuildRef")

        if payload.get("silver_build_id") != build.silver_build_id:
            raise ValueError("Manifest silver_build_id does not match SilverBuildRef")

        manifest_path = (
            self.silver_root
            / "manifests"
            / build.dataset_id
            / (f"{build.partition_key}={build.partition_value}")
            / (f"silver_build_id={build.silver_build_id}")
            / "manifest.json"
        )

        atomic_write_text(manifest_path, json.dumps(payload, indent=4, ensure_ascii=False))

        return manifest_path

    def read_manifest(self, *, path: str) -> dict[str, Any]:
        """Read one immutable Silver build manifest."""

        return LocalPublicationManifestReader(
            data_root=self.workspace_root, manifests_root=self.silver_root / "manifests"
        ).read_manifest(path=path)

    def list_build_artifact_directories(
        self, *, builds: Collection[SilverBuildArtifactQuery] | None = None
    ) -> dict[str, tuple[Path, ...]]:
        """List local artifact directories by Silver build."""

        if builds is not None and not builds:
            return {}

        if builds is not None:
            return self._list_requested_build_artifact_directories(builds=builds)

        grouped: dict[str, set[Path]] = {}

        for artifact_root in self._artifact_roots():
            if not artifact_root.exists():
                continue

            for build_directory in artifact_root.rglob("silver_build_id=*"):
                if not build_directory.is_dir():
                    continue

                build_id = build_directory.name.removeprefix("silver_build_id=")

                if not build_id:
                    logger.warning(
                        "Ignored Silver build directory with an empty identifier: %s",
                        build_directory,
                    )
                    continue

                try:
                    normalized_build_id = require_path_segment(build_id, "silver_build_id")
                except ValueError:
                    logger.warning("Ignored invalid Silver build directory: %s", build_directory)
                    continue

                grouped.setdefault(normalized_build_id, set()).add(build_directory.resolve())

        return self._freeze_build_directories(grouped)

    def _list_requested_build_artifact_directories(
        self, *, builds: Collection[SilverBuildArtifactQuery]
    ) -> dict[str, tuple[Path, ...]]:
        grouped: dict[str, set[Path]] = {}

        for build in builds:
            if build.partition_key is None or build.partition_value is None:
                self._find_unpartitioned_build_directories(build=build, grouped=grouped)
                continue

            partition_directory = f"{build.partition_key}={build.partition_value}"
            build_directory = f"silver_build_id={build.silver_build_id}"
            manifest_directory = (
                self.silver_root
                / "manifests"
                / build.dataset_id
                / partition_directory
                / build_directory
            )
            self._record_existing_directory(
                grouped=grouped, silver_build_id=build.silver_build_id, directory=manifest_directory
            )

            for artifact_root in (self.tables_root, self.silver_root / "transformation_impacts"):
                if not artifact_root.exists():
                    continue

                for table_directory in artifact_root.iterdir():
                    if not table_directory.is_dir():
                        continue

                    self._record_existing_directory(
                        grouped=grouped,
                        silver_build_id=build.silver_build_id,
                        directory=(table_directory / partition_directory / build_directory),
                    )

        return self._freeze_build_directories(grouped)

    def _find_unpartitioned_build_directories(
        self, *, build: SilverBuildArtifactQuery, grouped: dict[str, set[Path]]
    ) -> None:
        directory_name = f"silver_build_id={build.silver_build_id}"

        for artifact_root in self._artifact_roots():
            if not artifact_root.exists():
                continue

            for directory in artifact_root.rglob(directory_name):
                self._record_existing_directory(
                    grouped=grouped, silver_build_id=build.silver_build_id, directory=directory
                )

    @staticmethod
    def _record_existing_directory(
        *, grouped: dict[str, set[Path]], silver_build_id: str, directory: Path
    ) -> None:
        if directory.is_dir():
            grouped.setdefault(silver_build_id, set()).add(directory.resolve())

    @staticmethod
    def _freeze_build_directories(grouped: dict[str, set[Path]]) -> dict[str, tuple[Path, ...]]:
        return {
            silver_build_id: tuple(sorted(paths))
            for silver_build_id, paths in sorted(grouped.items())
        }

    def _artifact_roots(self) -> tuple[Path, ...]:
        return (
            self.tables_root.resolve(),
            (self.silver_root / "manifests").resolve(),
            (self.silver_root / "transformation_impacts").resolve(),
        )

    def delete_build_artifact_directories(
        self, *, silver_build_id: str, artifact_directories: Collection[Path] | None = None
    ) -> SilverBuildArtifactDeletionResult:
        """Delete local artifacts while preserving per-directory failures."""

        normalized_build_id = require_path_segment(silver_build_id, "silver_build_id")

        artifact_roots = self._artifact_roots()
        directories = tuple(artifact_directories or ())

        if artifact_directories is None:
            directories = self.list_build_artifact_directories().get(normalized_build_id, ())

        validated_directories = tuple(
            self._validate_deletion_directory(
                directory=directory,
                artifact_roots=artifact_roots,
                silver_build_id=normalized_build_id,
            )
            for directory in directories
        )

        deleted: list[Path] = []
        errors: list[SilverArtifactDeletionError] = []

        for resolved_directory in validated_directories:
            try:
                shutil.rmtree(resolved_directory)
            except FileNotFoundError:
                deleted.append(resolved_directory)
            except OSError as error:
                errors.append(
                    SilverArtifactDeletionError(
                        artifact_directory=resolved_directory,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
            else:
                deleted.append(resolved_directory)

        return SilverBuildArtifactDeletionResult(
            silver_build_id=normalized_build_id,
            requested_directories=validated_directories,
            deleted_directories=tuple(deleted),
            errors=tuple(errors),
        )

    @staticmethod
    def _validate_deletion_directory(
        *, directory: Path, artifact_roots: tuple[Path, ...], silver_build_id: str
    ) -> Path:
        resolved_directory = directory.resolve()
        is_inside_artifact_storage = False

        for artifact_root in artifact_roots:
            try:
                resolved_directory.relative_to(artifact_root)
            except ValueError:
                continue

            is_inside_artifact_storage = True
            break

        if not is_inside_artifact_storage:
            raise RuntimeError(
                f"Refusing to delete Silver artifact outside managed storage: {resolved_directory}"
            )

        expected_directory_name = f"silver_build_id={silver_build_id}"

        if resolved_directory.name != expected_directory_name:
            raise RuntimeError(
                f"Refusing to delete unexpected Silver artifact directory: {resolved_directory}"
            )

        return resolved_directory

    def write_latest_view(self, *, table_key: str, publication_id: str, content: str) -> Path:
        """Write a current view without replacing another publication's view."""

        normalized_table_key = require_path_segment(table_key, "table_key")
        normalized_publication_id = require_path_segment(publication_id, "publication_id")

        view_path = (
            self.silver_root
            / "views"
            / normalized_table_key
            / f"publication={normalized_publication_id}"
            / "latest.sql"
        )

        atomic_write_text(view_path, content)

        return view_path

    def write_history_view(self, *, table_key: str, content: str) -> Path:
        """Write the regenerated history view for one table."""

        normalized_table_key = require_path_segment(table_key, "table_key")

        view_path = self.silver_root / "views" / normalized_table_key / "history.sql"

        atomic_write_text(view_path, content)

        return view_path

    def table_relative_path(self, path: str | Path) -> Path:
        """Return a path relative to Silver tables."""

        resolved_path = Path(path).expanduser().resolve()

        try:
            return resolved_path.relative_to(self.tables_root.resolve())
        except ValueError as error:
            raise ValueError(
                f"Silver table artifact is outside table storage: {resolved_path}"
            ) from error

    def resolve_publication_asset_path(self, file_path: str) -> Path:
        """Resolve a workspace-relative publication path without allowing escape."""

        if not file_path.strip():
            raise ValueError("Publication asset path must not be empty")

        relative_path = Path(file_path)

        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("Publication asset path must be a safe relative path")

        resolved_path = (self.workspace_root / relative_path).resolve()

        try:
            resolved_path.relative_to(self.tables_root.resolve())
        except ValueError as error:
            raise ValueError(
                f"Publication asset is outside Silver table storage: {file_path}"
            ) from error

        return resolved_path

    def relative_path(self, path: str | Path) -> str:
        """Return a path relative to the workspace."""

        resolved_path = Path(path).expanduser().resolve()

        try:
            relative_path = resolved_path.relative_to(self.workspace_root)
        except ValueError as error:
            raise ValueError(
                f"Silver artifact path is outside workspace root: {resolved_path}"
            ) from error

        return relative_path.as_posix()
