"""Storage boundary for Bronze artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from metrka_core.storage.atomic_writes import atomic_copy_file, atomic_write_text
from metrka_core.storage.naming import pointer_file_name
from metrka_core.storage.path_segments import require_path_segment


class BronzeArtifactStore(Protocol):
    """Storage operations required by Bronze processing."""

    def run_dir(self, *, run_id: str) -> Path:
        """Resolve one Bronze run directory."""
        ...

    def prepare_run_dir(self, *, run_id: str) -> Path:
        """Create and return one Bronze run directory."""
        ...

    def copy_into_run(self, *, run_id: str, source_file: Path, file_name: str) -> Path:
        """Copy one source file into a Bronze run."""
        ...

    def write_latest_pointer(self, *, dataset_id: str, payload: dict[str, Any]) -> Path:
        """Atomically write one Bronze latest pointer."""
        ...

    def relative_path(self, path: str | Path) -> str:
        """Return a workspace-relative POSIX path."""
        ...


@dataclass(frozen=True)
class LocalBronzeArtifactStore:
    """Store Bronze artifacts on a local filesystem."""

    workspace_root: Path
    bronze_root: Path
    current_root: Path

    def __post_init__(self) -> None:
        normalized_roots: dict[str, Path] = {}

        for field_name in ("workspace_root", "bronze_root", "current_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            normalized_roots[field_name] = value.expanduser().resolve()

        workspace_root = normalized_roots["workspace_root"]

        for field_name in ("bronze_root", "current_root"):
            try:
                normalized_roots[field_name].relative_to(workspace_root)
            except ValueError as error:
                raise ValueError(f"{field_name} must be inside workspace_root") from error

        for field_name, value in normalized_roots.items():
            object.__setattr__(self, field_name, value)

    def run_dir(self, *, run_id: str) -> Path:
        """Resolve one Bronze run directory."""

        return self.bronze_root / "runs" / require_path_segment(run_id, "run_id")

    def prepare_run_dir(self, *, run_id: str) -> Path:
        """Create one Bronze run directory."""

        run_dir = self.run_dir(run_id=run_id)

        run_dir.mkdir(parents=True, exist_ok=True)

        return run_dir

    def copy_into_run(self, *, run_id: str, source_file: Path, file_name: str) -> Path:
        """Copy one source file into a Bronze run."""

        source_path = source_file.resolve()

        if not source_path.is_file():
            raise FileNotFoundError(f"Bronze source file does not exist: {source_path}")

        normalized_file_name = require_path_segment(file_name, "file_name")

        run_dir = self.prepare_run_dir(run_id=run_id)

        target_path = run_dir / normalized_file_name

        atomic_copy_file(source_path, target_path)

        return target_path

    def write_latest_pointer(self, *, dataset_id: str, payload: dict[str, Any]) -> Path:
        """Atomically write one Bronze latest pointer."""

        normalized_dataset_id = require_path_segment(dataset_id, "dataset_id")

        if not isinstance(payload, dict):
            raise TypeError("Bronze latest pointer payload must be a dictionary")

        if payload.get("dataset_id") != normalized_dataset_id:
            raise ValueError(
                "Bronze latest pointer payload dataset_id does not match the requested dataset_id"
            )

        pointer_path = self.current_root / "latest" / "bronze" / pointer_file_name(dataset_id)

        atomic_write_text(pointer_path, json.dumps(payload, indent=4, ensure_ascii=False))

        return pointer_path

    def relative_path(self, path: str | Path) -> str:
        """Return a path relative to the workspace."""

        resolved_path = Path(path).expanduser().resolve()

        try:
            relative_path = resolved_path.relative_to(self.workspace_root)
        except ValueError as error:
            raise ValueError(
                f"Bronze artifact path is outside workspace root: {resolved_path}"
            ) from error

        return relative_path.as_posix()
