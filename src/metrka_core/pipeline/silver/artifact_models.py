"""Value objects shared by Silver artifact ports and adapters."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metrka_core.storage.path_segments import require_path_segment


@dataclass(frozen=True)
class SilverBuildRef:
    """Logical identity of one immutable Silver build."""

    dataset_id: str
    partition_key: str
    partition_value: str
    silver_build_id: str

    def __post_init__(self) -> None:
        for field_name in ("dataset_id", "partition_key", "partition_value", "silver_build_id"):
            object.__setattr__(
                self, field_name, require_path_segment(getattr(self, field_name), field_name)
            )


@dataclass(frozen=True)
class SilverBuildArtifactQuery:
    """Known build identity used to locate its artifact directories."""

    dataset_id: str
    silver_build_id: str
    partition_key: str | None
    partition_value: str | None

    def __post_init__(self) -> None:
        for field_name in ("dataset_id", "silver_build_id"):
            object.__setattr__(
                self, field_name, require_path_segment(getattr(self, field_name), field_name)
            )

        if (self.partition_key is None) != (self.partition_value is None):
            raise ValueError(
                "partition_key and partition_value must either both be set or both be None"
            )

        for field_name in ("partition_key", "partition_value"):
            value = getattr(self, field_name)

            if value is not None:
                object.__setattr__(self, field_name, require_path_segment(value, field_name))


@dataclass(frozen=True, slots=True)
class SilverArtifactDeletionError:
    """One artifact directory that could not be removed."""

    artifact_directory: Path
    error_type: str
    message: str

    def __post_init__(self) -> None:
        if not self.error_type.strip():
            raise ValueError("error_type must not be empty")


@dataclass(frozen=True, slots=True)
class SilverBuildArtifactDeletionResult:
    """Structured result of removing one build's artifact directories."""

    silver_build_id: str
    requested_directories: tuple[Path, ...]
    deleted_directories: tuple[Path, ...]
    errors: tuple[SilverArtifactDeletionError, ...]

    def __post_init__(self) -> None:
        require_path_segment(self.silver_build_id, "silver_build_id")

        requested = set(self.requested_directories)
        deleted = set(self.deleted_directories)
        failed = {error.artifact_directory for error in self.errors}

        if len(requested) != len(self.requested_directories):
            raise ValueError("requested_directories must not contain duplicates")

        if deleted & failed:
            raise ValueError("A directory cannot be both deleted and failed")

        if deleted | failed != requested:
            raise ValueError("Every requested directory must have one deletion outcome")

    @property
    def deleted(self) -> bool:
        """Return whether every requested directory is now absent."""

        return not self.errors


@dataclass(frozen=True)
class SilverArtifactRef:
    """Logical identity of one immutable Silver table build."""

    dataset_id: str
    table_key: str
    partition_key: str
    partition_value: str
    silver_build_id: str

    def __post_init__(self) -> None:
        for field_name in (
            "dataset_id",
            "table_key",
            "partition_key",
            "partition_value",
            "silver_build_id",
        ):
            object.__setattr__(
                self, field_name, require_path_segment(getattr(self, field_name), field_name)
            )
