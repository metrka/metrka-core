"""Workspace-wide audit for Silver artifacts with no database build record."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore


class UnknownArtifactCause(StrEnum):
    """Why an artifact directory cannot be connected to a Silver build."""

    MISSING_BUILD_RECORD = "missing_build_record"
    NOT_A_BUILD_ID = "not_a_build_id"


@dataclass(frozen=True, slots=True)
class UnknownSilverBuildArtifacts:
    """Filesystem artifacts whose dataset ownership cannot be established."""

    artifact_name: str
    silver_build_id: str | None
    artifact_directories: tuple[Path, ...]
    cause: UnknownArtifactCause


@dataclass(frozen=True, slots=True)
class SilverWorkspaceOrphanAudit:
    """Result of one workspace-wide unknown-artifact audit."""

    unknown_builds: tuple[UnknownSilverBuildArtifacts, ...]


class SilverWorkspaceOrphanAuditor:
    """Report unknown Silver artifact directories once per workspace."""

    def __init__(
        self, *, silver_builds: PostgresSilverBuildStore, silver_store: SilverArtifactStore
    ) -> None:
        self._silver_builds = silver_builds
        self._silver_store = silver_store

    def audit(self) -> SilverWorkspaceOrphanAudit:
        """Compare all artifact directories with build records in one batch."""

        build_directories = self._silver_store.list_build_artifact_directories()
        canonical_ids = {
            silver_build_id: _canonical_uuid(silver_build_id)
            for silver_build_id in build_directories
        }
        requested_build_ids = tuple(
            canonical_id for canonical_id in canonical_ids.values() if canonical_id is not None
        )
        known_builds = self._silver_builds.find_by_ids(requested_build_ids)

        unknown_builds = tuple(
            UnknownSilverBuildArtifacts(
                artifact_name=artifact_name,
                silver_build_id=canonical_ids[artifact_name],
                artifact_directories=artifact_directories,
                cause=(
                    UnknownArtifactCause.NOT_A_BUILD_ID
                    if canonical_ids[artifact_name] is None
                    else UnknownArtifactCause.MISSING_BUILD_RECORD
                ),
            )
            for artifact_name, artifact_directories in build_directories.items()
            if canonical_ids[artifact_name] not in known_builds
        )

        return SilverWorkspaceOrphanAudit(unknown_builds=unknown_builds)


def _canonical_uuid(value: str) -> str | None:
    try:
        return str(UUID(value))
    except ValueError:
        return None
