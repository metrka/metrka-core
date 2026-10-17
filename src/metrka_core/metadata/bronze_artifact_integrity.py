"""Capture and verify immutable Bronze file manifests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metrka_core.metadata.file_marshal_models import BronzeArtifactDigest
from metrka_core.storage.checksums import sha256_file


class BronzeArtifactIntegrityError(RuntimeError):
    """Raised when physical Bronze files do not match their recorded manifest."""

    def __init__(self, message: str, *, details: dict[str, Any]) -> None:
        super().__init__(message)
        self.details = details


@dataclass(frozen=True, slots=True)
class BronzeArtifactVerification:
    """Successful verification summary for one Bronze run."""

    artifact_count: int
    total_bytes: int


def capture_bronze_artifacts(
    *, bronze_run_dir: Path, output_paths: list[Path]
) -> tuple[BronzeArtifactDigest, ...]:
    """Hash every file produced by one successful Bronze ingestion."""

    root = bronze_run_dir.resolve()

    if not root.is_dir():
        raise FileNotFoundError(f"Bronze run directory does not exist: {root}")

    if not output_paths:
        raise ValueError("Cannot capture an empty Bronze artifact manifest")

    artifacts: list[BronzeArtifactDigest] = []

    for output_path in output_paths:
        if output_path.is_symlink():
            raise ValueError(f"Bronze artifacts must not be symbolic links: {output_path}")

        resolved = output_path.resolve()

        try:
            relative_path = resolved.relative_to(root).as_posix()
        except ValueError as error:
            raise ValueError(
                f"Bronze artifact is outside its run directory: {output_path}"
            ) from error

        if not resolved.is_file():
            raise FileNotFoundError(f"Bronze artifact does not exist: {resolved}")

        artifacts.append(
            BronzeArtifactDigest(
                relative_path=relative_path,
                sha256=sha256_file(resolved),
                size_bytes=resolved.stat().st_size,
            )
        )

    ordered = tuple(sorted(artifacts, key=lambda artifact: artifact.relative_path))
    relative_paths = [artifact.relative_path for artifact in ordered]

    if len(relative_paths) != len(set(relative_paths)):
        raise ValueError("Bronze output_paths contains the same file more than once")

    return ordered


def verify_bronze_artifacts(
    *, bronze_run_dir: Path, expected: tuple[BronzeArtifactDigest, ...]
) -> BronzeArtifactVerification:
    """Fail unless the complete Bronze run directory matches its recorded manifest."""

    root = bronze_run_dir.resolve()

    if not expected:
        raise BronzeArtifactIntegrityError(
            "Bronze artifact manifest is missing",
            details={"failure_code": "BRONZE_ARTIFACT_MANIFEST_MISSING"},
        )

    if not root.is_dir():
        raise BronzeArtifactIntegrityError(
            f"Bronze run directory is missing: {root}",
            details={"failure_code": "BRONZE_RUN_DIRECTORY_MISSING", "bronze_run_path": str(root)},
        )

    expected_by_path = {artifact.relative_path: artifact for artifact in expected}

    if len(expected_by_path) != len(expected):
        raise BronzeArtifactIntegrityError(
            "Bronze artifact manifest contains duplicate relative paths",
            details={"failure_code": "BRONZE_ARTIFACT_MANIFEST_INVALID"},
        )

    actual_by_path = _actual_files(root)
    missing_paths = sorted(set(expected_by_path) - set(actual_by_path))
    unexpected_paths = sorted(set(actual_by_path) - set(expected_by_path))

    size_mismatches: list[dict[str, Any]] = []
    hash_mismatches: list[dict[str, Any]] = []

    for relative_path in sorted(set(expected_by_path) & set(actual_by_path)):
        expected_artifact = expected_by_path[relative_path]
        actual_path = actual_by_path[relative_path]
        actual_size = actual_path.stat().st_size

        if actual_size != expected_artifact.size_bytes:
            size_mismatches.append(
                {
                    "relative_path": relative_path,
                    "expected_size_bytes": expected_artifact.size_bytes,
                    "actual_size_bytes": actual_size,
                }
            )
            continue

        actual_hash = sha256_file(actual_path)

        if actual_hash != expected_artifact.sha256:
            hash_mismatches.append(
                {
                    "relative_path": relative_path,
                    "expected_sha256": expected_artifact.sha256,
                    "actual_sha256": actual_hash,
                }
            )

    if missing_paths or unexpected_paths or size_mismatches or hash_mismatches:
        raise BronzeArtifactIntegrityError(
            "Physical Bronze files do not match the recorded artifact manifest",
            details={
                "failure_code": "BRONZE_ARTIFACT_INTEGRITY_MISMATCH",
                "missing_paths": missing_paths,
                "unexpected_paths": unexpected_paths,
                "size_mismatches": size_mismatches,
                "hash_mismatches": hash_mismatches,
            },
        )

    return BronzeArtifactVerification(
        artifact_count=len(expected), total_bytes=sum(artifact.size_bytes for artifact in expected)
    )


def require_bronze_artifacts_match_source(
    *,
    captured: tuple[BronzeArtifactDigest, ...],
    expected_from_source: tuple[BronzeArtifactDigest, ...],
) -> None:
    """Reject a Bronze copy or extraction that differs from its source bytes."""

    captured_by_path = {artifact.relative_path: artifact for artifact in captured}
    expected_by_path = {artifact.relative_path: artifact for artifact in expected_from_source}

    if captured_by_path == expected_by_path:
        return

    raise BronzeArtifactIntegrityError(
        "Bronze output does not match the bytes fingerprinted in the source asset",
        details={
            "failure_code": "BRONZE_ARTIFACT_SOURCE_MISMATCH",
            "expected": [
                {
                    "relative_path": artifact.relative_path,
                    "sha256": artifact.sha256,
                    "size_bytes": artifact.size_bytes,
                }
                for artifact in sorted(
                    expected_from_source, key=lambda artifact: artifact.relative_path
                )
            ],
            "captured": [
                {
                    "relative_path": artifact.relative_path,
                    "sha256": artifact.sha256,
                    "size_bytes": artifact.size_bytes,
                }
                for artifact in sorted(captured, key=lambda artifact: artifact.relative_path)
            ],
        },
    )


def _actual_files(root: Path) -> dict[str, Path]:
    actual: dict[str, Path] = {}

    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise BronzeArtifactIntegrityError(
                f"Bronze run contains a symbolic link: {path}",
                details={"failure_code": "BRONZE_ARTIFACT_SYMBOLIC_LINK", "path": str(path)},
            )

        if path.is_file():
            actual[path.relative_to(root).as_posix()] = path

    return actual
