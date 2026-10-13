"""ZIP adapter for writing and verifying customer workspace packages."""

from __future__ import annotations

import shutil
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Final, cast
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile, ZipInfo

from metrka_core.datasets.workspace_export_models import (
    WORKSPACE_EXPORT_MANIFEST_NAME,
    WorkspaceExportFile,
    WorkspaceExportIntegrityError,
    WorkspaceExportManifest,
    WorkspaceExportVerificationResult,
)
from metrka_core.storage.atomic_writes import atomic_write
from metrka_core.storage.checksums import format_sha256_checksum, sha256_file, sha256_stream
from metrka_core.storage.portable_paths import validate_portable_relative_path

_MAX_MANIFEST_SIZE_BYTES: Final = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class WorkspaceExportSourceFile:
    """Source file paired with its already-calculated manifest entry."""

    source_path: Path
    manifest_entry: WorkspaceExportFile


def write_workspace_export(
    *,
    destination: Path,
    manifest: WorkspaceExportManifest,
    source_files: tuple[WorkspaceExportSourceFile, ...],
    overwrite: bool,
) -> WorkspaceExportVerificationResult:
    """Write atomically and verify the temporary ZIP before publication."""

    resolved_destination = destination.expanduser().resolve()
    if resolved_destination.suffix.lower() != ".zip":
        raise ValueError("Workspace export destination must use the .zip extension")
    if resolved_destination.exists() and not overwrite:
        raise FileExistsError(f"Workspace export package already exists: {resolved_destination}")
    if resolved_destination.exists() and not resolved_destination.is_file():
        raise ValueError(f"Workspace export destination is not a file: {resolved_destination}")

    verified: list[WorkspaceExportVerificationResult] = []

    def write_package(temporary_path: Path) -> None:
        with ZipFile(
            temporary_path, "w", compression=ZIP_DEFLATED, compresslevel=6, strict_timestamps=False
        ) as archive:
            manifest_info = ZipInfo(
                f"{manifest.workspace_name}/{WORKSPACE_EXPORT_MANIFEST_NAME}",
                date_time=(1980, 1, 1, 0, 0, 0),
            )
            manifest_info.compress_type = ZIP_DEFLATED
            manifest_info.external_attr = 0o100644 << 16
            archive.writestr(manifest_info, manifest.to_json_bytes())

            for source in source_files:
                archive.write(
                    source.source_path,
                    arcname=f"{manifest.workspace_name}/{source.manifest_entry.path}",
                )

        verified.append(verify_workspace_export(temporary_path))

    atomic_write(resolved_destination, write_package)
    verification = verified[0]
    return WorkspaceExportVerificationResult(
        workspace_name=verification.workspace_name,
        source_placement=verification.source_placement,
        package_path=resolved_destination,
        package_checksum=verification.package_checksum,
        created_at=verification.created_at,
        file_count=verification.file_count,
        total_size_bytes=verification.total_size_bytes,
    )


def verify_workspace_export(package_path: str | Path) -> WorkspaceExportVerificationResult:
    """Verify manifest shape, ZIP membership, sizes, and every payload checksum."""

    resolved_package_path = Path(package_path).expanduser().resolve()
    if not resolved_package_path.is_file():
        raise FileNotFoundError(f"Workspace export package does not exist: {resolved_package_path}")

    try:
        manifest = _verify_archive(resolved_package_path)
    except WorkspaceExportIntegrityError:
        raise
    except (BadZipFile, OSError, RuntimeError, ValueError) as error:
        raise WorkspaceExportIntegrityError(
            f"Could not verify workspace export package: {type(error).__name__}: {error}"
        ) from error

    return WorkspaceExportVerificationResult(
        workspace_name=manifest.workspace_name,
        source_placement=manifest.source_placement,
        package_path=resolved_package_path,
        package_checksum=format_sha256_checksum(sha256_file(resolved_package_path)),
        created_at=manifest.created_at,
        file_count=manifest.file_count,
        total_size_bytes=manifest.total_size_bytes,
    )


def extract_verified_workspace_export(
    package_path: str | Path, destination: str | Path
) -> WorkspaceExportManifest:
    """Verify and safely extract one package into a new portable workspace root."""

    resolved_package_path = Path(package_path).expanduser().resolve()
    if not resolved_package_path.is_file():
        raise FileNotFoundError(f"Workspace export package does not exist: {resolved_package_path}")

    resolved_destination = Path(destination).expanduser().resolve()
    if resolved_destination.exists():
        raise FileExistsError(
            f"Workspace import destination already exists: {resolved_destination}"
        )
    if not resolved_destination.parent.is_dir():
        raise FileNotFoundError(
            f"Workspace import destination parent does not exist: {resolved_destination.parent}"
        )

    try:
        archive = ZipFile(resolved_package_path, "r")
    except (BadZipFile, OSError, RuntimeError, ValueError) as error:
        raise WorkspaceExportIntegrityError(
            f"Could not open workspace export package: {type(error).__name__}: {error}"
        ) from error

    with archive:
        try:
            manifest = _verify_open_archive(archive)
        except WorkspaceExportIntegrityError:
            raise
        except (BadZipFile, OSError, RuntimeError, ValueError) as error:
            raise WorkspaceExportIntegrityError(
                f"Could not verify workspace export package: {type(error).__name__}: {error}"
            ) from error

        resolved_destination.mkdir()
        try:
            manifest_entries = {entry.path: entry for entry in manifest.files}
            for info in archive.infolist():
                archive_path = PurePosixPath(info.filename)
                relative_path = archive_path.relative_to(manifest.workspace_name)
                extracted_path = resolved_destination.joinpath(*relative_path.parts)
                extracted_path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info, "r") as source, extracted_path.open("xb") as target:
                    shutil.copyfileobj(source, target)

                relative_name = relative_path.as_posix()
                if relative_name == WORKSPACE_EXPORT_MANIFEST_NAME:
                    extracted_manifest = WorkspaceExportManifest.from_json_bytes(
                        extracted_path.read_bytes()
                    )
                    if extracted_manifest != manifest:
                        raise WorkspaceExportIntegrityError(
                            "Workspace export manifest changed during extraction"
                        )
                    continue

                entry = manifest_entries[relative_name]
                if extracted_path.stat().st_size != entry.size_bytes:
                    raise WorkspaceExportIntegrityError(
                        f"Extracted workspace size mismatch for {entry.path!r}"
                    )
                actual_checksum = format_sha256_checksum(sha256_file(extracted_path))
                if actual_checksum != entry.checksum:
                    raise WorkspaceExportIntegrityError(
                        f"Extracted workspace checksum mismatch for {entry.path!r}"
                    )
        except BaseException as error:
            try:
                shutil.rmtree(resolved_destination)
            except OSError as cleanup_error:
                error.add_note(
                    f"Workspace import cleanup also failed for {resolved_destination}: "
                    f"{cleanup_error}"
                )
            raise

    return manifest


def _verify_archive(package_path: Path) -> WorkspaceExportManifest:
    with ZipFile(package_path, "r") as archive:
        return _verify_open_archive(archive)


def _verify_open_archive(archive: ZipFile) -> WorkspaceExportManifest:
    infos = archive.infolist()
    if not infos:
        raise WorkspaceExportIntegrityError("Workspace export package is empty")

    names = [info.filename for info in infos]
    if len(names) != len(set(names)) or len(names) != len({name.casefold() for name in names}):
        raise WorkspaceExportIntegrityError(
            "Workspace export package contains colliding ZIP member names"
        )
    for info in infos:
        _validate_zip_member(info)

    manifest_infos = [
        info
        for info in infos
        if PurePosixPath(info.filename).name == WORKSPACE_EXPORT_MANIFEST_NAME
    ]
    if len(manifest_infos) != 1:
        raise WorkspaceExportIntegrityError(
            "Workspace export package must contain exactly one manifest"
        )

    manifest_info = manifest_infos[0]
    if manifest_info.file_size > _MAX_MANIFEST_SIZE_BYTES:
        raise WorkspaceExportIntegrityError("Workspace export manifest is too large")

    manifest = WorkspaceExportManifest.from_json_bytes(archive.read(manifest_info))
    package_root = PurePosixPath(manifest_info.filename).parent
    if package_root.parts != (manifest.workspace_name,):
        raise WorkspaceExportIntegrityError(
            "Workspace export manifest directory does not match workspace_name"
        )

    expected_names = {
        f"{manifest.workspace_name}/{WORKSPACE_EXPORT_MANIFEST_NAME}",
        *(f"{manifest.workspace_name}/{entry.path}" for entry in manifest.files),
    }
    actual_names = set(names)
    if actual_names != expected_names:
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        raise WorkspaceExportIntegrityError(
            "Workspace export ZIP membership does not match its manifest: "
            f"missing={missing}, unexpected={unexpected}"
        )

    info_by_name = {info.filename: info for info in infos}
    for entry in manifest.files:
        info = info_by_name[f"{manifest.workspace_name}/{entry.path}"]
        if info.file_size != entry.size_bytes:
            raise WorkspaceExportIntegrityError(
                f"Workspace export size mismatch for {entry.path!r}: "
                f"expected {entry.size_bytes}, received {info.file_size}"
            )
        with archive.open(info, "r") as stream:
            actual_checksum = format_sha256_checksum(sha256_stream(cast(BinaryIO, stream)))
        if actual_checksum != entry.checksum:
            raise WorkspaceExportIntegrityError(
                f"Workspace export checksum mismatch for {entry.path!r}"
            )

    return manifest


def _validate_zip_member(info: ZipInfo) -> None:
    if info.is_dir():
        raise WorkspaceExportIntegrityError(
            f"Workspace export contains an unexpected directory entry: {info.filename!r}"
        )
    try:
        validate_portable_relative_path(info.filename)
    except ValueError as error:
        raise WorkspaceExportIntegrityError(
            f"Workspace export ZIP member is unsafe: {info.filename!r}: {error}"
        ) from error

    unix_mode = info.external_attr >> 16
    if stat.S_IFMT(unix_mode) == stat.S_IFLNK:
        raise WorkspaceExportIntegrityError(
            f"Workspace export ZIP member must not be a symbolic link: {info.filename!r}"
        )
