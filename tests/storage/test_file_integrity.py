"""Tests for immutable workspace-file verification."""

from __future__ import annotations

import hashlib
from pathlib import Path

from metrka_core.storage.file_integrity import (
    FileIntegrityExpectation,
    FileIntegrityFailureCode,
    FileIntegrityStatus,
    Sha256WorkspaceFileIntegrityVerifier,
)


def _expectation(*, file_path: str, checksum: str) -> FileIntegrityExpectation:
    return FileIntegrityExpectation(
        artifact_kind="silver_manifest",
        owner_id="publication-1",
        file_path=file_path,
        expected_checksum=checksum,
    )


def test_workspace_file_integrity_accepts_matching_sha256(tmp_path: Path) -> None:
    path = tmp_path / "data" / "manifest.json"
    path.parent.mkdir(parents=True)
    content = b'{"dataset_id":"example.data"}\n'
    path.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()

    result = Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path).inspect(
        _expectation(file_path="data/manifest.json", checksum=f"sha256:{expected}")
    )

    assert result.status is FileIntegrityStatus.PASSED
    assert result.actual_checksum == f"sha256:{expected}"
    assert result.failure_codes == ()


def test_workspace_file_integrity_rejects_changed_file(tmp_path: Path) -> None:
    path = tmp_path / "data" / "manifest.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"changed")

    result = Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path).inspect(
        _expectation(file_path="data/manifest.json", checksum=f"sha256:{'1' * 64}")
    )

    assert result.status is FileIntegrityStatus.FAILED
    assert result.failure_codes == (FileIntegrityFailureCode.CHECKSUM_MISMATCH,)


def test_workspace_file_integrity_rejects_path_escape(tmp_path: Path) -> None:
    result = Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path).inspect(
        _expectation(file_path="../outside.json", checksum=f"sha256:{'1' * 64}")
    )

    assert result.status is FileIntegrityStatus.FAILED
    assert result.failure_codes == (FileIntegrityFailureCode.INVALID_PATH,)


def test_workspace_file_integrity_rejects_bare_digest_in_checksum_field(tmp_path: Path) -> None:
    path = tmp_path / "data" / "manifest.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"content")

    result = Sha256WorkspaceFileIntegrityVerifier(workspace_root=tmp_path).inspect(
        _expectation(file_path="data/manifest.json", checksum="1" * 64)
    )

    assert result.status is FileIntegrityStatus.FAILED
    assert result.failure_codes == (FileIntegrityFailureCode.INVALID_EXPECTED_CHECKSUM,)
