"""Tests for the release wheel's provenance and Apache-2.0 audit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AUDIT_SCRIPT = REPOSITORY_ROOT / "release" / "installed_e2e_smoke" / "audit_wheel.py"
DIST_INFO = "metrka_core-1.0.0.dist-info"


def _metadata(
    *,
    metadata_version: str = "2.4",
    license_expression: str | None = "Apache-2.0",
    license_file: str | None = "LICENSE",
) -> str:
    lines = [f"Metadata-Version: {metadata_version}", "Name: metrka-core", "Version: 1.0.0"]

    if license_expression is not None:
        lines.append(f"License-Expression: {license_expression}")

    if license_file is not None:
        lines.append(f"License-File: {license_file}")

    return "\n".join([*lines, "", ""])


def _write_wheel(
    path: Path,
    *,
    metadata: str | None = None,
    license_payload: bytes | None = None,
    include_license_payload: bool = True,
) -> None:
    resolved_metadata = metadata if metadata is not None else _metadata()
    resolved_license = (
        license_payload
        if license_payload is not None
        else (REPOSITORY_ROOT / "LICENSE").read_bytes()
    )

    with ZipFile(path, "w") as archive:
        archive.writestr("metrka_core/_build_provenance.json", "{}")
        archive.writestr(f"{DIST_INFO}/METADATA", resolved_metadata)

        if include_license_payload:
            archive.writestr(f"{DIST_INFO}/licenses/LICENSE", resolved_license)


def _audit(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AUDIT_SCRIPT), str(path)], capture_output=True, text=True, check=False
    )


def test_wheel_audit_accepts_exact_apache_metadata_and_license(tmp_path: Path) -> None:
    wheel = tmp_path / "metrka_core-1.0.0-py3-none-any.whl"
    _write_wheel(wheel)

    result = _audit(wheel)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == f"Wheel audit passed: {wheel.name}\n"


@pytest.mark.parametrize(
    ("metadata", "expected_error"),
    [
        (_metadata(metadata_version="2.3"), "Core Metadata 2.4 or newer"),
        (_metadata(license_expression=None), "License-Expression must be 'Apache-2.0'"),
        (_metadata(license_expression="MIT"), "received 'MIT'"),
        (_metadata(license_file=None), "exactly one License-File named LICENSE"),
    ],
)
def test_wheel_audit_rejects_missing_or_incorrect_license_metadata(
    tmp_path: Path, metadata: str, expected_error: str
) -> None:
    wheel = tmp_path / "invalid-metadata.whl"
    _write_wheel(wheel, metadata=metadata)

    result = _audit(wheel)

    assert result.returncode == 1
    assert expected_error in result.stderr


def test_wheel_audit_rejects_missing_license_payload(tmp_path: Path) -> None:
    wheel = tmp_path / "missing-license.whl"
    _write_wheel(wheel, include_license_payload=False)

    result = _audit(wheel)

    assert result.returncode == 1
    assert "wheel is missing declared license payload" in result.stderr


@pytest.mark.parametrize(
    ("license_payload", "expected_error"),
    [
        (b"", "wheel license file is empty"),
        (b"not the Apache license\n", "does not match the standard Apache License 2.0"),
    ],
)
def test_wheel_audit_rejects_empty_or_modified_license_text(
    tmp_path: Path, license_payload: bytes, expected_error: str
) -> None:
    wheel = tmp_path / "invalid-license.whl"
    _write_wheel(wheel, license_payload=license_payload)

    result = _audit(wheel)

    assert result.returncode == 1
    assert expected_error in result.stderr
