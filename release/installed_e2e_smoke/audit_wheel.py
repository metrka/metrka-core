"""Verify release-critical contents of one built Metrka wheel."""

from __future__ import annotations

import hashlib
import sys
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import NoReturn
from zipfile import ZipFile

_REQUIRED_LICENSE_EXPRESSION = "Apache-2.0"
_REQUIRED_LICENSE_FILE = "LICENSE"
_APACHE_LICENSE_TEXT_SHA256 = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"


def _fail(message: str) -> NoReturn:
    raise SystemExit(message)


def _core_metadata_version(value: str | None) -> tuple[int, int]:
    if value is None:
        _fail("wheel metadata is missing Metadata-Version")

    parts = value.split(".")

    if len(parts) != 2 or any(not part.isdigit() for part in parts):
        _fail(f"wheel contains invalid Metadata-Version: {value!r}")

    return int(parts[0]), int(parts[1])


def _normalized_text_sha256(payload: bytes, *, path: str) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        _fail(f"wheel license is not valid UTF-8: {path}: {error}")

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")

    if not normalized.strip():
        _fail(f"wheel license file is empty: {path}")

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def audit_wheel(wheel_path: Path) -> None:
    """Fail unless the wheel contains verified provenance and legal metadata."""

    if not wheel_path.is_file():
        _fail(f"wheel does not exist: {wheel_path}")

    with ZipFile(wheel_path) as archive:
        archive_names = archive.namelist()
        names = set(archive_names)
        metadata_paths = sorted(
            name for name in archive_names if name.endswith(".dist-info/METADATA")
        )

        if len(metadata_paths) != 1:
            _fail(
                "wheel must contain exactly one .dist-info/METADATA file; "
                f"found {len(metadata_paths)}"
            )

        metadata_path = metadata_paths[0]
        metadata = BytesParser(policy=policy.default).parsebytes(archive.read(metadata_path))
        metadata_version = _core_metadata_version(metadata.get("Metadata-Version"))

        if metadata_version < (2, 4):
            _fail(
                "wheel metadata must use Core Metadata 2.4 or newer for PEP 639; "
                f"received {metadata.get('Metadata-Version')!r}"
            )

        license_expression = metadata.get("License-Expression")

        if license_expression != _REQUIRED_LICENSE_EXPRESSION:
            _fail(
                "wheel License-Expression must be "
                f"{_REQUIRED_LICENSE_EXPRESSION!r}; received {license_expression!r}"
            )

        license_files = [str(value) for value in metadata.get_all("License-File", [])]

        if license_files != [_REQUIRED_LICENSE_FILE]:
            _fail(
                "wheel must declare exactly one License-File named LICENSE; "
                f"received {license_files!r}"
            )

        dist_info_directory = metadata_path.removesuffix("/METADATA")
        wheel_license_path = f"{dist_info_directory}/licenses/{_REQUIRED_LICENSE_FILE}"

        if wheel_license_path not in names:
            _fail(f"wheel is missing declared license payload: {wheel_license_path}")

        license_hash = _normalized_text_sha256(
            archive.read(wheel_license_path), path=wheel_license_path
        )

        if license_hash != _APACHE_LICENSE_TEXT_SHA256:
            _fail(
                "wheel license does not match the standard Apache License 2.0 text: "
                f"sha256={license_hash}"
            )

    provenance_path = "metrka_core/_build_provenance.json"

    if provenance_path not in names:
        _fail(f"wheel is missing {provenance_path}")

    development_modules = sorted(
        name for name in names if "/_scratch/" in name or "/_docs/" in name
    )

    if development_modules:
        formatted_names = "\n".join(f"- {name}" for name in development_modules)
        _fail(f"wheel contains excluded development modules:\n{formatted_names}")


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: audit_wheel.py <wheel-path>")

    wheel_path = Path(sys.argv[1]).resolve()
    audit_wheel(wheel_path)
    print(f"Wheel audit passed: {wheel_path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
