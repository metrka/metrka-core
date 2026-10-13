"""
Secure ZIP extractor.

Unpacks a single ZIP file into a target directory.

Features:
    - Zip-Slip protection: blocks directory  traversal attacks.
    - Smart CDC Extraction: accepts a specific list of files to extract.
"""

from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ZipExtractResult:
    """Immutable result for extracting one ZIP."""

    zip_path: Path
    dest_dir: Path
    expected_count: int = 0
    extracted_count: int = 0
    error: str | None = None
    extracted_files: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.error is None and self.expected_count == self.extracted_count


def _is_safe_member(dest_dir: Path, member_name: str) -> bool:
    """Block zip-slip (absolute paths or traversal outside `dest_dir`)."""
    member_path = Path(member_name)
    if member_path.is_absolute():
        return False

    dest_dir.mkdir(parents=True, exist_ok=True)

    base = dest_dir.resolve(strict=False)
    out = (dest_dir / member_name).resolve(strict=False)

    # ensure resolved output starts with the base directory
    return base == out or base in out.parents


def secure_extract_zip(
    zip_path: str | Path,
    dest_dir: str | Path,
    files: list[str] | None = None,
    *,
    members_to_extract: list[str] | None = None,
    strict: bool = True,
    safe: bool = True,
) -> ZipExtractResult:
    """
    Extract a ZIP file safely.

    If `members_to_extract` is provided, ONLY those specific internal files are extracted.
    If not, extracts all non-directory files.
    """
    zip_path = Path(zip_path).resolve()
    dest_dir = Path(dest_dir).resolve()

    if not zip_path.exists():
        return ZipExtractResult(zip_path, dest_dir, 0, 0, error="ZIP file not found")

    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # determine files we actually care about
            all_members = zf.infolist()
            target_members = []

            for zi in all_members:
                if zi.is_dir():
                    continue

                # if files list provided, ignore files not on the list
                if members_to_extract is not None and zi.filename not in members_to_extract:
                    continue
                target_members.append(zi)

            if members_to_extract is not None:
                found_members = {zi.filename for zi in target_members}
                missing_members = set(members_to_extract) - found_members

                if missing_members and strict:
                    return ZipExtractResult(
                        zip_path=zip_path,
                        dest_dir=dest_dir,
                        expected_count=len(members_to_extract),
                        extracted_count=len(target_members),
                        error=f"Requested ZIP member(s) not found: {sorted(missing_members)}",
                    )

            # safety check(Zip-Slip prevention)
            if safe:
                for z in target_members:
                    if not _is_safe_member(dest_dir, z.filename):
                        raise RuntimeError(
                            f"Unsafe ZIP member path blocked (zip-slip): {z.filename}"
                        )

            # extract
            zf.extractall(path=dest_dir, members=target_members)

            # verify it landed on disk
            expected_count = len(target_members)
            extracted_files = [
                zi.filename for zi in target_members if (dest_dir / zi.filename).exists()
            ]
            extracted_count = len(extracted_files)

            logger.info(
                "Extracted %d/%d files from %s", extracted_count, expected_count, zip_path.name
            )

            return ZipExtractResult(
                zip_path=zip_path,
                dest_dir=dest_dir,
                expected_count=expected_count,
                extracted_count=extracted_count,
                extracted_files=extracted_files,
            )

    except Exception as e:
        logger.exception("Extraction failed for %s", zip_path.name)
        return ZipExtractResult(
            zip_path=zip_path,
            dest_dir=dest_dir,
            expected_count=0,
            extracted_count=0,
            error=f"{type(e).__name__}: {e}",
        )
