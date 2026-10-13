"""
Fingerprint ZIP archives and track internal member changes.

Used to determine which internal files (e.g., CSVs) inside ZIP
have changed, preventing redundant extraction and processing.

"""

from __future__ import annotations

import hashlib
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ZipMemberMeta:
    """Fingerprint + size of one file inside the ZIP."""

    name: str
    sha256: str
    size: int
    compressed_size: int


@dataclass(frozen=True)
class ZipDiff:
    """Added/removed/changed members between snapshots."""

    added: list[str]
    removed: list[str]
    changed: list[str]  # content and/or size changed

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.changed)


def _hash_zip_member(
    zf: zipfile.ZipFile, zi: zipfile.ZipInfo, *, chunk_size: int = 1024 * 1024
) -> str:
    """Compute SHA256 of a single ZIP member's *decompressed* bytes."""

    h = hashlib.sha256()
    with zf.open(zi, "r") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            h.update(chunk)

    return h.hexdigest()


def zip_members_to_metadata(members: dict[str, ZipMemberMeta]) -> dict[str, dict[str, str | int]]:
    """Convert ZIP member fingerprints to JSON-safe metadata."""
    return {
        name: {
            "name": member.name,
            "sha256": member.sha256,
            "size": member.size,
            "compressed_size": member.compressed_size,
        }
        for name, member in members.items()
    }


def zip_members_from_metadata(
    metadata: dict[str, dict[str, str | int]],
) -> dict[str, ZipMemberMeta]:
    """Rebuild ZIP member fingerprints from stored metadata."""
    return {
        name: ZipMemberMeta(
            name=str(value["name"]),
            sha256=str(value["sha256"]),
            size=_required_non_negative_integer(value["size"], member_name=name, field_name="size"),
            compressed_size=_required_non_negative_integer(
                value["compressed_size"], member_name=name, field_name="compressed_size"
            ),
        )
        for name, value in metadata.items()
    }


def _required_non_negative_integer(value: str | int, *, member_name: str, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            f"ZIP member {member_name!r} field {field_name!r} must be a non-negative integer"
        )

    return value


def zip_members_to_extract(
    *,
    current_members: dict[str, ZipMemberMeta],
    previous_metadata: dict[str, dict[str, str | int]] | None,
) -> list[str]:
    """Return ZIP member names that are new or changed."""
    previous_members = zip_members_from_metadata(previous_metadata) if previous_metadata else None

    diff = compute_zip_diff(previous_members, current_members)
    return diff.added + diff.changed


def scan_zip_members(zip_path: str | Path) -> dict[str, ZipMemberMeta]:
    """Scan a ZIP file and return each member metadata without extracting files."""

    zip_path = Path(zip_path)
    t0 = perf_counter()
    try:
        file_size = zip_path.stat().st_size
    except OSError:
        file_size = None

    logger.info(
        "scan_zip_members start: file=%s size_bytes=%s",
        zip_path.name,
        file_size if file_size is not None else "unknown",
    )

    members: dict[str, ZipMemberMeta] = {}
    hashed = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        for zi in zf.infolist():
            if zi.is_dir():
                continue

            logger.debug(
                "hash start: file=%s member=%s size=%d csize=%d",
                zip_path.name,
                zi.filename,
                zi.file_size,
                zi.compress_size,
            )

            sha = _hash_zip_member(zf, zi)

            members[zi.filename] = ZipMemberMeta(
                name=zi.filename, sha256=sha, size=zi.file_size, compressed_size=zi.compress_size
            )
            hashed += 1
    # summary
    dt_ms = int((perf_counter() - t0) * 1000)
    logger.info(
        "scan_zip_members done: file=%s members=%d hashed=%d elapsed_ms=%s",
        zip_path.name,
        len(members),
        hashed,
        dt_ms,
    )

    return members


def compute_zip_diff(
    prev_members: dict[str, ZipMemberMeta] | None, cur_members: dict[str, ZipMemberMeta]
) -> ZipDiff:
    """
    Compute which internal files changed between an old ZIP and a new ZIP.

    On the first run (prev is None), all current members will be reported as "added".
    """

    logger.debug(
        "compute_zip_diff start: prev=%d cur=%d",
        0 if not prev_members else len(prev_members),
        len(cur_members),
    )
    if not prev_members:
        added = sorted(cur_members.keys())
        logger.info("zip_diff first run: added=%d removed=0 changed=0", len(added))
        return ZipDiff(added=added, removed=[], changed=[])

    prev_names = set(prev_members.keys())
    cur_names = set(cur_members.keys())

    added = sorted(cur_names - prev_names)
    removed = sorted(prev_names - cur_names)

    changed = []
    for name in cur_names & prev_names:
        p, c = prev_members[name], cur_members[name]
        # compare hash and decompressed size
        if p.sha256 != c.sha256 or p.size != c.size:
            changed.append(name)

    changed = sorted(changed)

    logger.info(
        "compute_zip_diff done: added=%d removed=%d changed=%d",
        len(added),
        len(removed),
        len(changed),
    )

    return ZipDiff(added=added, removed=removed, changed=changed)
