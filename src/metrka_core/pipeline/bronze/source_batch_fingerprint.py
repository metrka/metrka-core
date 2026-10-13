"""Deterministic fingerprints for batches of landed source files."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from metrka_core.storage.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class SourceBatchMember:
    """Fingerprint information for one source file."""

    filename: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class SourceBatchFingerprint:
    """Portable identity of one source-file batch."""

    sha256: str
    members: tuple[SourceBatchMember, ...]


def fingerprint_source_batch(paths: Iterable[Path]) -> SourceBatchFingerprint:
    """Build an order-independent fingerprint without storing absolute paths."""
    ordered_paths = sorted(paths, key=lambda path: (path.name.casefold(), path.name))

    if not ordered_paths:
        raise ValueError("Cannot fingerprint an empty source batch")

    normalized_names: set[str] = set()
    members: list[SourceBatchMember] = []

    for path in ordered_paths:
        normalized_name = path.name.casefold()

        if normalized_name in normalized_names:
            raise ValueError(f"Source batch contains duplicate filename: {path.name}")

        normalized_names.add(normalized_name)

        members.append(
            SourceBatchMember(
                filename=path.name, sha256=sha256_file(path), size_bytes=path.stat().st_size
            )
        )

    manifest = [
        {"filename": member.filename, "sha256": member.sha256, "size_bytes": member.size_bytes}
        for member in members
    ]

    canonical_manifest = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    batch_hash = hashlib.sha256(canonical_manifest).hexdigest()

    return SourceBatchFingerprint(sha256=batch_hash, members=tuple(members))
