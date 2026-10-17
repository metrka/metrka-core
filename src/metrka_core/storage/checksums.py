"""Canonical SHA-256 calculation and textual representations."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import BinaryIO, Final

SHA256_HEX_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
SHA256_CHECKSUM_PATTERN: Final = re.compile(r"sha256:([0-9a-f]{64})\Z")


def sha256_file(path: Path) -> str:
    """Return a file's SHA-256 digest as 64 lowercase hexadecimal characters."""

    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def sha256_stream(stream: BinaryIO) -> str:
    """Return the SHA-256 digest of a readable binary stream at its current position."""

    digest = hashlib.sha256()

    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)

    return digest.hexdigest()


def parse_sha256_hex(value: str) -> str:
    """Validate and return a raw lowercase SHA-256 hexadecimal digest."""

    if SHA256_HEX_PATTERN.fullmatch(value) is None:
        raise ValueError("Expected a lowercase 64-character SHA-256 hexadecimal digest")

    return value


def format_sha256_checksum(digest: str) -> str:
    """Format one validated raw digest for a generic checksum field."""

    return f"sha256:{parse_sha256_hex(digest)}"


def sha256_checksum(path: Path) -> str:
    """Return a file checksum in canonical ``sha256:<lowercase hex>`` form."""

    return format_sha256_checksum(sha256_file(path))


def parse_sha256_checksum(value: str) -> str:
    """Validate a canonical checksum and return its raw hexadecimal digest."""

    match = SHA256_CHECKSUM_PATTERN.fullmatch(value)

    if match is None:
        raise ValueError(
            "Expected checksum in 'sha256:<64 lowercase hexadecimal characters>' format"
        )

    return match.group(1)
