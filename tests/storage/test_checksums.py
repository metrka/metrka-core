"""Tests for the one canonical file-checksum implementation."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pytest

from metrka_core.storage.checksums import (
    format_sha256_checksum,
    parse_sha256_checksum,
    parse_sha256_hex,
    sha256_checksum,
    sha256_file,
    sha256_stream,
)

ABC_SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_file_hash_and_checksum_have_controlled_representations(tmp_path: Path) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"abc")

    assert sha256_file(path) == ABC_SHA256
    assert sha256_checksum(path) == f"sha256:{ABC_SHA256}"
    assert format_sha256_checksum(ABC_SHA256) == f"sha256:{ABC_SHA256}"
    assert parse_sha256_hex(ABC_SHA256) == ABC_SHA256
    assert parse_sha256_checksum(f"sha256:{ABC_SHA256}") == ABC_SHA256
    assert sha256_stream(BytesIO(b"abc")) == ABC_SHA256


@pytest.mark.parametrize(
    "value",
    [
        ABC_SHA256.upper(),
        f" {ABC_SHA256}",
        f"{ABC_SHA256} ",
        f"sha256:{ABC_SHA256}",
        "not-a-digest",
    ],
)
def test_raw_digest_parser_rejects_noncanonical_values(value: str) -> None:
    with pytest.raises(ValueError, match="lowercase 64-character"):
        parse_sha256_hex(value)


@pytest.mark.parametrize(
    "value",
    [
        ABC_SHA256,
        f"sha256:{ABC_SHA256.upper()}",
        f"SHA256:{ABC_SHA256}",
        f"sha256: {ABC_SHA256}",
        "sha256:not-a-digest",
    ],
)
def test_checksum_parser_requires_algorithm_and_canonical_digest(value: str) -> None:
    with pytest.raises(ValueError, match="sha256:<64 lowercase"):
        parse_sha256_checksum(value)
