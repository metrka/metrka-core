"""
Tests for ZIP fingerprinting and member-level diff.

The tests cover hashing, scanning ZIP members and detecting
added, removed or changed files between snapshots.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from unittest.mock import Mock, patch
from zipfile import ZipFile

import pytest

from metrka_core.validation.preflight.zip_fingerprint import (
    ZipDiff,
    ZipMemberMeta,
    _hash_zip_member,
    compute_zip_diff,
    scan_zip_members,
    zip_members_from_metadata,
)


def test_zipdiff_no_files_returns_false() -> None:
    no_diff = ZipDiff(added=[], removed=[], changed=[])

    assert no_diff.has_changes is False


def test_zip_diff_has_files_returns_true() -> None:
    diff = ZipDiff(added=["new.csv"], removed=[], changed=[])

    assert diff.has_changes is True
    assert diff.added[0] == "new.csv"


def test_get_member_hash_chunk_read_returns_hash(tmp_path: Path) -> None:
    zip_path = tmp_path / "test_chunking.zip"

    data = b"HELLO_WORLD_123"

    expected_hash = sha256(data).hexdigest()

    with ZipFile(zip_path, "w") as zf:
        zf.writestr("data.csv", data)

    with ZipFile(zip_path, "r") as zf:
        zi = zf.getinfo("data.csv")

        actual_hash = _hash_zip_member(zf, zi, chunk_size=3)

    assert actual_hash == expected_hash


def test_scan_zip_members_ignores_empty_directories(tmp_path: Path) -> None:
    zip_path = tmp_path / "test_with_folders.zip"

    with ZipFile(zip_path, "w") as zf:
        zf.writestr("real_data.csv", b"Hello you")

        zf.writestr("empty_folder/", b"")

    result = scan_zip_members(zip_path)

    assert len(result) == 1
    assert "real_data.csv" in result
    assert "empty_folder/" not in result


def test_scan_zip_members_empty_zip_returns_empty_dict(tmp_path: Path) -> None:
    zip_path = tmp_path / "empty.zip"

    with ZipFile(zip_path, "w"):
        pass

    result = scan_zip_members(zip_path)

    assert result == {}


@patch("metrka_core.validation.preflight.zip_fingerprint.zipfile.ZipFile")
@patch("metrka_core.validation.preflight.zip_fingerprint.Path.stat")
def test_scan_zip_members_continues_when_stat_fails(
    mock_stat: Mock, mock_zipfile: Mock, tmp_path: Path
) -> None:
    """Stat failure should not stop ZIP member scanning"""
    fake_path = tmp_path / "ghost_file.zip"

    mock_stat.side_effect = OSError("Hard disk failed")

    mock_zipfile.return_value.__enter__.return_value.infolist.return_value = []

    result = scan_zip_members(fake_path)

    assert result == {}

    mock_stat.assert_called_once()


def test_compute_zip_diff_none_prev_members_returns_added() -> None:
    zip_1 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        ),
        "releases.csv": ZipMemberMeta(
            name="releases.csv", sha256="qwertyuop89", size=490, compressed_size=320
        ),
    }

    result = compute_zip_diff(prev_members=None, cur_members=zip_1)

    assert result.has_changes is True
    assert len(result.added) == 2


def test_compute_zip_diff_same_files_returns_no_changes() -> None:
    zip_1 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        )
    }

    zip_2 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        )
    }

    result = compute_zip_diff(prev_members=zip_1, cur_members=zip_2)

    assert result.has_changes is False
    assert len(result.added) == 0 and len(result.removed) == 0 and len(result.changed) == 0


def test_compute_zip_diff_removed() -> None:
    old_zip = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        )
    }
    new_zip = {
        "releases.csv": ZipMemberMeta(
            name="releases.csv", sha256="qwertyuop89", size=490, compressed_size=320
        )
    }

    result = compute_zip_diff(prev_members=old_zip, cur_members=new_zip)

    assert result.has_changes is True
    assert len(result.removed) == 1
    assert len(result.added) == 1


def test_compute_zip_diff_when_diff_hash_same_name() -> None:
    zip_1 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        )
    }

    zip_2 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop123", size=270, compressed_size=180
        )
    }

    result = compute_zip_diff(prev_members=zip_1, cur_members=zip_2)

    assert result.has_changes is True
    assert len(result.changed) == 1
    assert len(result.added) == 0
    assert len(result.removed) == 0


def test_compute_zip_diff_when_diff_size_same_values() -> None:
    zip_1 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=320, compressed_size=180
        )
    }

    zip_2 = {
        "inmates.csv": ZipMemberMeta(
            name="inmates.csv", sha256="qwertyuop", size=270, compressed_size=180
        )
    }

    result = compute_zip_diff(prev_members=zip_1, cur_members=zip_2)

    assert result.has_changes is True
    assert result.changed == ["inmates.csv"]
    assert len(result.added) == 0
    assert len(result.removed) == 0


@pytest.mark.parametrize("field_name", ["size", "compressed_size"])
@pytest.mark.parametrize("invalid_value", [-1, True])
def test_stored_zip_metadata_rejects_invalid_size(field_name: str, invalid_value: int) -> None:
    metadata: dict[str, dict[str, str | int]] = {
        "inmates.csv": {
            "name": "inmates.csv",
            "sha256": "a" * 64,
            "size": 270,
            "compressed_size": 180,
        }
    }
    metadata["inmates.csv"][field_name] = invalid_value

    with pytest.raises(ValueError, match=rf"field '{field_name}'.*non-negative integer"):
        zip_members_from_metadata(metadata)
