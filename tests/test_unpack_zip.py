"""
Tests for safe ZIP extraction.

The tests cover normal extraction, selective extraction
and protection against unsafe archive paths.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

from metrka_core.pipeline.bronze.unpack_zip import (
    ZipExtractResult,
    _is_safe_member,
    secure_extract_zip,
)


def _create_zip(tmp_path: Path, files: dict) -> None:
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files.items():
            zf.writestr(filename, content)


def test_zip_extract_result_returns_true_on_count_equals() -> None:
    test_result_ok = ZipExtractResult(
        zip_path="./test_zip.zip",
        dest_dir="./dest_folder",
        expected_count=5,
        extracted_count=5,
        error=None,
    )

    assert test_result_ok.passed is True


def test_zip_extract_result_returns_false_on_count_not_equals() -> None:
    test_result_ok = ZipExtractResult(
        zip_path="./test_zip.zip",
        dest_dir="./dest_folder",
        expected_count=3,
        extracted_count=5,
        error=None,
    )

    assert test_result_ok.passed is False


def test_zip_extract_result_returns_false_on_error() -> None:
    test_result_ok = ZipExtractResult(
        zip_path="./test_zip.zip",
        dest_dir="./dest_folder",
        expected_count=3,
        extracted_count=3,
        error="There is an error!",
    )

    assert test_result_ok.passed is False


def test_is_safe_member_good_file_returns_true(tmp_path: Path) -> None:
    dest_path = tmp_path
    file_name = "data.csv"

    result = _is_safe_member(dest_dir=dest_path, member_name=file_name)

    assert result is True


def test_is_safe_member_good_dir_returns_true(tmp_path: Path) -> None:
    dest_path = tmp_path
    file_name = "inmates/data.csv"

    result = _is_safe_member(dest_dir=dest_path, member_name=file_name)

    assert result is True


def test_is_safe_member_bad_path_returns_false(tmp_path: Path) -> None:
    dest_path = tmp_path
    file_name = "/programs/data.csv"

    result = _is_safe_member(dest_dir=dest_path, member_name=file_name)

    assert result is False


def test_is_safe_member_bad_path_up_returns_false(tmp_path: Path) -> None:
    dest_path = tmp_path
    file_name = "../../../secret.csv"

    result = _is_safe_member(dest_dir=dest_path, member_name=file_name)

    assert result is False


def test_secure_extract_zip_not_exists_return_passed_false(tmp_path: Path) -> None:
    ghost_zip_path = tmp_path / "this_file_does_not_exist.zip"
    dest_folder = tmp_path / "extraction_folder"

    result = secure_extract_zip(zip_path=ghost_zip_path, dest_dir=dest_folder)

    assert result.passed is False
    assert result.error == "ZIP file not found"

    assert result.expected_count == 0
    assert result.extracted_count == 0

    assert result.zip_path == ghost_zip_path.resolve()
    assert result.dest_dir == dest_folder.resolve()


def test_secure_extract_zip_extract_only_changed_files(tmp_path: Path) -> None:
    zip_path = tmp_path / "files.zip"
    target_path = tmp_path / "destination"
    _create_zip(
        zip_path,
        {"inmates.csv": b"1,Alice", "releases.csv": b"99, John", "offenses.csv": b"1,Speeding"},
    )

    result = secure_extract_zip(
        zip_path=zip_path, dest_dir=target_path, members_to_extract=["offenses.csv"]
    )

    assert result.passed is True
    assert Path(tmp_path / "destination" / "offenses.csv").exists() is True
    assert Path(tmp_path / "destination" / "inmates.csv").exists() is False
    assert Path(tmp_path / "destination" / "releases.csv").exists() is False

    assert result.expected_count == 1
    assert result.extracted_count == 1
    assert result.extracted_files == ["offenses.csv"]


def test_secure_extract_zip_extracts_all_files(tmp_path: Path) -> None:
    zip_path = tmp_path / "files.zip"
    target_path = tmp_path / "destination"
    _create_zip(
        zip_path,
        {"inmates.csv": b"1,Alice", "releases.csv": b"99, John", "offenses.csv": b"1,Speeding"},
    )

    result = secure_extract_zip(zip_path=zip_path, dest_dir=target_path, members_to_extract=None)

    assert result.passed is True
    assert Path(tmp_path / "destination" / "offenses.csv").exists() is True
    assert Path(tmp_path / "destination" / "inmates.csv").exists() is True
    assert Path(tmp_path / "destination" / "releases.csv").exists() is True

    assert result.expected_count == 3
    assert result.extracted_count == 3
    assert result.extracted_files == ["inmates.csv", "releases.csv", "offenses.csv"]


@patch("metrka_core.pipeline.bronze.unpack_zip.zipfile.ZipFile")
def test_secure_extract_zip_blocks_zip_slip_attack(mock_zipfile: Mock, tmp_path: Path) -> None:
    """Unsafe member paths should be blocked before extraction."""
    fake_zip = tmp_path / "hacker_payload.zip"
    fake_zip.touch()

    dest_dir = tmp_path / "safe_extraction_folder"

    hacker_file_name = "../../../windows/systems32/hack.sh"
    malicious_zi = zipfile.ZipInfo(hacker_file_name)

    mock_zipfile.return_value.__enter__.return_value.infolist.return_value = [malicious_zi]

    result = secure_extract_zip(zip_path=fake_zip, dest_dir=dest_dir, safe=True)

    assert result.passed is False
    assert result.expected_count == 0
    assert result.extracted_count == 0

    assert result.error is not None
    assert "RuntimeError" in result.error
    assert "Unsafe ZIP member path blocked" in result.error
    assert hacker_file_name in result.error


def test_secure_extract_zip_extracts_nested_memebr(tmp_path: Path) -> None:
    zip_path = tmp_path / "files.zip"
    target_path = tmp_path / "destination"

    _create_zip(zip_path, {"inmates/data.csv": b"1,Alice"})

    result = secure_extract_zip(zip_path=zip_path, dest_dir=target_path, members_to_extract=None)

    assert result.passed is True
    assert (target_path / "inmates" / "data.csv").exists() is True
    assert result.expected_count == 1
    assert result.extracted_count == 1


def test_secure_extract_zip_requested_member_missing_in_zip(tmp_path: Path) -> None:
    zip_path = tmp_path / "files.zip"
    target_path = tmp_path / "destination"

    _create_zip(zip_path, {"inmates.csv": b"1,Alice", "releases.csv": b"99,John"})

    result = secure_extract_zip(
        zip_path=zip_path, dest_dir=target_path, members_to_extract=["offenses.csv"]
    )

    assert result.passed is False
    assert result.extracted_count == 0
    assert result.expected_count == 1
    assert result.error is not None
    assert "Requested ZIP member(s) not found" in result.error
