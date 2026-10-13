"""
Tests for ZIP preflight checks.

What we do:
- verify one ZIP file at a time
- cover missing files and non-ZIP inputs
- check CRC success and CRC failures
- check error handling for unexpected exceptions

"""

from pathlib import Path
from unittest.mock import Mock, patch
from zipfile import BadZipFile, ZipFile

import pytest

from metrka_core.validation.preflight.zip_verify import ZipVerifyEntry, verify_single_zip


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"file_name": "good_file.zip", "is_zipfile": True, "crc_ok": True, "error": None}, True),
        ({"file_name": "bad_file.zip", "is_zipfile": False}, False),
        (
            {
                "file_name": "bad_crc_file.zip",
                "is_zipfile": True,
                "crc_ok": False,
                "error": "CRC failed",
            },
            False,
        ),
    ],
)
def test_zipverifyentry_passed_property(kwargs: dict, expected: bool) -> None:
    entry = ZipVerifyEntry(**kwargs)

    assert entry.passed is expected


def test_verify_missing_file(tmp_path: Path) -> None:
    file_path = tmp_path / "no_file_test.zip"

    entry = verify_single_zip(file_path)

    assert entry.file_name == "no_file_test.zip"
    assert entry.is_zipfile is False
    assert entry.error is not None
    assert "file does not exist" in entry.error
    assert entry.passed is False


def test_verify_not_a_zip(tmp_path: Path) -> None:
    file_path = tmp_path / "fake.zip"
    file_path.write_bytes(b"Hello. I am not a ZIP file")

    entry = verify_single_zip(file_path)

    assert entry.file_name == "fake.zip"
    assert entry.is_zipfile is False
    assert entry.error is not None
    assert "not a valid zip format" in entry.error
    assert entry.passed is False


def test_verify_valid_zip(tmp_path: Path) -> None:
    file_path = tmp_path / "happy_file.zip"
    member_file = tmp_path / "member.txt"
    member_file.write_text("Hello", encoding="utf-8")

    with ZipFile(file_path, "w") as test_zip:
        test_zip.write(member_file, arcname=member_file.name)

    entry = verify_single_zip(file_path)

    assert entry.file_name == "happy_file.zip"
    assert entry.crc_bad_member is None
    assert entry.is_zipfile is True
    assert entry.crc_ok is True
    assert entry.error is None
    assert entry.passed is True


@patch("metrka_core.validation.preflight.zip_verify.zipfile.ZipFile")
@patch("metrka_core.validation.preflight.zip_verify.zipfile.is_zipfile")
def test_verify_corrupted_crc(mock_is_zipfile: Mock, mock_zipfile: Mock, tmp_path: Path) -> None:
    bad_file = tmp_path / "corrupt.zip"
    bad_file.touch()

    mock_is_zipfile.return_value = True

    mock_zipfile.return_value.__enter__.return_value.testzip.return_value = "broken_data.csv"

    entry = verify_single_zip(bad_file)

    assert entry.is_zipfile is True
    assert entry.crc_ok is False
    assert entry.crc_bad_member == "broken_data.csv"
    assert entry.passed is False
    assert entry.error is not None
    assert "CRC failed on member" in entry.error

    mock_is_zipfile.assert_called_once()
    mock_zipfile.assert_called_once()
    mock_zipfile.return_value.__enter__.return_value.testzip.assert_called_once()


@patch("metrka_core.validation.preflight.zip_verify.zipfile.is_zipfile")
def test_verify_exception_returns_report(mock_is_zipfile: Mock, tmp_path: Path) -> None:
    locked_file = tmp_path / "locked.zip"
    locked_file.touch()

    mock_is_zipfile.side_effect = PermissionError("Access Denied")

    entry = verify_single_zip(locked_file)

    assert entry.is_zipfile is False
    assert entry.passed is False
    assert entry.error is not None
    assert "PermissionError" in entry.error
    assert "Access Denied" in entry.error


@patch("metrka_core.validation.preflight.zip_verify.zipfile.ZipFile")
@patch("metrka_core.validation.preflight.zip_verify.zipfile.is_zipfile")
def test_verify_zipfile_open_raises_badzipfile(
    mock_is_zipfile: Mock, mock_zipfile: Mock, tmp_path: Path
) -> None:
    file_path = tmp_path / "truncated.zip"
    file_path.touch()

    mock_is_zipfile.return_value = True

    mock_zipfile.side_effect = BadZipFile("File is not a zip file")

    entry = verify_single_zip(file_path)

    assert entry.file_name == "truncated.zip"
    assert entry.is_zipfile is False
    assert entry.passed is False
    assert entry.error is not None
    assert "BadZipFile" in entry.error

    mock_is_zipfile.assert_called_once()
    mock_zipfile.assert_called_once()
