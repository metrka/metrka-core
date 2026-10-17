from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from metrka_core.validation.preflight.xlsx_verify import verify_single_xlsx

VALID_MEMBERS = {
    "[Content_Types].xml",
    "_rels/.rels",
    "xl/workbook.xml",
    "xl/_rels/workbook.xml.rels",
    "xl/worksheets/sheet1.xml",
}


def _write_package(path: Path, members: set[str]) -> None:
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for member in members:
            archive.writestr(member, b"<xml />")


def test_valid_xlsx_package_passes(tmp_path: Path) -> None:
    path = tmp_path / "valid.xlsx"
    _write_package(path, VALID_MEMBERS)

    result = verify_single_xlsx(path)

    assert result.passed
    assert result.crc_ok is True
    assert result.missing_members == ()
    assert result.worksheet_count == 1
    assert result.error is None


def test_non_zip_xlsx_fails(tmp_path: Path) -> None:
    path = tmp_path / "invalid.xlsx"
    path.write_text("not an XLSX package", encoding="utf-8")

    result = verify_single_xlsx(path)

    assert not result.passed
    assert result.is_zipfile is False
    assert result.error == "not a valid ZIP-based XLSX package"


def test_missing_required_member_fails(tmp_path: Path) -> None:
    path = tmp_path / "missing-member.xlsx"
    members = VALID_MEMBERS - {"xl/workbook.xml"}
    _write_package(path, members)

    result = verify_single_xlsx(path)

    assert not result.passed
    assert result.missing_members == ("xl/workbook.xml",)
    assert result.error == "required XLSX package members are missing"


def test_package_without_worksheets_fails(tmp_path: Path) -> None:
    path = tmp_path / "no-worksheets.xlsx"
    members = {member for member in VALID_MEMBERS if not member.startswith("xl/worksheets/")}
    _write_package(path, members)

    result = verify_single_xlsx(path)

    assert not result.passed
    assert result.worksheet_count == 0
    assert result.error == "XLSX package contains no worksheets"


def test_empty_xlsx_fails(tmp_path: Path) -> None:
    path = tmp_path / "empty.xlsx"
    path.touch()

    result = verify_single_xlsx(path)

    assert not result.passed
    assert result.error == "file is empty"
