"""Preflight verification for XLSX packages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import BadZipFile, ZipFile, is_zipfile

REQUIRED_XLSX_MEMBERS = frozenset(
    {"[Content_Types].xml", "_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
)


@dataclass(frozen=True, slots=True)
class XlsxVerifyEntry:
    """Verification result for one XLSX package."""

    file_name: str
    is_zipfile: bool = False
    crc_ok: bool | None = None
    crc_bad_member: str | None = None
    missing_members: tuple[str, ...] = ()
    worksheet_count: int | None = None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return (
            self.is_zipfile
            and self.crc_ok is True
            and not self.missing_members
            and self.worksheet_count is not None
            and self.worksheet_count > 0
            and self.error is None
        )


def verify_single_xlsx(path: str | Path) -> XlsxVerifyEntry:
    """Verify the physical structure of one XLSX package."""

    file_path = Path(path)

    if not file_path.is_file():
        return XlsxVerifyEntry(file_name=file_path.name, error="file does not exist")

    if file_path.stat().st_size == 0:
        return XlsxVerifyEntry(file_name=file_path.name, error="file is empty")

    if not is_zipfile(file_path):
        return XlsxVerifyEntry(file_name=file_path.name, error="not a valid ZIP-based XLSX package")

    try:
        with ZipFile(file_path, "r") as archive:
            bad_member = archive.testzip()

            if bad_member is not None:
                return XlsxVerifyEntry(
                    file_name=file_path.name,
                    is_zipfile=True,
                    crc_ok=False,
                    crc_bad_member=bad_member,
                    error=f"CRC failed on member: {bad_member}",
                )

            members = set(archive.namelist())
            missing_members = tuple(sorted(REQUIRED_XLSX_MEMBERS - members))

            worksheet_count = sum(
                member.startswith("xl/worksheets/") and member.endswith(".xml")
                for member in members
            )

            error: str | None = None

            if missing_members:
                error = "required XLSX package members are missing"
            elif worksheet_count == 0:
                error = "XLSX package contains no worksheets"

            return XlsxVerifyEntry(
                file_name=file_path.name,
                is_zipfile=True,
                crc_ok=True,
                missing_members=missing_members,
                worksheet_count=worksheet_count,
                error=error,
            )

    except BadZipFile as error:
        return XlsxVerifyEntry(file_name=file_path.name, error=f"BadZipFile: {error}")
    except OSError as error:
        return XlsxVerifyEntry(file_name=file_path.name, error=f"{type(error).__name__}: {error}")
