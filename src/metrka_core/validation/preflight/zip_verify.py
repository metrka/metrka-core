"""
ZIP preflight checks.

We sanity-check candidate ZIP file before the pipeline touches it:
- does it look like a ZIP?
- if so, does it pass CRC check?

We collect results for every file and (optionally) raise once at the end.
"""

from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ZipVerifyEntry:
    """Verification result for one candidate file."""

    file_name: str
    is_zipfile: bool = False
    crc_ok: bool | None = None
    crc_bad_member: str | None = None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.is_zipfile and self.crc_ok is True and self.error is None


def verify_single_zip(path: str | Path) -> ZipVerifyEntry:
    """Check one file (is_zipfile +  CRC) and return the result."""

    path = Path(path)

    if not path.exists():
        return ZipVerifyEntry(file_name=path.name, is_zipfile=False, error="file does not exist")

    try:
        is_zip = zipfile.is_zipfile(path)
        logger.debug("file=%s is_zipfile=%s", path.name, is_zip)

        if not is_zip:
            return ZipVerifyEntry(
                file_name=path.name, is_zipfile=False, error="not a valid zip format"
            )

        # Test CRC
        with zipfile.ZipFile(path, "r") as zf:
            bad_member = zf.testzip()
            crc_ok = bad_member is None
            error = f"CRC failed on member: {bad_member}" if not crc_ok else None

            return ZipVerifyEntry(
                file_name=path.name,
                is_zipfile=True,
                crc_ok=crc_ok,
                crc_bad_member=bad_member,
                error=error,
            )

    except Exception as e:
        logger.exception("ZIP verification failed for file=%s", path.name)
        return ZipVerifyEntry(
            file_name=path.name, is_zipfile=False, error=f"{type(e).__name__}: {e}"
        )
