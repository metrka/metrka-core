"""
save.py

Save DataFrames to disk, creating parent directories.

Because the filesystem will not do it for you out of respect.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from metrka_core.storage.atomic_writes import atomic_write
from metrka_core.storage.table_formats import (
    TABLE_FORMAT_EXTENSIONS,
    TableFormat,
    normalize_table_format,
)

logger = logging.getLogger(__name__)


def save_table(
    df: pd.DataFrame,
    dest_path: str | Path,
    fmt: str = "csv",
    index: bool = False,
    base_dir: str | Path | None = None,
) -> Path:
    """
    Write a DataFrame to a local file.

    The destination extension is derived from `fmt`.
    Parent directories are created automatically.
    `base_dir` affects only the path shown in logs.
    """
    table_format = normalize_table_format(fmt)

    out_path = Path(dest_path).resolve().with_suffix(TABLE_FORMAT_EXTENSIONS[table_format])

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if base_dir is None:
        display_path = out_path.name
    else:
        try:
            display_path = out_path.relative_to(Path(base_dir).expanduser().resolve()).as_posix()
        except ValueError:
            display_path = str(out_path)

    rows, cols = df.shape
    logger.info(
        "Saving table: format=%s rows=%d cols=%d dest=%s", table_format, rows, cols, display_path
    )

    if table_format is TableFormat.CSV:

        def write_csv(temporary_path: Path) -> None:
            df.to_csv(temporary_path, index=index, encoding="utf-8")

        atomic_write(out_path, write_csv)

    elif table_format is TableFormat.PARQUET:
        try:

            def write_parquet(temporary_path: Path) -> None:
                df.to_parquet(temporary_path, index=index)

            atomic_write(out_path, write_parquet)
        except ImportError as e:
            logger.error("Failed: Parquet engine missing.")
            raise ImportError(
                "Writing parquet requires 'pyarrow' or 'fastparquet'. Run: `pip install pyarrow`"
            ) from e

    logger.debug("Saved successfully: %s", display_path)

    return out_path
