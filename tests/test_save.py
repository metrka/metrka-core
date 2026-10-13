"""
Tests for `metrka_core.storage.save`.

Ensures the save_table pipeline actually writes to disk and handles bad formats
without silently failing.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from metrka_core.storage.save import save_table


# ==============================================================================
# Fixtures
# ==============================================================================
@pytest.fixture
def dummy_df() -> pd.DataFrame:
    return pd.DataFrame(
        {"person_id": ["123", "456"], "last_name": ["Smith", "Doe"], "age": [45, 32]}
    )


# ==============================================================================
# Tests: I/O and Directory creation
# ==============================================================================


def test_save_table_csv_creates_dirs(dummy_df: pd.DataFrame, tmp_path: Path) -> None:
    """Test that deeply nested folders are created and CSV is saved accurately."""

    target_dir = tmp_path / "deeply" / "nested" / "silver_run"
    target_file = target_dir / "inmates"

    returned_path = save_table(dummy_df, target_file, fmt="csv", index=False)

    assert returned_path.exists()
    assert returned_path.suffix == ".csv"
    assert returned_path.parent == target_dir

    read_back_df = pd.read_csv(returned_path)
    assert len(read_back_df) == 2
    assert "last_name" in read_back_df.columns
    assert read_back_df.iloc[0]["last_name"] == "Smith"


def test_save_table_parquet_enforces_extension(dummy_df: pd.DataFrame, tmp_path: Path) -> None:

    target_file = tmp_path / "inmates.wrong_ext"

    returned_path = save_table(dummy_df, target_file, fmt="parquet")

    assert returned_path.exists()
    assert returned_path.name == "inmates.parquet"

    read_back_df = pd.read_parquet(returned_path)
    assert len(read_back_df) == 2
    assert read_back_df.iloc[1]["age"] == 32


# ==============================================================================
# Tests: Error handling
# ==============================================================================


def test_save_table_unsupported_format(dummy_df: pd.DataFrame, tmp_path: Path) -> None:
    """Passing a bad format string instantly halts the pipeline."""
    target_file = tmp_path / "data"

    with pytest.raises(ValueError, match="Unsupported format: 'excel'"):
        save_table(dummy_df, target_file, fmt="excel")


def test_save_table_normalizes_supported_format(dummy_df: pd.DataFrame, tmp_path: Path) -> None:
    returned_path = save_table(dummy_df, tmp_path / "data", fmt=" CSV ")

    assert returned_path.suffix == ".csv"


def test_save_table_parquet_missing_engine(dummy_df: pd.DataFrame, tmp_path: Path) -> None:
    target_file = tmp_path / "data"

    with (
        patch.object(pd.DataFrame, "to_parquet", side_effect=ImportError("No engine found")),
        pytest.raises(ImportError, match="Writing parquet requires 'pyarrow'"),
    ):
        save_table(dummy_df, target_file, fmt="parquet")
