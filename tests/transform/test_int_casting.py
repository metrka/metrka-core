"""Integer casts accept whole numbers written with a decimal point."""

from __future__ import annotations

import pandas as pd
import pytest

from metrka_core.transform.ops.casting import cast_columns


def test_int_cast_reads_whole_numbers_written_as_decimals() -> None:
    # A column with blank cells is read as floats in Bronze and written as "30.0".
    frame = pd.DataFrame({"count": pd.Series(["30.0", None, "12", "1E2"], dtype="string")})

    result = cast_columns(frame, {"count": "int"})

    assert str(result.data["count"].dtype) == "Int64"
    assert result.data["count"].tolist()[0] == 30
    assert pd.isna(result.data["count"].tolist()[1])
    assert result.data["count"].tolist()[2:] == [12, 100]


def test_int_cast_rejects_a_fractional_value() -> None:
    frame = pd.DataFrame({"count": pd.Series(["30.0", "30.5"], dtype="string")})

    with pytest.raises(ValueError, match="'30.5' is not a whole number"):
        cast_columns(frame, {"count": "int"})


def test_int_cast_rejects_text() -> None:
    frame = pd.DataFrame({"count": pd.Series(["30", "n/a"], dtype="string")})

    with pytest.raises(ValueError, match="'n/a' is not a valid integer"):
        cast_columns(frame, {"count": "int"})


def test_int_cast_keeps_large_whole_numbers_exact() -> None:
    frame = pd.DataFrame({"id": pd.Series(["9007199254740993.0"], dtype="string")})

    result = cast_columns(frame, {"id": "int"})

    assert result.data["id"].tolist() == [9007199254740993]
