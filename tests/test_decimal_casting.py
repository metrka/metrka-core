from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.pipeline.silver.fingerprints import fingerprint_silver_table
from metrka_core.transform.ops.casting import cast_columns


def test_decimal_cast_has_an_explicit_arrow_decimal_dtype() -> None:
    source = pd.DataFrame({"rate": pd.Series(["12.30", None, "0.00"], dtype="string")})

    result = cast_columns(source, {"rate": "decimal(10,2)"})
    rate = result.data["rate"]

    assert isinstance(rate.dtype, pd.ArrowDtype)
    assert rate.dtype.pyarrow_dtype == pa.decimal128(10, 2)
    assert rate.iloc[0] == Decimal("12.30")
    assert pd.isna(rate.iloc[1])
    assert rate.iloc[2] == Decimal("0.00")


def test_decimal_cast_output_can_be_fingerprinted() -> None:
    source = pd.DataFrame({"rate": pd.Series(["12.30", "0.00"], dtype="string")})

    result = cast_columns(source, {"rate": "decimal(10,2)"})

    fingerprint = fingerprint_silver_table(table_key="rates", table=result.data)

    assert len(fingerprint.logical_data_hash) == 64
    assert len(fingerprint.schema_hash) == 64
