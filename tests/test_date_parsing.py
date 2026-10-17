from __future__ import annotations

from datetime import date

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.pipeline.silver.fingerprints import fingerprint_silver_table
from metrka_core.transform.ops.dates import parse_dates


def test_date_parse_has_an_explicit_arrow_date_dtype() -> None:
    source = pd.DataFrame({"detainer_start_date": pd.Series(["08/15/2026", None], dtype="string")})

    result = parse_dates(
        source, {"detainer_start_date": {"cast_to": "date", "format_in": "%m/%d/%Y"}}
    )
    parsed = result.data["detainer_start_date"]

    assert isinstance(parsed.dtype, pd.ArrowDtype)
    assert parsed.dtype.pyarrow_dtype == pa.date32()
    assert parsed.iloc[0] == date(2026, 8, 15)
    assert pd.isna(parsed.iloc[1])


def test_date_parse_output_can_be_fingerprinted() -> None:
    source = pd.DataFrame({"detainer_start_date": pd.Series(["08/15/2026"], dtype="string")})

    result = parse_dates(
        source, {"detainer_start_date": {"cast_to": "date", "format_in": "%m/%d/%Y"}}
    )

    fingerprint = fingerprint_silver_table(table_key="INMATE_ACTIVE_DETAINERS", table=result.data)

    assert len(fingerprint.logical_data_hash) == 64
    assert len(fingerprint.schema_hash) == 64
