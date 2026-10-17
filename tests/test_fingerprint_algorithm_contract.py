from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]

from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
    SILVER_FINGERPRINT_VERSION,
    combine_silver_table_fingerprints,
    fingerprint_silver_table,
)

# These constants are public compatibility promises for the controlled V1
# fingerprint algorithms. Do not update them merely to make a changed test
# pass. A deliberate algorithm change requires a new algorithm identifier
# and a separately pinned contract.
EXPECTED_V1_TABLE_SCHEMA_HASH = "f425457d52af20c4fe961451d7271223cfac8a05340b0b2f3d0dd7d8c1ea0912"
EXPECTED_V1_TABLE_LOGICAL_DATA_HASH = (
    "97458ad706602f1b924953aebe1e445bc51a3cd277b1fcbcc1cac42f2f685635"
)
EXPECTED_V1_DATASET_SCHEMA_HASH = "b9b0e504f9448a2f0269a7fe0ce0ca8e7b30e8c1e5304cbd2dc9915ea7544010"
EXPECTED_V1_DATASET_LOGICAL_DATA_HASH = (
    "078259a0234f43d378969d4499607901aad703a615bfb17f10755433cd87d079"
)


def _canonical_v1_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": pd.Series(["a", "b"], dtype="string"),
            "year": pd.Series([2025, 2026], dtype="Int64"),
            "amount": pd.Series(
                [Decimal("1.50"), Decimal("2.00")], dtype=pd.ArrowDtype(pa.decimal128(10, 2))
            ),
            "measured": pd.Series(pd.to_datetime(["2026-01-01", "2026-01-02"], utc=True)),
            "grade": pd.Series(
                pd.Categorical(["low", "high"], categories=["low", "high"], ordered=True)
            ),
            "observed_on": pd.Series(
                [date(2026, 1, 1), date(2026, 1, 2)], dtype=pd.ArrowDtype(pa.date32())
            ),
        }
    )


def test_table_fingerprint_is_pinned_for_canonical_types_v1() -> None:
    fingerprint = fingerprint_silver_table(table_key="pinned", table=_canonical_v1_table())

    assert fingerprint.fingerprint_version == SILVER_FINGERPRINT_VERSION == 1
    assert SCHEMA_HASH_ALGORITHM == "metrka.logical-schema.sha256.canonical-types.v1"
    assert LOGICAL_DATA_HASH_ALGORITHM == "metrka.logical-data.sha256.sorted-rows.v1"
    assert fingerprint.schema_hash_algorithm == SCHEMA_HASH_ALGORITHM
    assert fingerprint.logical_hash_algorithm == LOGICAL_DATA_HASH_ALGORITHM
    assert fingerprint.schema_hash == EXPECTED_V1_TABLE_SCHEMA_HASH
    assert fingerprint.logical_data_hash == EXPECTED_V1_TABLE_LOGICAL_DATA_HASH


def test_dataset_fingerprint_is_pinned_for_canonical_types_v1() -> None:
    table_fingerprint = fingerprint_silver_table(table_key="pinned", table=_canonical_v1_table())

    fingerprint = combine_silver_table_fingerprints([table_fingerprint])

    assert fingerprint.fingerprint_version == SILVER_FINGERPRINT_VERSION == 1
    assert fingerprint.schema_hash_algorithm == SCHEMA_HASH_ALGORITHM
    assert fingerprint.logical_hash_algorithm == LOGICAL_DATA_HASH_ALGORITHM
    assert fingerprint.schema_hash == EXPECTED_V1_DATASET_SCHEMA_HASH
    assert fingerprint.logical_data_hash == EXPECTED_V1_DATASET_LOGICAL_DATA_HASH
