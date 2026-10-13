from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from metrka_core.pipeline.silver.fingerprints import (
    SCHEMA_HASH_ALGORITHM,
    combine_silver_table_fingerprints,
    fingerprint_silver_table,
)


def test_table_fingerprint_ignores_row_order() -> None:
    first = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    second = first.iloc[::-1].reset_index(drop=True)

    left = fingerprint_silver_table(table_key="people", table=first)
    right = fingerprint_silver_table(table_key="people", table=second)

    assert left.logical_data_hash == right.logical_data_hash
    assert left.schema_hash == right.schema_hash


def test_value_change_changes_data_hash_but_not_schema_hash() -> None:
    first = pd.DataFrame({"id": [1], "name": ["A"]})
    second = pd.DataFrame({"id": [1], "name": ["B"]})

    left = fingerprint_silver_table(table_key="people", table=first)
    right = fingerprint_silver_table(table_key="people", table=second)

    assert left.logical_data_hash != right.logical_data_hash
    assert left.schema_hash == right.schema_hash


def test_type_change_changes_schema_hash() -> None:
    strings = pd.DataFrame({"id": ["1", "2"]})
    integers = pd.DataFrame({"id": [1, 2]})

    left = fingerprint_silver_table(table_key="people", table=strings)
    right = fingerprint_silver_table(table_key="people", table=integers)

    assert left.schema_hash != right.schema_hash


def test_dataset_fingerprint_ignores_table_input_order() -> None:
    people = fingerprint_silver_table(table_key="people", table=pd.DataFrame({"id": [1]}))
    places = fingerprint_silver_table(table_key="places", table=pd.DataFrame({"id": [2]}))

    first = combine_silver_table_fingerprints([people, places])
    second = combine_silver_table_fingerprints([places, people])

    assert first == second


def test_object_dtype_is_rejected_at_the_silver_output_boundary() -> None:
    object_strings = pd.DataFrame({"name": pd.Series(["A", "B"], dtype="object")})

    with pytest.raises(TypeError, match="ambiguous object dtype"):
        fingerprint_silver_table(table_key="people", table=object_strings)


def test_explicit_string_dtype_is_supported() -> None:
    strings = pd.DataFrame({"name": pd.Series(["A", "B"], dtype="string")})

    fingerprint = fingerprint_silver_table(table_key="people", table=strings)

    assert len(fingerprint.schema_hash) == 64


def test_integer_schema_is_stable_across_numpy_and_nullable_dtypes() -> None:
    numpy_integers = pd.DataFrame({"id": pd.Series([1, 2], dtype="int64")})
    nullable_integers = pd.DataFrame({"id": pd.Series([1, 2], dtype="Int64")})

    left = fingerprint_silver_table(table_key="people", table=numpy_integers)
    right = fingerprint_silver_table(table_key="people", table=nullable_integers)

    assert left.schema_hash == right.schema_hash


def test_decimal_precision_and_scale_are_part_of_the_schema_hash() -> None:
    decimal_10_2 = pd.DataFrame(
        {"amount": pd.Series([Decimal("12.30")], dtype=pd.ArrowDtype(pa.decimal128(10, 2)))}
    )
    decimal_12_2 = pd.DataFrame(
        {"amount": pd.Series([Decimal("12.30")], dtype=pd.ArrowDtype(pa.decimal128(12, 2)))}
    )

    left = fingerprint_silver_table(table_key="payments", table=decimal_10_2)
    right = fingerprint_silver_table(table_key="payments", table=decimal_12_2)

    assert left.logical_data_hash == right.logical_data_hash
    assert left.schema_hash != right.schema_hash


def test_date_and_datetime_have_distinct_schema_types() -> None:
    dates = pd.DataFrame(
        {"event_at": pd.Series([date(2026, 8, 16)], dtype=pd.ArrowDtype(pa.date32()))}
    )
    datetimes = pd.DataFrame(
        {"event_at": pd.Series(["2026-08-16T00:00:00"], dtype="datetime64[us]")}
    )

    date_fingerprint = fingerprint_silver_table(table_key="events", table=dates)
    datetime_fingerprint = fingerprint_silver_table(table_key="events", table=datetimes)

    assert date_fingerprint.schema_hash != datetime_fingerprint.schema_hash


def test_unknown_dtype_is_rejected_instead_of_entering_schema_hash() -> None:
    complex_values = pd.DataFrame({"value": pd.Series([1 + 2j], dtype="complex128")})

    with pytest.raises(TypeError, match="Unmapped dtype in Silver fingerprint"):
        fingerprint_silver_table(table_key="complex_values", table=complex_values)


def test_category_values_are_part_of_the_schema_hash() -> None:
    first = pd.DataFrame(
        {"status": pd.Series(pd.Categorical(["active"], categories=["active", "inactive"]))}
    )
    second = pd.DataFrame(
        {"status": pd.Series(pd.Categorical(["active"], categories=["active", "archived"]))}
    )

    left = fingerprint_silver_table(table_key="people", table=first)
    right = fingerprint_silver_table(table_key="people", table=second)

    assert left.logical_data_hash == right.logical_data_hash
    assert left.schema_hash != right.schema_hash


def test_category_order_is_part_of_the_schema_hash() -> None:
    unordered = pd.DataFrame(
        {"status": pd.Series(pd.Categorical(["active"], categories=["active"], ordered=False))}
    )
    ordered = pd.DataFrame(
        {"status": pd.Series(pd.Categorical(["active"], categories=["active"], ordered=True))}
    )

    left = fingerprint_silver_table(table_key="people", table=unordered)
    right = fingerprint_silver_table(table_key="people", table=ordered)

    assert left.schema_hash != right.schema_hash


def test_manifest_identifies_the_controlled_v1_schema_algorithm() -> None:
    table = fingerprint_silver_table(table_key="people", table=pd.DataFrame({"id": [1]}))
    dataset = combine_silver_table_fingerprints([table])

    assert SCHEMA_HASH_ALGORITHM == "metrka.logical-schema.sha256.canonical-types.v1"
    assert dataset.to_manifest_dict()["schema_algorithm"] == SCHEMA_HASH_ALGORITHM
