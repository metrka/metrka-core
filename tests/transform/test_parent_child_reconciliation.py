from __future__ import annotations

import pandas as pd
import pytest

from metrka_core.transform.ops.reconciliation import reconcile_parent_child_rows
from metrka_core.transform.schema import apply_transformation

CONFIG = {
    "group_by": ["reporting_year", "indicator_id"],
    "parent": {"column": "geography_name", "equals": "Florida"},
    "measure_column": "licensed_bed_count",
    "residual": {"label_column": "geography_name", "label_value": "Unallocated"},
    "remove_parent": True,
    "negative_difference": "fail",
}


def _table(*, florida: int, alachua: int, baker: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "geography_name": ["Florida", "Alachua", "Baker"],
            "licensed_bed_count": pd.Series([florida, alachua, baker], dtype="Int64"),
            "reporting_year": pd.Series([2002, 2002, 2002], dtype="Int64"),
            "indicator_id": pd.Series(["0321", "0321", "0321"], dtype="string"),
        }
    )


def test_removes_parent_when_child_sum_matches() -> None:
    source = _table(florida=30, alachua=10, baker=20)

    result = reconcile_parent_child_rows(source, CONFIG)

    assert result.data["geography_name"].tolist() == ["Alachua", "Baker"]
    assert result.data["licensed_bed_count"].tolist() == [10, 20]

    assert len(result.evidence) == 1
    assert result.evidence[0].affected_row_count == 0
    assert result.evidence[0].metrics["parent_total"] == 30
    assert result.evidence[0].metrics["child_total"] == 30
    assert result.evidence[0].metrics["difference"] == 0
    assert result.evidence[0].metrics["parent"] == {"column": "geography_name", "value": "Florida"}
    assert result.evidence[0].metrics["residual"] == {
        "column": "geography_name",
        "value": "Unallocated",
    }
    assert result.evidence[0].reason == (
        "The geography_name = Florida row was compared with the sum of the other rows "
        "as declared in the data contract."
    )


def test_adds_unallocated_row_for_positive_difference() -> None:
    source = _table(florida=35, alachua=10, baker=20)

    result = reconcile_parent_child_rows(source, CONFIG)

    assert result.data["geography_name"].tolist() == ["Alachua", "Baker", "Unallocated"]
    assert result.data["licensed_bed_count"].tolist() == [10, 20, 5]

    assert len(result.evidence) == 1
    assert result.evidence[0].affected_row_count == 1
    assert result.evidence[0].metrics["parent_total"] == 35
    assert result.evidence[0].metrics["child_total"] == 30
    assert result.evidence[0].metrics["difference"] == 5


def test_rejects_child_sum_greater_than_parent_total() -> None:
    source = _table(florida=25, alachua=10, baker=20)

    with pytest.raises(ValueError, match="Child total exceeds parent total"):
        reconcile_parent_child_rows(source, CONFIG)


def test_schema_transformation_applies_parent_child_reconciliation() -> None:
    source = pd.DataFrame(
        {
            "County": ["Florida", "Alachua", "Baker"],
            "Count": ["35", "10", "20"],
            "reporting_year": ["2002", "2002", "2002"],
            "cid_id": ["0321", "0321", "0321"],
        }
    )

    config = {
        "columns": {
            "County": {"rename_to": "geography_name", "cast_to": "string"},
            "Count": {"rename_to": "licensed_bed_count", "cast_to": "int"},
            "reporting_year": {"rename_to": "reporting_year", "cast_to": "int"},
            "cid_id": {"rename_to": "indicator_id", "cast_to": "string"},
        },
        "parent_child_reconciliation": CONFIG,
        "canonical_order": [
            "geography_name",
            "licensed_bed_count",
            "reporting_year",
            "indicator_id",
        ],
    }

    result = apply_transformation(source, config)

    assert result.data["geography_name"].tolist() == ["Alachua", "Baker", "Unallocated"]
    assert result.data["licensed_bed_count"].tolist() == [10, 20, 5]
    assert result.data.columns.tolist() == [
        "geography_name",
        "licensed_bed_count",
        "reporting_year",
        "indicator_id",
    ]

    reconciliation_evidence = [
        item for item in result.evidence if item.operation == "parent_child_reconciliation"
    ]

    assert len(reconciliation_evidence) == 1
    assert reconciliation_evidence[0].metrics["difference"] == 5


def test_reconciles_multiple_groups_with_missing_dimensions() -> None:
    source = pd.DataFrame(
        {
            "geography_name": pd.Series(
                ["Florida", "Alachua", "Baker", "Florida", "Alachua", "Baker"], dtype="string"
            ),
            "licensed_bed_count": pd.Series([35, 10, 20, 12, 5, 7], dtype="Int64"),
            "reporting_year": pd.Series([2002, 2002, 2002, 2003, 2003, 2003], dtype="Int64"),
            "year_breakdown": pd.Series(["single-year"] * 6, dtype="string"),
            "group_dimension": pd.Series([pd.NA] * 6, dtype="string"),
            "group_value": pd.Series([pd.NA] * 6, dtype="string"),
            "indicator_id": pd.Series(["0321"] * 6, dtype="string"),
        }
    )

    config = {
        **CONFIG,
        "group_by": [
            "reporting_year",
            "year_breakdown",
            "group_dimension",
            "group_value",
            "indicator_id",
        ],
    }

    result = reconcile_parent_child_rows(source, config)

    assert result.data["geography_name"].tolist() == [
        "Alachua",
        "Baker",
        "Unallocated",
        "Alachua",
        "Baker",
    ]
    assert result.data["licensed_bed_count"].tolist() == [10, 20, 5, 5, 7]
    assert result.data["reporting_year"].tolist() == [2002, 2002, 2002, 2003, 2003]

    assert len(result.evidence) == 2
    assert [item.metrics["difference"] for item in result.evidence] == [5, 0]

    assert result.evidence[0].metrics["group"]["group_dimension"] is None
    assert result.evidence[0].metrics["group"]["group_value"] is None


def test_rejects_group_without_parent_row() -> None:
    source = _table(florida=30, alachua=10, baker=20)
    source.loc[source["geography_name"] == "Florida", "geography_name"] = "Miami-Dade"

    with pytest.raises(ValueError, match="requires exactly one parent row"):
        reconcile_parent_child_rows(source, CONFIG)


def test_rejects_group_with_multiple_parent_rows() -> None:
    source = _table(florida=30, alachua=10, baker=20)

    duplicate_parent = source.iloc[[0]].copy()
    source = pd.concat([source, duplicate_parent], ignore_index=True)

    with pytest.raises(ValueError, match="requires exactly one parent row"):
        reconcile_parent_child_rows(source, CONFIG)


def test_rejects_missing_child_measure() -> None:
    source = _table(florida=30, alachua=10, baker=20)
    source.loc[source["geography_name"] == "Baker", "licensed_bed_count"] = pd.NA

    with pytest.raises(ValueError, match="child totals must not be missing"):
        reconcile_parent_child_rows(source, CONFIG)
