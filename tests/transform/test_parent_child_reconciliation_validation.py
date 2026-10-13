from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.transform.validation import ContractValidationError, validate_contract_file


def _write_contract(
    tmp_path: Path, *, measure_column: str = "licensed_bed_count", measure_cast_type: str = "int"
) -> Path:
    contract_path = tmp_path / "contract.yaml"

    contract_path.write_text(
        yaml.safe_dump(
            {
                "tables": {
                    "adult_substance_abuse_beds": {
                        "columns": {
                            "County": {"rename_to": "geography_name", "cast_to": "string"},
                            "Count": {
                                "rename_to": "licensed_bed_count",
                                "cast_to": measure_cast_type,
                            },
                            "reporting_year": {"rename_to": "reporting_year", "cast_to": "int"},
                            "cid_id": {"rename_to": "indicator_id", "cast_to": "string"},
                        },
                        "parent_child_reconciliation": {
                            "group_by": ["reporting_year", "indicator_id"],
                            "parent": {"column": "geography_name", "equals": "Florida"},
                            "measure_column": measure_column,
                            "residual": {
                                "label_column": "geography_name",
                                "label_value": "Unallocated",
                            },
                            "remove_parent": True,
                            "negative_difference": "fail",
                        },
                        "canonical_order": [
                            "geography_name",
                            "licensed_bed_count",
                            "reporting_year",
                            "indicator_id",
                        ],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    return contract_path


def test_accepts_parent_child_reconciliation_configuration(tmp_path: Path) -> None:
    contract_path = _write_contract(tmp_path)

    validated = validate_contract_file(contract_path)

    reconciliation = validated["tables"]["adult_substance_abuse_beds"][
        "parent_child_reconciliation"
    ]

    assert reconciliation["measure_column"] == "licensed_bed_count"
    assert reconciliation["parent"] == {"column": "geography_name", "equals": "Florida"}
    assert reconciliation["residual"]["label_value"] == "Unallocated"


def test_rejects_unknown_reconciliation_measure_column(tmp_path: Path) -> None:
    contract_path = _write_contract(tmp_path, measure_column="missing_count")

    with pytest.raises(ContractValidationError, match="unknown measure_column 'missing_count'"):
        validate_contract_file(contract_path)


def test_rejects_non_numeric_reconciliation_measure_column(tmp_path: Path) -> None:
    contract_path = _write_contract(tmp_path, measure_cast_type="string")

    with pytest.raises(
        ContractValidationError,
        match=("measure_column 'licensed_bed_count' must use a numeric cast type"),
    ):
        validate_contract_file(contract_path)
