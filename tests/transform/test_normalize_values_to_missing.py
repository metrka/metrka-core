"""normalize_values with replace_with: null publishes a source's placeholder text as missing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from metrka_core.lineage.transformation.models import TransformationObservation
from metrka_core.transform.schema import apply_transformation
from metrka_core.transform.validation import ContractValidationError, validate_contract_file

SUPPRESSED_REASON = 'The source publishes some small counts as "Suppressed".'


def _write_contract(tmp_path: Path, mappings: dict[str, object]) -> Path:
    contract = {
        "tables": {
            "counts": {
                "columns": {
                    "County": {"rename_to": "county", "cast_to": "string"},
                    "Count": {
                        "rename_to": "hospitalizations_count",
                        "cast_to": "int",
                        "normalize_values": {"reason": SUPPRESSED_REASON, "mappings": mappings},
                    },
                },
                "canonical_order": ["county", "hospitalizations_count"],
            }
        }
    }
    path = tmp_path / "contract.yaml"
    path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return path


def test_a_placeholder_becomes_missing_and_the_column_keeps_its_number_type(tmp_path: Path) -> None:
    contract = validate_contract_file(
        _write_contract(
            tmp_path, {"Suppressed": {"replace_with": None}, "1,121": {"replace_with": "1121"}}
        )
    )
    source = pd.DataFrame(
        {"County": ["Moore", "Shelby", "Knox"], "Count": ["Suppressed", "1,121", "87"]}, dtype=str
    )

    result = apply_transformation(source, contract["tables"]["counts"])

    counts = result.data["hospitalizations_count"]
    assert str(counts.dtype) == "Int64"
    assert counts.isna().tolist() == [True, False, False]
    assert counts.dropna().tolist() == [1121, 87]

    changes = {
        observation.before_value: observation
        for observation in result.evidence
        if isinstance(observation, TransformationObservation)
        and observation.operation == "normalize_values"
    }
    assert changes["Suppressed"].after_value is None
    assert changes["Suppressed"].affected_row_count == 1
    assert changes["Suppressed"].meta["reason"] == SUPPRESSED_REASON
    assert changes["1,121"].after_value == "1121"


def test_a_list_or_mapping_is_still_not_a_replacement(tmp_path: Path) -> None:
    with pytest.raises(ContractValidationError, match="scalar value or null"):
        validate_contract_file(_write_contract(tmp_path, {"Suppressed": {"replace_with": []}}))
