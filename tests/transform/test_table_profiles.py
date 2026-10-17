"""Contract tables that reuse one structure by naming a profile."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from metrka_core.catalog.glossary import Glossary, published_contract_bytes
from metrka_core.pipeline.config import load_table_cfg
from metrka_core.transform.profiles import ProfileError, load_contract
from metrka_core.transform.schema import apply_transformation
from metrka_core.transform.validation import ContractValidationError, validate_contract_file

COUNTY_COUNT = {
    "version": 1,
    "table": {
        "columns": {
            "County": {
                "rename_to": "geography_name",
                "cast_to": "string",
                "meta": {"description": "Florida county name."},
            },
            "Count": {
                "rename_to": "event_count",
                "cast_to": "int",
                "meta": {"description": "Number of events."},
            },
        },
        "canonical_order": ["geography_name", "event_count"],
    },
}


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _contract(table: dict[str, Any]) -> dict[str, Any]:
    return {"meta": {"version": "1", "info": "Beds."}, "tables": {"beds": table}}


def _source(
    tmp_path: Path, table: dict[str, Any], *, profile: dict[str, Any] = COUNTY_COUNT
) -> Path:
    """A source folder with one profile and one dataset folder whose contract uses it."""

    _write(tmp_path / "source" / "profiles" / "county_count.yaml", profile)
    return _write(tmp_path / "source" / "beds" / "conf" / "beds.yaml", _contract(table))


def test_a_table_takes_its_structure_from_the_profile(tmp_path: Path) -> None:
    contract_path = _source(
        tmp_path,
        {
            "uses": "county_count",
            "display_name": "Licensed beds by county",
            "columns": {"Count": {"meta": {"description": "Number of licensed beds."}}},
        },
    )

    table = validate_contract_file(contract_path)["tables"]["beds"]

    assert table["profile"] == "county_count"
    assert table["display_name"] == "Licensed beds by county"
    assert table["canonical_order"] == ["geography_name", "event_count"]
    assert table["columns"]["Count"] == {
        "rename_to": "event_count",
        "cast_to": "int",
        "meta": {"description": "Number of licensed beds."},
    }
    assert table["columns"]["County"]["meta"] == {"description": "Florida county name."}


def test_a_table_cannot_change_the_structure_of_its_profile(tmp_path: Path) -> None:
    contract_path = _source(
        tmp_path,
        {
            "uses": "county_count",
            "canonical_order": ["event_count", "geography_name"],
            "columns": {
                "Count": {"rename_to": "bed_count"},
                "Rate": {"meta": {"description": "Not in the profile."}},
            },
        },
    )

    with pytest.raises(ContractValidationError) as raised:
        validate_contract_file(contract_path)

    message = str(raised.value)
    assert "['canonical_order'] come from profile 'county_count'" in message
    assert "column 'Count' may set only ['meta']" in message
    assert "column 'Rate' is not in profile 'county_count'" in message


def test_a_missing_profile_names_where_it_was_looked_for(tmp_path: Path) -> None:
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml", _contract({"uses": "county_rate"})
    )

    with pytest.raises(ProfileError, match="profile 'county_rate' was not found"):
        load_contract(contract_path)


def test_the_nearest_profile_wins(tmp_path: Path) -> None:
    contract_path = _source(tmp_path, {"uses": "county_count"})
    nearer = {"version": 1, "table": {**COUNTY_COUNT["table"], "info": "Dataset folder profile."}}
    _write(tmp_path / "source" / "beds" / "profiles" / "county_count.yaml", nearer)

    assert load_contract(contract_path)["tables"]["beds"]["info"] == "Dataset folder profile."


def test_a_flat_workspace_keeps_profiles_beside_conf(tmp_path: Path) -> None:
    _write(tmp_path / "workspace" / "profiles" / "county_count.yaml", COUNTY_COUNT)
    contract_path = _write(
        tmp_path / "workspace" / "conf" / "beds.yaml", _contract({"uses": "county_count"})
    )

    assert load_contract(contract_path)["tables"]["beds"]["profile"] == "county_count"


def test_a_table_is_built_the_same_with_or_without_its_profile(tmp_path: Path) -> None:
    with_profile = _source(tmp_path, {"uses": "county_count"})
    written_out = _write(
        tmp_path / "plain" / "conf" / "beds.yaml", _contract(dict(COUNTY_COUNT["table"]))
    )
    source = pd.DataFrame({"County": ["Alachua", "Baker"], "Count": ["18", "0"]})

    from_profile = apply_transformation(source, load_table_cfg(with_profile, table_key="beds"))
    from_contract = apply_transformation(source, load_table_cfg(written_out, table_key="beds"))

    pd.testing.assert_frame_equal(from_profile.data, from_contract.data)


def test_the_snapshot_keeps_the_table_as_it_was_built(tmp_path: Path) -> None:
    contract_path = _source(tmp_path, {"uses": "county_count"})

    first = published_contract_bytes(contract_path, Glossary())
    published = yaml.safe_load(first)
    assert published["tables"]["beds"]["columns"]["Count"]["rename_to"] == "event_count"
    assert "uses" not in published["tables"]["beds"]

    changed = {"version": 1, "table": {**COUNTY_COUNT["table"], "info": "Reworded."}}
    _write(tmp_path / "source" / "profiles" / "county_count.yaml", changed)
    second = published_contract_bytes(contract_path, Glossary())

    assert hashlib.sha256(first).hexdigest() != hashlib.sha256(second).hexdigest()


def test_a_contract_without_profiles_is_published_unchanged(tmp_path: Path) -> None:
    contract_path = _write(tmp_path / "conf" / "beds.yaml", _contract(dict(COUNTY_COUNT["table"])))

    assert published_contract_bytes(contract_path, Glossary()) == contract_path.read_bytes()


COUNTY_RATE = {
    "version": 1,
    "renamable": ["Count"],
    "table": {
        "columns": {
            "County": {"rename_to": "geography_name", "cast_to": "string"},
            "Year": {"rename_to": "reporting_year", "cast_to": "int"},
            "Count": {"rename_to": "count", "cast_to": "int"},
        },
        "parent_child_reconciliation": {
            "group_by": ["reporting_year"],
            "parent": {"column": "geography_name", "equals": "Florida"},
            "measure_column": "count",
            "residual": {"label_column": "geography_name", "label_value": "Unallocated"},
            "remove_parent": True,
            "negative_difference": "fail",
        },
        "canonical_order": ["geography_name", "count", "reporting_year"],
    },
}


def test_a_renamable_measure_takes_its_meaning_in_the_contract(tmp_path: Path) -> None:
    contract_path = _source(
        tmp_path,
        {"uses": "county_count", "columns": {"Count": {"rename_to": "licensed_bed_count"}}},
        profile=COUNTY_RATE,
    )

    table = validate_contract_file(contract_path)["tables"]["beds"]

    assert table["columns"]["Count"]["rename_to"] == "licensed_bed_count"
    assert table["canonical_order"] == ["geography_name", "licensed_bed_count", "reporting_year"]
    assert table["parent_child_reconciliation"]["measure_column"] == "licensed_bed_count"

    source = pd.DataFrame(
        {"County": ["Florida", "Alachua"], "Year": ["2002", "2002"], "Count": ["18", "18"]}
    )
    built = apply_transformation(source, load_table_cfg(contract_path, table_key="beds"))
    assert list(built.data.columns) == ["geography_name", "licensed_bed_count", "reporting_year"]
    assert built.data["geography_name"].tolist() == ["Alachua"]


def test_a_shared_key_keeps_its_name(tmp_path: Path) -> None:
    contract_path = _source(
        tmp_path,
        {"uses": "county_count", "columns": {"County": {"rename_to": "county"}}},
        profile=COUNTY_RATE,
    )

    with pytest.raises(ContractValidationError, match=r"column 'County' may set only \['meta'\]"):
        validate_contract_file(contract_path)


def test_a_profile_can_only_free_its_own_columns(tmp_path: Path) -> None:
    contract_path = _source(
        tmp_path, {"uses": "county_count"}, profile={**COUNTY_RATE, "renamable": ["Rate"]}
    )

    with pytest.raises(ContractValidationError, match=r"renamable columns \['Rate'\] are not in"):
        validate_contract_file(contract_path)


CHARTS_SOURCE = {
    "version": 1,
    "meta": {
        "category": "health-medicine",
        "source": {"publisher": "Florida Department of Health", "update_frequency": "annual"},
        "methodology": {"timing": "As of December.", "limitations": ["Shared source caveat."]},
    },
}

RATE_SHAPE = {
    **COUNTY_COUNT,
    "methodology": {"limitations": ["Rate caveat.", "Shared source caveat."]},
}


def _profiles(tmp_path: Path, **profiles: dict[str, Any]) -> None:
    for name, profile in profiles.items():
        _write(tmp_path / "source" / "profiles" / f"{name}.yaml", profile)


def test_a_dataset_says_what_its_source_profile_says_unless_it_says_otherwise(
    tmp_path: Path,
) -> None:
    _profiles(tmp_path, charts_source=CHARTS_SOURCE, county_count=COUNTY_COUNT)
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml",
        {
            "meta": {
                "uses": "charts_source",
                "title": "Beds",
                "source": {"url": "https://example.test/0321"},
                "methodology": {"timing": "As of December 31."},
            },
            "tables": {"beds": {"uses": "county_count"}},
        },
    )

    meta = load_contract(contract_path)["meta"]

    assert meta["profile"] == "charts_source"
    assert "uses" not in meta
    assert meta["title"] == "Beds"
    assert meta["category"] == "health-medicine"
    assert meta["source"] == {
        "publisher": "Florida Department of Health",
        "update_frequency": "annual",
        "url": "https://example.test/0321",
    }
    assert meta["methodology"]["timing"] == "As of December 31."


def test_limitations_add_up_from_source_then_table_shape_then_dataset(tmp_path: Path) -> None:
    _profiles(tmp_path, charts_source=CHARTS_SOURCE, county_rate=RATE_SHAPE)
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml",
        {
            "meta": {"uses": "charts_source", "methodology": {"limitations": ["Beds caveat."]}},
            "tables": {"beds": {"uses": "county_rate"}, "beds_again": {"uses": "county_rate"}},
        },
    )

    limitations = load_contract(contract_path)["meta"]["methodology"]["limitations"]

    # Each sentence once, in the order source, table shape, dataset.
    assert limitations == ["Shared source caveat.", "Rate caveat.", "Beds caveat."]


def test_a_dataset_reads_in_its_own_order_before_what_profiles_add(tmp_path: Path) -> None:
    shape_with_rule = {
        **RATE_SHAPE,
        "methodology": {
            "rules": {"reconciliation": {"description": "Florida row compared with counties."}},
            "limitations": ["Rate caveat."],
        },
    }
    _profiles(tmp_path, charts_source=CHARTS_SOURCE, county_rate=shape_with_rule)
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml",
        {
            "meta": {
                "uses": "charts_source",
                "title": "Beds",
                "methodology": {
                    "counts": "Licensed beds.",
                    "does_not_show": ["Occupied beds"],
                    "rules": {"reconciliation": {"affects": ["licensed_bed_count"]}},
                },
            },
            "tables": {"beds": {"uses": "county_rate"}},
        },
    )

    methodology = load_contract(contract_path)["meta"]["methodology"]

    # The site lists methodology in this order: what the dataset says comes first.
    assert list(methodology) == ["counts", "does_not_show", "rules", "limitations", "timing"]
    assert list(methodology["rules"]["reconciliation"]) == ["affects", "description"]


def test_a_shape_without_a_rate_brings_no_rate_caveats(tmp_path: Path) -> None:
    _profiles(tmp_path, county_count=COUNTY_COUNT, county_rate=RATE_SHAPE)
    count_contract = _write(
        tmp_path / "source" / "counts" / "conf" / "counts.yaml",
        {"meta": {"title": "Counts"}, "tables": {"counts": {"uses": "county_count"}}},
    )
    rate_contract = _write(
        tmp_path / "source" / "rates" / "conf" / "rates.yaml",
        {"meta": {"title": "Rates"}, "tables": {"rates": {"uses": "county_rate"}}},
    )

    assert "methodology" not in load_contract(count_contract)["meta"]
    assert load_contract(rate_contract)["meta"]["methodology"]["limitations"] == [
        "Rate caveat.",
        "Shared source caveat.",
    ]


def test_a_profile_is_used_for_what_it_holds(tmp_path: Path) -> None:
    _profiles(tmp_path, charts_source=CHARTS_SOURCE, county_count=COUNTY_COUNT)
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml",
        {"meta": {"uses": "county_count"}, "tables": {"beds": {"uses": "charts_source"}}},
    )

    with pytest.raises(ContractValidationError) as raised:
        validate_contract_file(contract_path)

    message = str(raised.value)
    assert "meta: profile 'county_count' has no 'meta'" in message
    assert "tables.beds: profile 'charts_source' has no 'table'" in message


def test_the_snapshot_keeps_the_meta_as_it_was_published(tmp_path: Path) -> None:
    _profiles(tmp_path, charts_source=CHARTS_SOURCE)
    contract_path = _write(
        tmp_path / "source" / "beds" / "conf" / "beds.yaml",
        {
            "meta": {"uses": "charts_source", "title": "Beds"},
            "tables": _contract(dict(COUNTY_COUNT["table"]))["tables"],
        },
    )

    published = yaml.safe_load(published_contract_bytes(contract_path, Glossary()))

    assert published["meta"]["source"]["publisher"] == "Florida Department of Health"
    assert published["meta"]["profile"] == "charts_source"
