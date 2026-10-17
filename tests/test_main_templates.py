"""Dataset main.yaml files that reuse one configuration by naming a template."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from metrka_core.datasets.main_templates import MainTemplateError, load_main_config
from metrka_core.datasets.source_config import load_source_config, require_dataset_folder_streams

FILENAME_REGEX = (
    r"^cid(?P<cid_id>[0-9]+)__(?P<year_breakdown>[^_]+)__default__all__(?P<year>[0-9]{4})[.]xlsx$"
)

CHARTS_YEARLY_COUNTY = {
    "version": 1,
    "inputs": {
        "cid": {"description": "CHARTS indicator number, four digits."},
        "table": {"description": "Silver table the period is read from.", "default": "${folder}"},
        "timeout_seconds": {"default": 120},
    },
    "main": {
        "workspace_name": "fl_healthcharts",
        "streams": {
            "${folder}": {
                "official_filename": "cid${cid}__single-year__default__all__*.xlsx",
                "download_url": "https://example.test/Dataviewer&cid=${cid}",
                "yaml_contract_name": "contract.yaml",
                "filename_metadata": {
                    "regex": FILENAME_REGEX,
                    "columns": {"reporting_year": {"from_group": "year", "type": "integer"}},
                    "member_key": ["reporting_year"],
                },
                "silver": {
                    "partition_by": "version_period",
                    "temporal_coverage": {"table": "${table}", "column": "reporting_year"},
                },
            }
        },
        "pipeline": {
            "acquisition": {
                "extractor": "http.files",
                "options": {"timeout_seconds": "${timeout_seconds}"},
            },
            "steps": [{"action": "bronze.ingest"}, {"action": "silver.process"}],
        },
    },
}


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _source(
    tmp_path: Path, main: dict[str, Any], *, template: dict[str, Any] = CHARTS_YEARLY_COUNTY
) -> Path:
    """A source folder with one template and one dataset folder whose main.yaml uses it."""

    _write(tmp_path / "fl_healthcharts" / "templates" / "charts_yearly_county.yaml", template)
    return _write(tmp_path / "fl_healthcharts" / "beds" / "conf" / "main.yaml", main)


def test_a_dataset_main_yaml_is_filled_in_from_its_template(tmp_path: Path) -> None:
    main_path = _source(tmp_path, {"uses": "charts_yearly_county", "with": {"cid": "0321"}})

    config = load_main_config(main_path)

    stream = config["streams"]["beds"]
    assert config["workspace_name"] == "fl_healthcharts"
    assert stream["official_filename"] == "cid0321__single-year__default__all__*.xlsx"
    assert stream["download_url"] == "https://example.test/Dataviewer&cid=0321"
    # A default may name the folder; a regex keeps its own braces and dollar sign.
    assert stream["silver"]["temporal_coverage"]["table"] == "beds"
    assert stream["filename_metadata"]["regex"] == FILENAME_REGEX
    # A value that is only a placeholder keeps the input's type.
    assert config["pipeline"]["acquisition"]["options"]["timeout_seconds"] == 120


def test_the_filled_in_configuration_is_what_the_pipeline_reads(tmp_path: Path) -> None:
    main_path = _source(tmp_path, {"uses": "charts_yearly_county", "with": {"cid": "0321"}})

    source_config = load_source_config(main_path, expected_ws_name="fl_healthcharts")

    require_dataset_folder_streams(source_config, dataset_name="beds")
    assert source_config.streams["beds"].yaml_contract_name == "contract.yaml"


def test_with_overrides_a_default(tmp_path: Path) -> None:
    main_path = _source(
        tmp_path,
        {"uses": "charts_yearly_county", "with": {"cid": "0321", "table": "beds_by_county"}},
    )

    table = load_main_config(main_path)["streams"]["beds"]["silver"]["temporal_coverage"]["table"]

    assert table == "beds_by_county"


def test_a_main_yaml_without_a_template_is_read_unchanged(tmp_path: Path) -> None:
    plain = {"workspace_name": "fl_healthcharts", "streams": {"beds": {"official_filename": "x"}}}
    main_path = _write(tmp_path / "beds" / "conf" / "main.yaml", plain)

    assert load_main_config(main_path) == plain


def test_a_dataset_sets_only_uses_and_with(tmp_path: Path) -> None:
    main_path = _source(
        tmp_path,
        {
            "uses": "charts_yearly_county",
            "with": {"cid": "0321", "cdi": "0322"},
            "pipeline": {"steps": []},
        },
    )

    with pytest.raises(MainTemplateError) as raised:
        load_main_config(main_path)

    message = str(raised.value)
    assert "['pipeline'] come from template 'charts_yearly_county'" in message
    assert "with ['cdi'] are not inputs of template 'charts_yearly_county'" in message


def test_an_input_without_a_default_must_be_given(tmp_path: Path) -> None:
    main_path = _source(tmp_path, {"uses": "charts_yearly_county"})

    with pytest.raises(MainTemplateError, match="needs input 'cid' in 'with'"):
        load_main_config(main_path)


def test_a_template_declares_every_placeholder_it_uses(tmp_path: Path) -> None:
    template = {
        **CHARTS_YEARLY_COUNTY,
        "main": {**CHARTS_YEARLY_COUNTY["main"], "workspace_name": "${source}"},
    }
    main_path = _source(
        tmp_path, {"uses": "charts_yearly_county", "with": {"cid": "0321"}}, template=template
    )

    with pytest.raises(MainTemplateError, match=r"placeholders \['source'\] are not inputs"):
        load_main_config(main_path)


def test_a_missing_template_names_where_it_was_looked_for(tmp_path: Path) -> None:
    main_path = _write(
        tmp_path / "fl_healthcharts" / "beds" / "conf" / "main.yaml", {"uses": "nope"}
    )

    with pytest.raises(MainTemplateError) as raised:
        load_main_config(main_path)

    message = str(raised.value)
    assert "template 'nope' was not found" in message
    assert str(tmp_path / "fl_healthcharts" / "templates" / "nope.yaml") in message


def test_the_nearest_template_wins(tmp_path: Path) -> None:
    main_path = _source(tmp_path, {"uses": "charts_yearly_county", "with": {"cid": "0321"}})
    nearer = {
        **CHARTS_YEARLY_COUNTY,
        "main": {**CHARTS_YEARLY_COUNTY["main"], "workspace_name": "dataset_folder_template"},
    }
    _write(
        tmp_path / "fl_healthcharts" / "beds" / "templates" / "charts_yearly_county.yaml", nearer
    )

    assert load_main_config(main_path)["workspace_name"] == "dataset_folder_template"


def test_a_template_cannot_use_another_template(tmp_path: Path) -> None:
    template = {"version": 1, "main": {"uses": "other"}}
    main_path = _source(tmp_path, {"uses": "charts_yearly_county"}, template=template)

    with pytest.raises(MainTemplateError, match="a template cannot use another template"):
        load_main_config(main_path)


def test_folder_is_given_by_metrka(tmp_path: Path) -> None:
    template = {**CHARTS_YEARLY_COUNTY, "inputs": {"folder": {"default": "x"}}}
    main_path = _source(tmp_path, {"uses": "charts_yearly_county"}, template=template)

    with pytest.raises(MainTemplateError, match="input 'folder' is given by Metrka"):
        load_main_config(main_path)
