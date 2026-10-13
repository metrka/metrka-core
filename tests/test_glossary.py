"""Shared definitions referenced from contract texts as ``[[term]]``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from metrka_core.catalog.glossary import (
    Glossary,
    TermScope,
    load_glossary,
    published_contract_bytes,
)
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.pipeline.silver.silver_artifacts import (
    contract_snapshot_metadata,
    snapshot_contract,
)
from metrka_core.storage.contract_store import LocalContractSnapshotStore

SOURCE_GLOSSARY = {
    "version": 1,
    "terms": {
        "adult": "Age 18 or older.",
        "variability_measure": {
            "label": "Variability measure",
            "definition": "Standard error of the rate for [[adult|adults]].",
        },
        "unused": "Not referenced by the contract.",
    },
}


def _contract(info: str, **meta: Any) -> dict[str, Any]:
    return {
        "meta": {"version": "1", "info": info, **meta},
        "tables": {"beds": {"columns": {}, "canonical_order": []}},
    }


def _write(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def _glossary(tmp_path: Path, document: dict[str, Any] = SOURCE_GLOSSARY) -> Glossary:
    return load_glossary(_write(tmp_path / "glossary.yaml", document))


def test_contract_uses_terms_from_every_level(tmp_path: Path) -> None:
    contract = _contract(
        "Beds for [[adult|adults]], with a [[variability_measure]] and [[unallocated]] rows.",
        glossary={"licensed_bed": "A bed AHCA authorizes for one type of care."},
    )

    terms = _glossary(tmp_path).terms_used_by(contract, where="beds.yaml")

    assert list(terms) == ["adult", "licensed_bed", "unallocated", "variability_measure"]
    assert terms["adult"].scope is TermScope.SOURCE
    assert terms["licensed_bed"].scope is TermScope.DATASET
    assert terms["unallocated"].scope is TermScope.METRKA
    assert terms["variability_measure"].label == "Variability measure"


def test_a_definition_brings_the_terms_it_uses(tmp_path: Path) -> None:
    terms = _glossary(tmp_path).terms_used_by(
        _contract("Rates with a [[variability_measure]]."), where="beds.yaml"
    )

    assert list(terms) == ["adult", "variability_measure"]


def test_every_term_problem_is_reported(tmp_path: Path) -> None:
    contract = _contract("[[child]] and [[Adult]].", glossary={"adult": "Defined twice."})
    contract["glossary"] = {}

    with pytest.raises(ValueError) as raised:
        _glossary(tmp_path).terms_used_by(contract, where="beds.yaml")

    message = str(raised.value)
    assert "[[child]] is not defined in any glossary" in message
    assert "[[Adult]] must name a term key" in message
    assert "'adult' is already defined as a source term" in message
    assert "top-level 'glossary' is reserved" in message


def test_a_source_cannot_redefine_a_metrka_term(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'unallocated' is a Metrka term"):
        _glossary(tmp_path, {"version": 1, "terms": {"unallocated": "Unknown county."}})


def test_a_workspace_without_glossary_has_metrka_terms(tmp_path: Path) -> None:
    glossary = load_glossary(tmp_path / "glossary.yaml")

    assert list(glossary.terms) == ["unallocated"]


def test_a_contract_without_terms_is_published_unchanged(tmp_path: Path) -> None:
    contract_path = _write(tmp_path / "conf" / "beds.yaml", _contract("Licensed beds."))

    assert published_contract_bytes(contract_path, _glossary(tmp_path)) == (
        contract_path.read_bytes()
    )


def test_the_snapshot_keeps_the_definitions_it_used(tmp_path: Path) -> None:
    contract_path = _write(tmp_path / "conf" / "beds.yaml", _contract("Beds for [[adult|adults]]."))
    store = LocalContractSnapshotStore(
        definition_root=tmp_path,
        data_root=tmp_path / "data",
        snapshots_root=tmp_path / "data" / "contracts",
        glossary=_glossary(tmp_path),
    )

    paths = snapshot_contract(
        contract_store=store, dataset_id="src.beds", contract_path=contract_path
    )

    published = yaml.safe_load(paths.yaml_path.read_text(encoding="utf-8"))
    assert published["meta"]["info"] == "Beds for [[adult|adults]]."
    assert published["glossary"] == {
        "adult": {"label": "Adult", "definition": "Age 18 or older.", "scope": "source"}
    }


def test_a_changed_definition_makes_a_new_snapshot(tmp_path: Path) -> None:
    contract_path = _write(tmp_path / "conf" / "beds.yaml", _contract("Beds for [[adult|adults]]."))

    def snapshot_hash(definition: str) -> str:
        store = LocalContractSnapshotStore(
            definition_root=tmp_path,
            data_root=tmp_path / "data",
            snapshots_root=tmp_path / "data" / "contracts",
            glossary=_glossary(tmp_path, {"version": 1, "terms": {"adult": definition}}),
        )
        return contract_snapshot_metadata(
            contract_store=store, dataset_id="src.beds", contract_path=contract_path
        )["contract_hash"]

    assert snapshot_hash("Age 18 or older.") != snapshot_hash("Age 19 or older.")


def test_the_glossary_sits_beside_the_dataset_folders(tmp_path: Path) -> None:
    source = WorkspaceLocation.portable(
        workspace_name="src", workspace_root=tmp_path / "src", dataset_folders=True
    )
    flat = WorkspaceLocation.portable(workspace_name="flat", workspace_root=tmp_path / "flat")

    assert source.dataset_folder("beds").glossary_path == (tmp_path / "src" / "glossary.yaml")
    assert flat.glossary_path == (tmp_path / "flat" / "glossary.yaml")
