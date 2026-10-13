from __future__ import annotations

from pathlib import Path

import pytest

from metrka_core.storage.bronze_store import LocalBronzeArtifactStore
from metrka_core.storage.naming import pointer_file_name
from metrka_core.storage.silver_store import LocalSilverArtifactStore


def test_dataset_without_stream_has_no_artificial_dot() -> None:
    assert pointer_file_name("fdc_obis") == "dataset--fdc_obis.json"


def test_dataset_stream_preserves_hierarchy_separator() -> None:
    assert pointer_file_name("fdc_obis.inmates") == "dataset--fdc_obis.inmates.json"


def test_dot_and_underscore_identifiers_do_not_collide() -> None:
    assert pointer_file_name("workspace.people") != pointer_file_name("workspace_people")


@pytest.mark.parametrize(
    "dataset_id",
    [
        "",
        "Dataset.Stream",
        "dataset.",
        ".stream",
        "dataset..stream",
        "dataset/stream",
        "dataset\\stream",
        " dataset.stream",
        "dataset.stream ",
    ],
)
def test_invalid_dataset_ids_are_rejected(dataset_id: str) -> None:
    with pytest.raises(ValueError):
        pointer_file_name(dataset_id)


def test_bronze_and_silver_use_the_same_pointer_name(tmp_path: Path) -> None:
    current_root = tmp_path / "data" / "current"
    bronze_store = LocalBronzeArtifactStore(
        workspace_root=tmp_path,
        bronze_root=tmp_path / "data" / "files" / "bronze",
        current_root=current_root,
    )
    silver_store = LocalSilverArtifactStore(
        workspace_root=tmp_path,
        silver_root=tmp_path / "data" / "files" / "silver",
        current_root=current_root,
    )
    dataset_id = "fdc_obis.inmates"

    bronze_path = bronze_store.write_latest_pointer(
        dataset_id=dataset_id, payload={"dataset_id": dataset_id, "layer": "bronze"}
    )
    silver_path = silver_store.write_latest_pointer(
        dataset_id=dataset_id, payload={"dataset_id": dataset_id, "layer": "silver"}
    )

    expected_name = "dataset--fdc_obis.inmates.json"
    assert bronze_path.name == expected_name
    assert silver_path.name == expected_name
    assert bronze_path.parent == current_root / "latest" / "bronze"
    assert silver_path.parent == current_root / "latest" / "silver"
