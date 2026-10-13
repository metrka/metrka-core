"""Behavioral tests for public contract-snapshot reading."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from metrka_core.api import (
    ContractSnapshotFailure,
    ContractSnapshotReadError,
    create_contract_snapshot_reader,
)

SNAPSHOT_RELATIVE_PATH = (
    "contracts/demo.main/"
    "sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/"
    "contract.json"
)


def _snapshot_path(data_root: Path) -> Path:
    return data_root.joinpath(*SNAPSHOT_RELATIVE_PATH.split("/"))


def _write_snapshot(data_root: Path, payload: object) -> Path:
    snapshot_path = _snapshot_path(data_root)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(payload), encoding="utf-8")
    return snapshot_path


def test_public_reader_reads_json_object_from_contract_storage(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_snapshot(data_root, {"meta": {"methodology": {"scope": "public"}}})

    reader = create_contract_snapshot_reader(data_root=data_root)

    assert reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH) == {
        "meta": {"methodology": {"scope": "public"}}
    }


def test_public_reader_requires_pathlib_data_root(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="data_root must be a pathlib.Path"):
        create_contract_snapshot_reader(data_root=str(tmp_path))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "../contract.json",
        "/contracts/demo/contract.json",
        "C:/contracts/demo/contract.json",
        "contracts\\demo\\contract.json",
        "contracts//demo/contract.json",
        "./contracts/demo/contract.json",
        ".",
        "contracts/CON.json",
    ],
)
def test_public_reader_rejects_unsafe_or_noncanonical_paths(
    tmp_path: Path, unsafe_path: str
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=unsafe_path)

    assert captured.value.reason is ContractSnapshotFailure.UNSAFE_PATH
    assert captured.value.path == unsafe_path


def test_public_reader_rejects_json_outside_contract_storage(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest_path = data_root / "files" / "silver" / "manifests" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}", encoding="utf-8")
    (data_root / "contracts").mkdir()
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path="files/silver/manifests/manifest.json")

    assert captured.value.reason is ContractSnapshotFailure.OUTSIDE_STORAGE


def test_public_reader_rejects_symlink_escape(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    contracts_root = data_root / "contracts"
    contracts_root.mkdir(parents=True)
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    (outside_root / "contract.json").write_text("{}", encoding="utf-8")
    linked_root = contracts_root / "linked"

    try:
        linked_root.symlink_to(outside_root, target_is_directory=True)
    except OSError:
        pytest.skip("Creating directory symlinks is not available in this environment")

    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path="contracts/linked/contract.json")

    assert captured.value.reason is ContractSnapshotFailure.OUTSIDE_STORAGE


def test_public_reader_reports_missing_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "missing"
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH)

    assert captured.value.reason is ContractSnapshotFailure.NOT_FOUND


def test_public_reader_reports_missing_snapshot(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "contracts").mkdir(parents=True)
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH)

    assert captured.value.reason is ContractSnapshotFailure.NOT_FOUND


def test_public_reader_rejects_invalid_json(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    snapshot_path = _snapshot_path(data_root)
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text("{", encoding="utf-8")
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH)

    assert captured.value.reason is ContractSnapshotFailure.INVALID_JSON


def test_public_reader_rejects_non_object_json(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_snapshot(data_root, ["not", "an", "object"])
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH)

    assert captured.value.reason is ContractSnapshotFailure.INVALID_OBJECT


def test_public_reader_rejects_non_utf8_snapshot(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    snapshot_path = _snapshot_path(data_root)
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_bytes(b"\xff\xfe")
    reader = create_contract_snapshot_reader(data_root=data_root)

    with pytest.raises(ContractSnapshotReadError) as captured:
        reader.read_snapshot(path=SNAPSHOT_RELATIVE_PATH)

    assert captured.value.reason is ContractSnapshotFailure.READ_FAILED
