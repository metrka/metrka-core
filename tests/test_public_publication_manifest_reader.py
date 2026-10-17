"""Behavioral tests for public publication-manifest reading."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from metrka_core.api import (
    PublicationManifestFailure,
    PublicationManifestReadError,
    create_publication_manifest_reader,
)

MANIFEST_RELATIVE_PATH = "files/silver/manifests/demo/main/manifest.json"


def _manifest_path(data_root: Path) -> Path:
    return data_root.joinpath(*MANIFEST_RELATIVE_PATH.split("/"))


def _write_manifest(data_root: Path, payload: object) -> Path:
    manifest_path = _manifest_path(data_root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    return manifest_path


def test_public_reader_reads_json_object_from_silver_manifest_storage(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_manifest(data_root, {"dataset_id": "demo.main", "tables": []})

    reader = create_publication_manifest_reader(data_root=data_root)

    assert reader.read_manifest(path=MANIFEST_RELATIVE_PATH) == {
        "dataset_id": "demo.main",
        "tables": [],
    }


def test_public_reader_requires_pathlib_data_root(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="data_root must be a pathlib.Path"):
        create_publication_manifest_reader(data_root=str(tmp_path))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "../manifest.json",
        "/files/silver/manifests/manifest.json",
        "C:/files/silver/manifests/manifest.json",
        "files\\silver\\manifests\\manifest.json",
        "files//silver/manifests/manifest.json",
        "./files/silver/manifests/manifest.json",
        ".",
        "files/silver/manifests/CON.json",
    ],
)
def test_public_reader_rejects_unsafe_or_noncanonical_paths(
    tmp_path: Path, unsafe_path: str
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=unsafe_path)

    assert captured.value.reason is PublicationManifestFailure.UNSAFE_PATH
    assert captured.value.path == unsafe_path


def test_public_reader_rejects_json_outside_silver_manifest_storage(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    receipt_path = data_root / "receipts" / "execution.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text("{}", encoding="utf-8")
    (data_root / "files" / "silver" / "manifests").mkdir(parents=True)
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path="receipts/execution.json")

    assert captured.value.reason is PublicationManifestFailure.OUTSIDE_STORAGE


def test_public_reader_rejects_symlink_escape(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifests_root = data_root / "files" / "silver" / "manifests"
    manifests_root.mkdir(parents=True)
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    (outside_root / "manifest.json").write_text("{}", encoding="utf-8")
    linked_root = manifests_root / "linked"

    try:
        linked_root.symlink_to(outside_root, target_is_directory=True)
    except OSError:
        pytest.skip("Creating directory symlinks is not available in this environment")

    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path="files/silver/manifests/linked/manifest.json")

    assert captured.value.reason is PublicationManifestFailure.OUTSIDE_STORAGE


def test_public_reader_reports_missing_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "missing"
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=MANIFEST_RELATIVE_PATH)

    assert captured.value.reason is PublicationManifestFailure.NOT_FOUND


def test_public_reader_reports_missing_manifest(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "files" / "silver" / "manifests").mkdir(parents=True)
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=MANIFEST_RELATIVE_PATH)

    assert captured.value.reason is PublicationManifestFailure.NOT_FOUND


def test_public_reader_rejects_invalid_json(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest_path = _manifest_path(data_root)
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{", encoding="utf-8")
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=MANIFEST_RELATIVE_PATH)

    assert captured.value.reason is PublicationManifestFailure.INVALID_JSON


def test_public_reader_rejects_non_object_json(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_manifest(data_root, ["not", "an", "object"])
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=MANIFEST_RELATIVE_PATH)

    assert captured.value.reason is PublicationManifestFailure.INVALID_OBJECT


def test_public_reader_rejects_non_utf8_manifest(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    manifest_path = _manifest_path(data_root)
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(b"\xff\xfe")
    reader = create_publication_manifest_reader(data_root=data_root)

    with pytest.raises(PublicationManifestReadError) as captured:
        reader.read_manifest(path=MANIFEST_RELATIVE_PATH)

    assert captured.value.reason is PublicationManifestFailure.READ_FAILED
