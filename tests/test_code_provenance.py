"""Contract tests for portable core and dataset-repository provenance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from metrka_core.pipeline import provenance
from metrka_core.pipeline.provenance import (
    BuildProvenanceError,
    CodeProvenance,
    GitCodeRevision,
    _collect_core_code_revision,
    collect_dataset_code_revision,
    write_core_build_provenance,
)

COMMIT_SHA = "a" * 40


def _write_embedded_metadata(
    package_directory: Path, *, package_version: str = "1.0.0", dirty: bool = False
) -> None:
    package_directory.mkdir(parents=True, exist_ok=True)
    metadata_path = package_directory / "_build_provenance.json"
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": "metrka-core",
                "commit_sha": COMMIT_SHA,
                "branch": "main",
                "package_version": package_version,
                "dirty": dirty,
            }
        ),
        encoding="utf-8",
    )


def test_wheel_inside_consumer_repository_uses_embedded_core_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    consumer_root = tmp_path / "metrka-datasets"
    package_directory = consumer_root / ".venv" / "site-packages" / "metrka_core"
    _write_embedded_metadata(package_directory)

    monkeypatch.setattr(provenance, "_try_find_git_root", lambda _path: consumer_root)

    revision, dirty = _collect_core_code_revision(
        package_directory=package_directory, installed_package_version="1.0.0"
    )

    assert revision.repository == "metrka-core"
    assert revision.commit_sha == COMMIT_SHA
    assert revision.branch == "main"
    assert revision.package_version == "1.0.0"
    assert dirty is False


def test_installed_package_without_embedded_revision_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_directory = tmp_path / "site-packages" / "metrka_core"
    package_directory.mkdir(parents=True)
    monkeypatch.setattr(provenance, "_try_find_git_root", lambda _path: None)

    with pytest.raises(BuildProvenanceError, match="does not contain _build_provenance.json"):
        _collect_core_code_revision(
            package_directory=package_directory, installed_package_version="1.0.0"
        )


def test_embedded_revision_must_match_installed_package_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_directory = tmp_path / "site-packages" / "metrka_core"
    _write_embedded_metadata(package_directory, package_version="0.9.0")
    monkeypatch.setattr(provenance, "_try_find_git_root", lambda _path: None)

    with pytest.raises(BuildProvenanceError, match="does not match the installed distribution"):
        _collect_core_code_revision(
            package_directory=package_directory, installed_package_version="1.0.0"
        )


def test_build_writer_records_exact_git_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_directory = tmp_path / "src" / "metrka_core"
    package_directory.mkdir(parents=True)

    def fake_run_git(_repo_path: Path, *arguments: str) -> str:
        if arguments == ("rev-parse", "--show-toplevel"):
            return str(tmp_path)
        if arguments == ("status", "--porcelain", "--untracked-files=normal"):
            return ""
        if arguments == ("branch", "--show-current"):
            return "main"
        if arguments == ("rev-parse", "HEAD"):
            return COMMIT_SHA
        raise AssertionError(f"Unexpected Git arguments: {arguments}")

    monkeypatch.setattr(provenance, "_run_git", fake_run_git)

    destination = write_core_build_provenance(
        repository_root=tmp_path, package_version_value="1.0.0"
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": 1,
        "repository": "metrka-core",
        "commit_sha": COMMIT_SHA,
        "branch": "main",
        "package_version": "1.0.0",
        "dirty": False,
    }


def test_dataset_repository_without_python_distribution_uses_git_root_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository_root = tmp_path / "metrka-datasets"
    workspace_path = repository_root / "datasets" / "example"
    workspace_path.mkdir(parents=True)
    monkeypatch.setattr(provenance, "find_git_root", lambda _path: repository_root)
    monkeypatch.setattr(provenance, "_run_git", _clean_repository_git)

    revision, dirty = collect_dataset_code_revision(workspace_path)

    assert revision == GitCodeRevision(
        repository="metrka-datasets", commit_sha=COMMIT_SHA, branch="main", package_version=None
    )
    assert dirty is False


def test_dataset_repository_uses_optional_pyproject_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository_root = tmp_path / "private-checkout"
    workspace_path = repository_root / "datasets" / "example"
    workspace_path.mkdir(parents=True)
    (repository_root / "pyproject.toml").write_text(
        '[project]\nname = "metrka-datasets"\nversion = "0.1.0"\n', encoding="utf-8"
    )
    monkeypatch.setattr(provenance, "find_git_root", lambda _path: repository_root)
    monkeypatch.setattr(provenance, "_run_git", _clean_repository_git)

    revision, dirty = collect_dataset_code_revision(workspace_path)

    assert revision.repository == "metrka-datasets"
    assert revision.package_version == "0.1.0"
    assert dirty is False


def test_code_provenance_serializes_generic_dataset_key() -> None:
    core = GitCodeRevision(
        repository="metrka-core", commit_sha="a" * 40, branch="main", package_version="1.1.0"
    )
    dataset = GitCodeRevision(
        repository="open-datasets", commit_sha="b" * 40, branch="main", package_version=None
    )

    payload = CodeProvenance(metrka_core=core, dataset_repository=dataset, dirty=False).to_dict()

    assert payload == {
        "metrka_core": {
            "repository": "metrka-core",
            "commit_sha": "a" * 40,
            "branch": "main",
            "package_version": "1.1.0",
        },
        "dataset_repository": {
            "repository": "open-datasets",
            "commit_sha": "b" * 40,
            "branch": "main",
            "package_version": None,
        },
        "dirty": False,
    }


def _clean_repository_git(_repo_path: Path, *arguments: str) -> str:
    if arguments == ("branch", "--show-current"):
        return "main"
    if arguments == ("rev-parse", "HEAD"):
        return COMMIT_SHA
    if arguments == ("status", "--porcelain", "--untracked-files=normal"):
        return ""
    raise AssertionError(f"Unexpected Git arguments: {arguments}")
