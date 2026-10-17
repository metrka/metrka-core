"""Contract tests for release metadata and build tooling."""

from __future__ import annotations

import tomllib
from pathlib import Path

from metrka_core.build_provenance import _project_version

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _project() -> dict[str, object]:
    project = _document()["project"]

    assert isinstance(project, dict)
    return project


def _document() -> dict[str, object]:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as handle:
        document = tomllib.load(handle)

    return document


def test_project_version_is_documented_in_changelog() -> None:
    version = _project_version(REPOSITORY_ROOT)
    changelog = (REPOSITORY_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

    if ".dev" in version:
        assert "## [Unreleased]" in changelog
        return

    assert f"## [{version}] - " in changelog


def test_installed_command_uses_stable_cli_entrypoint() -> None:
    scripts = _project()["scripts"]

    assert scripts == {"metrka": "metrka_core.cli:main"}


def test_release_declares_apache_license_with_pep_639_metadata() -> None:
    document = _document()
    build_system = document["build-system"]
    project = document["project"]

    assert isinstance(build_system, dict)
    assert isinstance(project, dict)
    assert build_system["requires"] == ["setuptools>=77.0.3"]
    assert project["license"] == "Apache-2.0"
    assert project["license-files"] == ["LICENSE"]


def test_apache_license_file_is_present_and_non_empty() -> None:
    license_text = (REPOSITORY_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert license_text.startswith("                                 Apache License\n")
    assert "Version 2.0, January 2004" in license_text
    assert "END OF TERMS AND CONDITIONS" in license_text
