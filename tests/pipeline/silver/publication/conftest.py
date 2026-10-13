"""Shared fixtures for publication lifecycle tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from metrka_core.catalog.publication_asset_models import DatasetPublicationAsset
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.pipeline.silver.build_models import SilverBuild

from .fakes import make_asset, make_build, make_manifest, make_publication


@pytest.fixture
def publication() -> DatasetPublication:
    return make_publication()


@pytest.fixture
def publication_asset(publication: DatasetPublication) -> DatasetPublicationAsset:
    return make_asset(publication=publication)


@pytest.fixture
def silver_build() -> SilverBuild:
    return make_build()


@pytest.fixture
def manifest(publication: DatasetPublication) -> dict[str, Any]:
    return make_manifest(publication=publication)


@pytest.fixture
def artifact_root(tmp_path: Path) -> Path:
    return tmp_path / "workspace"
