from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from metrka_core.catalog.publication_asset_models import DatasetPublicationAssetRequest
from metrka_core.pipeline.silver.publication_asset_integrity import (
    Sha256PublicationAssetIntegrityVerifier,
)
from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityFailureCode,
    AssetIntegrityStatus,
)
from metrka_core.storage.silver_store import LocalSilverArtifactStore

from .fakes import FIXED_NOW


class SingleAssetPathResolver:
    """Test double exposing only the capability used by the verifier."""

    def __init__(self, *, expected_file_path: str, resolved_path: Path) -> None:
        self._expected_file_path = expected_file_path
        self._resolved_path = resolved_path

    def resolve_publication_asset_path(self, file_path: str) -> Path:
        assert file_path == self._expected_file_path
        return self._resolved_path


def _store(tmp_path: Path) -> LocalSilverArtifactStore:
    return LocalSilverArtifactStore(
        workspace_root=tmp_path,
        silver_root=tmp_path / "data" / "files" / "silver",
        current_root=tmp_path / "data" / "current",
    )


def _asset(
    *, store: LocalSilverArtifactStore, path: Path, content: bytes
) -> DatasetPublicationAssetRequest:
    return DatasetPublicationAssetRequest(
        table_key="people",
        file_path=store.relative_path(path),
        file_format="parquet",
        row_count=1,
        column_count=1,
        columns=("name",),
        size_bytes=len(content),
        checksum=f"sha256:{hashlib.sha256(content).hexdigest()}",
    )


def test_verifier_requires_only_publication_asset_path_resolution(tmp_path: Path) -> None:
    content = b"alpha"
    path = tmp_path / "data.parquet"
    path.write_bytes(content)
    file_path = "data/files/silver/tables/people/data.parquet"
    asset = DatasetPublicationAssetRequest(
        table_key="people",
        file_path=file_path,
        file_format="parquet",
        row_count=1,
        column_count=1,
        columns=("name",),
        size_bytes=len(content),
        checksum=f"sha256:{hashlib.sha256(content).hexdigest()}",
    )

    batch = Sha256PublicationAssetIntegrityVerifier(
        SingleAssetPathResolver(expected_file_path=file_path, resolved_path=path)
    ).inspect(assets=(asset,), checked_at=FIXED_NOW)

    assert batch.passed


def test_publication_asset_integrity_passes_for_unchanged_file(tmp_path: Path) -> None:
    store = _store(tmp_path)
    path = store.tables_root / "people" / "version_period=2026" / "build" / "data.parquet"
    path.parent.mkdir(parents=True)
    content = b"alpha"
    path.write_bytes(content)
    asset = _asset(store=store, path=path, content=content)

    batch = Sha256PublicationAssetIntegrityVerifier(store).inspect(
        assets=(asset,), checked_at=FIXED_NOW
    )

    assert batch.passed
    assert batch.failed_results == ()
    assert batch.results[0].status is AssetIntegrityStatus.PASSED
    assert batch.results[0].actual_size_bytes == len(content)
    assert batch.results[0].actual_checksum == asset.checksum


def test_publication_asset_integrity_detects_same_size_content_change(tmp_path: Path) -> None:
    store = _store(tmp_path)
    path = store.tables_root / "people" / "version_period=2026" / "build" / "data.parquet"
    path.parent.mkdir(parents=True)
    original = b"alpha"
    path.write_bytes(original)
    asset = _asset(store=store, path=path, content=original)
    path.write_bytes(b"bravo")

    batch = Sha256PublicationAssetIntegrityVerifier(store).inspect(
        assets=(asset,), checked_at=FIXED_NOW
    )

    assert not batch.passed
    assert batch.failed_results[0].failure_codes == (AssetIntegrityFailureCode.CHECKSUM_MISMATCH,)


def test_publication_asset_integrity_detects_missing_file(tmp_path: Path) -> None:
    store = _store(tmp_path)
    path = store.tables_root / "people" / "version_period=2026" / "build" / "data.parquet"
    asset = _asset(store=store, path=path, content=b"alpha")

    batch = Sha256PublicationAssetIntegrityVerifier(store).inspect(
        assets=(asset,), checked_at=FIXED_NOW
    )

    assert not batch.passed
    assert batch.failed_results[0].failure_codes == (AssetIntegrityFailureCode.MISSING_FILE,)
    assert batch.failed_results[0].actual_checksum is None


def test_publication_asset_integrity_rejects_empty_registry(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="contains no registered assets"):
        Sha256PublicationAssetIntegrityVerifier(_store(tmp_path)).inspect(
            assets=(), checked_at=FIXED_NOW
        )


def test_publication_asset_integrity_rejects_bare_digest_checksum(tmp_path: Path) -> None:
    store = _store(tmp_path)
    path = store.tables_root / "people" / "version_period=2026" / "build" / "data.parquet"
    path.parent.mkdir(parents=True)
    content = b"alpha"
    path.write_bytes(content)
    asset = replace(
        _asset(store=store, path=path, content=content),
        checksum=hashlib.sha256(content).hexdigest(),
    )

    batch = Sha256PublicationAssetIntegrityVerifier(store).inspect(
        assets=(asset,), checked_at=FIXED_NOW
    )

    assert not batch.passed
    assert batch.failed_results[0].failure_codes == (
        AssetIntegrityFailureCode.INVALID_EXPECTED_CHECKSUM,
    )
