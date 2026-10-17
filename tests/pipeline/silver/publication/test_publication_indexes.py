"""Tests for recoverable publication-backed Silver indexes."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from metrka_core.pipeline.silver.publication_indexes import PublicationBackedSilverIndexService

from .fakes import (
    DATASET_ID,
    FakePublicationStore,
    FakeSilverArtifactStore,
    InMemoryPublicationAssetStore,
    make_asset_request,
    make_manifest,
    make_publication,
)

FIXED_INDEX_TIME = datetime(2026, 8, 14, 16, 0, tzinfo=UTC)


class FrozenClock:
    """Return a deterministic projection timestamp."""

    def now_utc(self) -> datetime:
        return FIXED_INDEX_TIME


def _service(
    *,
    publications: FakePublicationStore,
    assets: InMemoryPublicationAssetStore,
    silver_store: FakeSilverArtifactStore,
) -> PublicationBackedSilverIndexService:
    return PublicationBackedSilverIndexService(
        publications=publications,
        publication_assets=assets,
        silver_store=silver_store,  # type: ignore[arg-type]
        clock=FrozenClock(),
    )


def test_refresh_current_reads_only_current_manifest(tmp_path: Path) -> None:
    current = make_publication()
    historical = make_publication(
        publication_id="publication-old",
        silver_build_id="build-old",
        manifest_path="manifests/build-old.json",
        version_period=date(2024, 1, 1),
        partition_value="2024",
        is_current=False,
    )
    publications = FakePublicationStore([current, historical])
    assets = InMemoryPublicationAssetStore(publications=publications)
    silver_store = FakeSilverArtifactStore(tmp_path)
    silver_store.manifests[current.manifest_path] = make_manifest(publication=current)
    silver_store.manifests[historical.manifest_path] = make_manifest(publication=historical)

    result = _service(
        publications=publications, assets=assets, silver_store=silver_store
    ).refresh_current(dataset_id=DATASET_ID)

    assert result.current_publication == current
    assert silver_store.manifest_reads == [current.manifest_path]
    assert len(result.view_paths) == 1
    assert silver_store.written_views[0][:2] == ("adult-lead-county", "latest")
    pointer = silver_store.pointer_payloads[DATASET_ID]
    assert pointer["publication_id"] == current.publication_id
    assert pointer["silver_build_id"] == current.silver_build_id

    assert pointer["published_at_utc"] == (current.published_at.isoformat())
    assert pointer["updated_at_utc"] == (FIXED_INDEX_TIME.isoformat())


def test_refresh_current_rejects_mismatched_manifest(tmp_path: Path) -> None:
    current = make_publication()
    publications = FakePublicationStore([current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    silver_store = FakeSilverArtifactStore(tmp_path)
    invalid_manifest = make_manifest(publication=current)
    invalid_manifest["dataset_id"] = "another.dataset"
    silver_store.manifests[current.manifest_path] = invalid_manifest

    with pytest.raises(ValueError, match="dataset_id mismatch"):
        _service(
            publications=publications, assets=assets, silver_store=silver_store
        ).refresh_current(dataset_id=DATASET_ID)


def test_refresh_current_rejects_mismatched_fingerprint_algorithm(tmp_path: Path) -> None:
    current = make_publication()
    publications = FakePublicationStore([current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    silver_store = FakeSilverArtifactStore(tmp_path)
    invalid_manifest = make_manifest(publication=current)
    fingerprints = invalid_manifest["fingerprints"]
    assert isinstance(fingerprints, dict)
    fingerprints["schema_algorithm"] = "another.schema.algorithm"
    silver_store.manifests[current.manifest_path] = invalid_manifest

    with pytest.raises(ValueError, match="fingerprint schema_algorithm mismatch"):
        _service(
            publications=publications, assets=assets, silver_store=silver_store
        ).refresh_current(dataset_id=DATASET_ID)


def test_history_index_uses_registered_assets_not_manifests(tmp_path: Path) -> None:
    current = make_publication()
    publications = FakePublicationStore([current])
    assets = InMemoryPublicationAssetStore(publications=publications)
    assets.register(publication_id=current.publication_id, assets=(make_asset_request(),))
    silver_store = FakeSilverArtifactStore(tmp_path)

    view_paths = _service(
        publications=publications, assets=assets, silver_store=silver_store
    ).rebuild_history(dataset_id=DATASET_ID)

    assert len(view_paths) == 1
    assert silver_store.manifest_reads == []
    assert silver_store.written_views[0][:2] == ("adult-lead-county", "history")


def test_refresh_current_requires_current_publication(tmp_path: Path) -> None:
    publications = FakePublicationStore()
    assets = InMemoryPublicationAssetStore(publications=publications)

    with pytest.raises(RuntimeError, match="without a current publication"):
        _service(
            publications=publications, assets=assets, silver_store=FakeSilverArtifactStore(tmp_path)
        ).refresh_current(dataset_id=DATASET_ID)
