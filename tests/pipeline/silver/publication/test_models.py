from __future__ import annotations

from datetime import date, datetime

import pytest

from metrka_core.catalog.publication_asset_models import DatasetPublicationAssetRequest
from metrka_core.catalog.publication_models import DatasetPublicationRequest
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)

from .fakes import (
    LOGICAL_HASH,
    PROCESSING_HASH,
    QUALITY_HASH,
    SCHEMA_HASH,
    make_asset,
    make_publication,
)


def test_current_publication_must_be_active() -> None:
    with pytest.raises(ValueError, match="active revision"):
        make_publication(is_active_revision=False, is_current=True)


def test_publication_request_requires_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        DatasetPublicationRequest(
            publication_id="publication-1",
            pipeline_run_id="pipeline-1",
            dataset_id="dataset-1",
            version_period=date(2025, 1, 1),
            partition_key="version_period",
            partition_value="2025",
            silver_build_id="build-1",
            engine_release_id="engine-1",
            processing_config_hash=PROCESSING_HASH,
            quality_config_hash=QUALITY_HASH,
            fingerprint_version=1,
            logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
            schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
            logical_data_hash=LOGICAL_HASH,
            schema_hash=SCHEMA_HASH,
            manifest_path="manifests/build-1.json",
            published_at=datetime(2026, 8, 13, 12, 0),
        )


def test_publication_request_requires_valid_fingerprints() -> None:
    with pytest.raises(ValueError, match="logical_data_hash"):
        DatasetPublicationRequest(
            publication_id="publication-1",
            pipeline_run_id="pipeline-1",
            dataset_id="dataset-1",
            version_period=date(2025, 1, 1),
            partition_key="version_period",
            partition_value="2025",
            silver_build_id="build-1",
            engine_release_id="engine-1",
            processing_config_hash=PROCESSING_HASH,
            quality_config_hash=QUALITY_HASH,
            fingerprint_version=1,
            logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
            schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
            logical_data_hash="short",
            schema_hash=SCHEMA_HASH,
            manifest_path="manifests/build-1.json",
            published_at=datetime.now().astimezone(),
        )


def test_publication_request_requires_algorithm_identity() -> None:
    with pytest.raises(ValueError, match="schema_hash_algorithm"):
        DatasetPublicationRequest(
            publication_id="publication-1",
            pipeline_run_id="pipeline-1",
            dataset_id="dataset-1",
            version_period=date(2025, 1, 1),
            partition_key="version_period",
            partition_value="2025",
            silver_build_id="build-1",
            engine_release_id="engine-1",
            processing_config_hash=PROCESSING_HASH,
            quality_config_hash=QUALITY_HASH,
            fingerprint_version=1,
            logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
            schema_hash_algorithm="",
            logical_data_hash=LOGICAL_HASH,
            schema_hash=SCHEMA_HASH,
            manifest_path="manifests/build-1.json",
            published_at=datetime.now().astimezone(),
        )


def test_publication_asset_rejects_parent_path() -> None:
    with pytest.raises(ValueError, match="safe relative POSIX path"):
        DatasetPublicationAssetRequest(
            table_key="table-1",
            file_path="../outside.parquet",
            file_format="parquet",
            row_count=1,
            column_count=1,
            columns=("value",),
            size_bytes=10,
            checksum="sha256:test",
        )


def test_publication_asset_checks_column_count() -> None:
    with pytest.raises(ValueError, match="column_count"):
        DatasetPublicationAssetRequest(
            table_key="table-1",
            file_path="tables/data.parquet",
            file_format="parquet",
            row_count=1,
            column_count=2,
            columns=("value",),
            size_bytes=10,
            checksum="sha256:test",
        )


def test_publication_asset_maps_to_history_view_entry() -> None:
    publication = make_publication()
    entry = make_asset(publication=publication).to_view_entry()

    assert entry["dataset_id"] == publication.dataset_id
    assert entry["silver_build_id"] == publication.silver_build_id
    assert entry["format"] == "parquet"
    assert entry["columns"] == ["county_name", "count"]
