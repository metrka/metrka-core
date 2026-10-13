"""Tests for acquiring existing landing assets."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.acquisition.landing import acquire_assets, collect_landed_assets
from metrka_core.pipeline.acquisition.models import (
    SourceCapture,
    SourceCaptureAssetReceipt,
    SourceCaptureReceipt,
)
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.storage.landing_store import LandingStore

CAPTURE_ID = "capture_20260905T191153Z_911f9856"
CAPTURED_AT = datetime(2026, 9, 5, 19, 11, 53, 68_400, tzinfo=UTC)
SOURCE_LAST_MODIFIED = datetime(2026, 9, 1, 17, 8, 25, tzinfo=UTC)


def _source_config() -> SourceConfig:
    return SourceConfig(
        workspace_name="fdc_obis",
        streams={
            "inmate_active": StreamConfig(
                name="inmate_active", official_filename="INMATE_ACTIVE_TEXTFILES.zip"
            )
        },
    )


def _capture(tmp_path: Path, *, source_capture_id: str = CAPTURE_ID) -> SourceCapture:
    capture_dir = tmp_path / source_capture_id
    capture_dir.mkdir()
    return SourceCapture(
        source_capture_id=source_capture_id,
        captured_at=CAPTURED_AT,
        directory=capture_dir,
        relative_path=f"2026-09-05/{source_capture_id}",
    )


def _deps(*, source_config: SourceConfig, landing_store: Mock) -> AcquisitionDeps:
    return AcquisitionDeps(
        source_config=source_config, landing_store=landing_store, execution_log_store=Mock()
    )


def test_backfill_reuses_immutable_asset_metadata_from_receipt(tmp_path: Path) -> None:
    capture = _capture(tmp_path)
    landed_file = capture.directory / "INMATE_ACTIVE_TEXTFILES.zip"
    landed_file.write_bytes(b"original source bytes")
    receipt = SourceCaptureReceipt(
        source_capture_id=capture.source_capture_id,
        pipeline_run_id="pipeline-original",
        captured_at=capture.captured_at,
        assets=(
            SourceCaptureAssetReceipt(
                stream_name="inmate_active",
                relative_path=landed_file.name,
                source_url="https://example.test/fdc",
                artifact_role="data",
                size_bytes=landed_file.stat().st_size,
                source_last_modified=SOURCE_LAST_MODIFIED,
            ),
        ),
    )
    landing_store = Mock(spec=LandingStore)
    landing_store.resolve_capture.return_value = capture
    landing_store.read_receipt.return_value = receipt

    result = acquire_assets(
        runtime=Mock(spec=ActionRuntime),
        deps=_deps(source_config=_source_config(), landing_store=landing_store),
        target_date="2026-09-05",
        target_source_capture_id=capture.source_capture_id,
        scheduled_extractor=Mock(),
        extractor_options={},
        backfill_source_url="manual_upload",
        backfill_match_mode="exact",
        backfill_source_last_modified_from="target_date",
    )

    assert len(result.landed_assets) == 1
    asset = result.landed_assets[0]
    assert asset.source_url == "https://example.test/fdc"
    assert asset.source_last_modified == SOURCE_LAST_MODIFIED
    assert asset.path == landed_file.resolve()
    landing_store.read_receipt.assert_called_once_with(capture)


def test_receipted_backfill_rejects_changed_file_size(tmp_path: Path) -> None:
    capture = _capture(tmp_path)
    landed_file = capture.directory / "INMATE_ACTIVE_TEXTFILES.zip"
    landed_file.write_bytes(b"changed")
    receipt = SourceCaptureReceipt(
        source_capture_id=capture.source_capture_id,
        pipeline_run_id="pipeline-original",
        captured_at=capture.captured_at,
        assets=(
            SourceCaptureAssetReceipt(
                stream_name="inmate_active",
                relative_path=landed_file.name,
                source_url="https://example.test/fdc",
                artifact_role="data",
                size_bytes=999,
                source_last_modified=SOURCE_LAST_MODIFIED,
            ),
        ),
    )
    landing_store = Mock(spec=LandingStore)
    landing_store.resolve_capture.return_value = capture
    landing_store.read_receipt.return_value = receipt

    with pytest.raises(RuntimeError, match="size does not match"):
        acquire_assets(
            runtime=Mock(spec=ActionRuntime),
            deps=_deps(source_config=_source_config(), landing_store=landing_store),
            target_date="2026-09-05",
            target_source_capture_id=capture.source_capture_id,
            scheduled_extractor=Mock(),
            extractor_options={},
            backfill_source_url="manual_upload",
            backfill_match_mode="exact",
            backfill_source_last_modified_from="target_date",
        )


def test_legacy_backfill_still_uses_configured_metadata(tmp_path: Path) -> None:
    capture = _capture(tmp_path, source_capture_id="legacy_2026-09-05")
    landed_file = capture.directory / "INMATE_ACTIVE_TEXTFILES.zip"
    landed_file.write_bytes(b"legacy")
    landing_store = Mock(spec=LandingStore)
    landing_store.resolve_capture.return_value = capture

    result = acquire_assets(
        runtime=Mock(spec=ActionRuntime),
        deps=_deps(source_config=_source_config(), landing_store=landing_store),
        target_date="2026-09-05",
        target_source_capture_id=None,
        scheduled_extractor=Mock(),
        extractor_options={},
        backfill_source_url="manual_upload",
        backfill_match_mode="exact",
        backfill_source_last_modified_from="target_date",
    )

    assert len(result.landed_assets) == 1
    assert result.landed_assets[0].source_url == "manual_upload"
    assert result.landed_assets[0].source_last_modified == datetime(2026, 9, 5, tzinfo=UTC)
    landing_store.read_receipt.assert_not_called()


def test_pattern_backfill_collects_multiple_files_for_one_stream(tmp_path: Path) -> None:
    source_config = SourceConfig(
        workspace_name="fl_ahca_adult_substance_abuse_beds",
        streams={"county": StreamConfig(name="county", official_filename="cid0321__*.xlsx")},
    )

    filenames = (
        "cid0321__single-year__default__all__2025.xlsx",
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__female__all__2025.xlsx",
    )

    for filename in filenames:
        (tmp_path / filename).write_bytes(b"xlsx")

    (tmp_path / "unrelated.xlsx").write_bytes(b"xlsx")

    assets = collect_landed_assets(
        source_config=source_config,
        target_dir=tmp_path,
        source_capture_id=CAPTURE_ID,
        match_mode="pattern",
    )

    assert [asset.path.name for asset in assets] == sorted(filenames)
    assert {asset.stream_name for asset in assets} == {"county"}


def test_backfill_records_each_streams_own_download_url(tmp_path: Path) -> None:
    source_config = SourceConfig(
        workspace_name="fl_healthcharts",
        streams={
            "beds": StreamConfig(
                name="beds",
                official_filename="cid0321__*.xlsx",
                extra={"download_url": "https://example.test/cid=0321"},
            ),
            "abuse": StreamConfig(name="abuse", official_filename="cid0560__*.xlsx"),
        },
    )
    (tmp_path / "cid0321__2025.xlsx").write_bytes(b"xlsx")
    (tmp_path / "cid0560__2025.xlsx").write_bytes(b"xlsx")

    assets = collect_landed_assets(
        source_config=source_config,
        target_dir=tmp_path,
        source_capture_id=CAPTURE_ID,
        source_url="https://example.test/portal",
        match_mode="pattern",
    )

    assert {asset.stream_name: asset.source_url for asset in assets} == {
        "beds": "https://example.test/cid=0321",
        "abuse": "https://example.test/portal",
    }


def test_pattern_backfill_rejects_file_matching_multiple_streams(tmp_path: Path) -> None:
    source_config = SourceConfig(
        workspace_name="fl_ahca_adult_substance_abuse_beds",
        streams={
            "county": StreamConfig(name="county", official_filename="cid0321__*.xlsx"),
            "default_group": StreamConfig(
                name="default_group", official_filename="cid0321__single-year__default__*.xlsx"
            ),
        },
    )

    filename = "cid0321__single-year__default__all__2025.xlsx"
    (tmp_path / filename).write_bytes(b"xlsx")

    with pytest.raises(RuntimeError, match="matches multiple streams"):
        collect_landed_assets(
            source_config=source_config,
            target_dir=tmp_path,
            source_capture_id=CAPTURE_ID,
            match_mode="pattern",
        )


def test_source_capture_receipt_allows_multiple_files_for_one_stream() -> None:
    receipt = SourceCaptureReceipt(
        source_capture_id=CAPTURE_ID,
        pipeline_run_id="pipeline-1",
        captured_at=CAPTURED_AT,
        assets=(
            SourceCaptureAssetReceipt(
                stream_name="county",
                relative_path=("cid0321__single-year__default__all__2002.xlsx"),
                source_url="manual_upload",
                artifact_role="data",
                size_bytes=100,
            ),
            SourceCaptureAssetReceipt(
                stream_name="county",
                relative_path=("cid0321__single-year__default__all__2025.xlsx"),
                source_url="manual_upload",
                artifact_role="data",
                size_bytes=120,
            ),
        ),
    )

    assert len(receipt.assets) == 2
    assert {asset.relative_path for asset in receipt.assets} == {
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__default__all__2025.xlsx",
    }


def test_source_capture_receipt_rejects_duplicate_stream_path() -> None:
    duplicate_asset = SourceCaptureAssetReceipt(
        stream_name="county",
        relative_path="cid0321__single-year__default__all__2002.xlsx",
        source_url="manual_upload",
        artifact_role="data",
        size_bytes=100,
    )

    with pytest.raises(ValueError, match="must not repeat stream and relative path"):
        SourceCaptureReceipt(
            source_capture_id=CAPTURE_ID,
            pipeline_run_id="pipeline-1",
            captured_at=CAPTURED_AT,
            assets=(duplicate_asset, duplicate_asset),
        )
