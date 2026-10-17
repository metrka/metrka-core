"""Acquisition of files already present in the landing zone."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from metrka_core.datasets.source_config import SourceConfig
from metrka_core.pipeline.acquisition.contracts import AssetExtractor
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.acquisition.models import SourceCaptureAssetReceipt, SourceCaptureReceipt
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import (
    AcquisitionResult,
    BackfillSourceLastModifiedMode,
    LandedAsset,
)

logger = logging.getLogger(__name__)


LandingMatchMode = Literal["exact", "pattern"]


def collect_landed_assets(
    *,
    source_config: SourceConfig,
    target_dir: Path,
    source_capture_id: str,
    source_url: str = "manual_upload",
    match_mode: LandingMatchMode = "exact",
    source_last_modified_from: BackfillSourceLastModifiedMode = "none",
    target_date: str | None = None,
) -> list[LandedAsset]:
    """
    Find files in a landing directory using configured stream filenames.

    Exact mode matches a configured official filename.
    Pattern mode treats the configured filename as a glob pattern.
    """

    if not target_dir.exists():
        raise FileNotFoundError(f"Landing directory does not exist: {target_dir}")

    if not target_dir.is_dir():
        raise NotADirectoryError(f"Landing path is not a directory: {target_dir}")

    target_date_modified: datetime | None = None

    if source_last_modified_from == "target_date":
        if target_date is None:
            raise ValueError("target_date is required when source_last_modified_from='target_date'")

        try:
            target_date_modified = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError as exc:
            raise ValueError("target_date must use YYYY-MM-DD format") from exc

    landed_assets: list[LandedAsset] = []
    assigned_streams: dict[Path, str] = {}

    for stream_name, stream in source_config.streams.items():
        landed_files: tuple[Path, ...]
        stream_url = stream.extra.get("download_url")
        asset_source_url = (
            stream_url.strip() if isinstance(stream_url, str) and stream_url.strip() else source_url
        )

        if match_mode == "exact":
            landed_file = source_config.find_landed_file(stream_name, target_dir)
            landed_files = () if landed_file is None else (landed_file,)
        elif match_mode == "pattern":
            landed_files = source_config.find_landed_files_by_pattern(stream_name, target_dir)
        else:
            raise ValueError(f"Unsupported landing match mode: {match_mode}")

        if not landed_files:
            logger.warning("No landed files found for stream %s in %s", stream_name, target_dir)
            continue

        for landed_file in landed_files:
            resolved_landed_file = landed_file.resolve()
            existing_stream = assigned_streams.get(resolved_landed_file)

            if existing_stream is not None:
                raise RuntimeError(
                    f"Landed file {landed_file} matches multiple streams: "
                    f"{existing_stream}, {stream_name}"
                )

            assigned_streams[resolved_landed_file] = stream_name
            source_last_modified = None

            if source_last_modified_from == "file_mtime":
                source_last_modified = datetime.fromtimestamp(landed_file.stat().st_mtime, tz=UTC)

            elif source_last_modified_from == "target_date":
                if stream.artifact_role == "data":
                    source_last_modified = target_date_modified
                else:
                    source_last_modified = datetime.fromtimestamp(
                        landed_file.stat().st_mtime, tz=UTC
                    )

            landed_assets.append(
                LandedAsset(
                    stream_name=stream_name,
                    path=landed_file,
                    source_capture_id=source_capture_id,
                    source_url=asset_source_url,
                    artifact_role=stream.artifact_role,
                    source_last_modified=source_last_modified,
                )
            )

    logger.info("Collected %d landed assets from %s", len(landed_assets), target_dir)

    return landed_assets


def collect_receipted_landed_assets(
    *, source_config: SourceConfig, target_dir: Path, receipt: SourceCaptureReceipt
) -> list[LandedAsset]:
    """Rebuild landed assets from immutable metadata recorded at acquisition."""

    capture_root = target_dir.expanduser().resolve()
    landed_assets: list[LandedAsset] = []

    for recorded_asset in receipt.assets:
        stream = source_config.streams.get(recorded_asset.stream_name)

        if stream is None:
            raise ValueError(
                "Source-capture receipt contains a stream that is not configured: "
                f"{recorded_asset.stream_name}"
            )

        if stream.artifact_role != recorded_asset.artifact_role:
            raise ValueError(
                "Source-capture receipt artifact role does not match the current "
                f"stream configuration: {recorded_asset.stream_name}"
            )

        relative_path = PurePosixPath(recorded_asset.relative_path)
        landed_file = capture_root.joinpath(*relative_path.parts).resolve()

        try:
            landed_file.relative_to(capture_root)
        except ValueError as exc:
            raise ValueError(
                "Source-capture receipt asset resolves outside its capture directory: "
                f"{recorded_asset.relative_path}"
            ) from exc

        if not landed_file.is_file():
            raise FileNotFoundError(f"Source-capture receipt asset does not exist: {landed_file}")

        actual_size_bytes = landed_file.stat().st_size

        if actual_size_bytes != recorded_asset.size_bytes:
            raise RuntimeError(
                "Source-capture receipt asset size does not match the file: "
                f"path={recorded_asset.relative_path!r}, "
                f"expected={recorded_asset.size_bytes}, "
                f"actual={actual_size_bytes}"
            )

        landed_assets.append(
            LandedAsset(
                stream_name=recorded_asset.stream_name,
                path=landed_file,
                source_capture_id=receipt.source_capture_id,
                source_url=recorded_asset.source_url,
                artifact_role=recorded_asset.artifact_role,
                source_last_modified=recorded_asset.source_last_modified,
            )
        )

    logger.info(
        "Collected %d landed assets from immutable source-capture receipt %s",
        len(landed_assets),
        receipt.source_capture_id,
    )

    return landed_assets


def acquire_assets(
    *,
    runtime: ActionRuntime,
    deps: AcquisitionDeps,
    target_date: str | None,
    target_source_capture_id: str | None,
    scheduled_extractor: AssetExtractor,
    extractor_options: dict[str, Any],
    backfill_source_url: str = "manual_upload",
    backfill_match_mode: LandingMatchMode = "exact",
    backfill_source_last_modified_from: BackfillSourceLastModifiedMode = "none",
) -> AcquisitionResult:
    """
    Acquire assets for either a backfill or a scheduled pipeline run.

    Backfill runs collect files already present in landing.
    Scheduled runs invoke the supplied source-specific extractor.
    """

    if target_date is not None:
        logger.info("BACKFILL MODE: Collecting landed assets for date %s", target_date)

        capture = deps.landing_store.resolve_capture(
            date_str=target_date, source_capture_id=target_source_capture_id
        )

        logger.info("Using source capture %s from %s", capture.source_capture_id, capture.directory)

        if capture.source_capture_id.startswith("legacy_"):
            landed_assets = collect_landed_assets(
                source_config=deps.source_config,
                target_dir=capture.directory,
                source_capture_id=capture.source_capture_id,
                source_url=backfill_source_url,
                match_mode=backfill_match_mode,
                source_last_modified_from=backfill_source_last_modified_from,
                target_date=target_date,
            )
        else:
            receipt = deps.landing_store.read_receipt(capture)
            landed_assets = collect_receipted_landed_assets(
                source_config=deps.source_config, target_dir=capture.directory, receipt=receipt
            )

        return AcquisitionResult(source_capture=capture, landed_assets=tuple(landed_assets))

    logger.info("SCHEDULED MODE: Running configured source extractor.")

    capture = deps.landing_store.begin_capture()

    logger.info("Created source capture %s at %s", capture.source_capture_id, capture.directory)

    landed_assets = scheduled_extractor(runtime, deps, capture, dict(extractor_options))

    receipt = SourceCaptureReceipt(
        source_capture_id=capture.source_capture_id,
        pipeline_run_id=runtime.pipeline_run_id,
        captured_at=capture.captured_at,
        assets=tuple(
            SourceCaptureAssetReceipt(
                stream_name=asset.stream_name,
                relative_path=asset.path.relative_to(capture.directory).as_posix(),
                source_url=asset.source_url,
                artifact_role=asset.artifact_role,
                size_bytes=asset.path.stat().st_size,
                source_last_modified=asset.source_last_modified,
            )
            for asset in landed_assets
        ),
    )

    receipt_path = deps.landing_store.write_receipt(capture, receipt)

    logger.info("Wrote source-capture receipt: %s", receipt_path)

    return AcquisitionResult(source_capture=capture, landed_assets=tuple(landed_assets))
