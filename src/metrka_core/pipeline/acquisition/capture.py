"""Acquire one source capture and register it in the metadata database."""

from __future__ import annotations

from typing import Any

from metrka_core.pipeline.acquisition.contracts import AssetExtractor
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.acquisition.landing import LandingMatchMode, acquire_assets
from metrka_core.pipeline.acquisition.postgres_source_capture_store import (
    PostgresSourceCaptureStore,
)
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import AcquisitionResult, BackfillSourceLastModifiedMode


def acquire_and_register_capture(
    *,
    runtime: ActionRuntime,
    deps: AcquisitionDeps,
    source_captures: PostgresSourceCaptureStore,
    target_date: str | None,
    target_source_capture_id: str | None,
    scheduled_extractor: AssetExtractor,
    extractor_options: dict[str, Any],
    backfill_source_url: str,
    backfill_match_mode: LandingMatchMode,
    backfill_source_last_modified_from: BackfillSourceLastModifiedMode,
) -> AcquisitionResult:
    """Acquire the assets of one run and record their source capture."""

    result = acquire_assets(
        runtime=runtime,
        deps=deps,
        target_date=target_date,
        target_source_capture_id=target_source_capture_id,
        scheduled_extractor=scheduled_extractor,
        extractor_options=extractor_options,
        backfill_source_url=backfill_source_url,
        backfill_match_mode=backfill_match_mode,
        backfill_source_last_modified_from=backfill_source_last_modified_from,
    )

    # A dataset-folder run is named `source.dataset`; its capture belongs to `source`.
    source_captures.register_capture(
        capture=result.source_capture,
        pipeline_run_id=runtime.pipeline_run_id,
        workspace_name=deps.source_config.workspace_name,
    )

    return result
