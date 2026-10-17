"""Tests for acquiring and registering one source capture."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from metrka_core.pipeline.acquisition.capture import acquire_and_register_capture
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import AcquisitionResult


def test_acquires_and_registers_capture() -> None:
    """A dataset-folder run is named `source.dataset`; its capture belongs to `source`."""

    runtime = Mock(spec=ActionRuntime)
    runtime.pipeline_run_id = "pipeline-test"
    runtime.dataset_name = "workspace-test.dataset"

    deps = Mock(spec=AcquisitionDeps)
    deps.source_config = Mock(workspace_name="workspace-test")
    source_captures = Mock()
    extractor = Mock()

    acquisition_result = Mock(spec=AcquisitionResult)
    acquisition_result.source_capture = Mock()

    with patch(
        "metrka_core.pipeline.acquisition.capture.acquire_assets", return_value=acquisition_result
    ) as acquire_assets_mock:
        result = acquire_and_register_capture(
            runtime=runtime,
            deps=deps,
            source_captures=source_captures,
            target_date="2026-08-15",
            target_source_capture_id="capture-test",
            scheduled_extractor=extractor,
            extractor_options={"provider": "test"},
            backfill_source_url="manual_upload",
            backfill_match_mode="exact",
            backfill_source_last_modified_from="none",
        )

    assert result is acquisition_result

    acquire_assets_mock.assert_called_once_with(
        runtime=runtime,
        deps=deps,
        target_date="2026-08-15",
        target_source_capture_id="capture-test",
        scheduled_extractor=extractor,
        extractor_options={"provider": "test"},
        backfill_source_url="manual_upload",
        backfill_match_mode="exact",
        backfill_source_last_modified_from="none",
    )

    source_captures.register_capture.assert_called_once_with(
        capture=acquisition_result.source_capture,
        pipeline_run_id="pipeline-test",
        workspace_name="workspace-test",
    )


def test_does_not_register_failed_acquisition() -> None:
    runtime = Mock(spec=ActionRuntime)
    runtime.pipeline_run_id = "pipeline-test"
    runtime.dataset_name = "workspace-test"

    deps = Mock(spec=AcquisitionDeps)
    source_captures = Mock()

    with (
        patch(
            "metrka_core.pipeline.acquisition.capture.acquire_assets",
            side_effect=RuntimeError("acquisition failed"),
        ),
        pytest.raises(RuntimeError, match="acquisition failed"),
    ):
        acquire_and_register_capture(
            runtime=runtime,
            deps=deps,
            source_captures=source_captures,
            target_date=None,
            target_source_capture_id=None,
            scheduled_extractor=Mock(),
            extractor_options={},
            backfill_source_url="manual_upload",
            backfill_match_mode="exact",
            backfill_source_last_modified_from="none",
        )

    source_captures.register_capture.assert_not_called()
