"""Acquisition-only pipelines still create and register a standard receipt."""

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import LandedAsset, parse_pipeline_spec
from metrka_core.pipeline.registry import PipelineRegistry
from metrka_core.pipeline.runner import execute_configured_pipeline
from metrka_core.pipeline.runtime_services import UuidSourceCaptureIdGenerator
from metrka_core.storage.landing_store import LocalLandingStore


def test_capture_only_writes_receipt_and_registers_capture(tmp_path: Path) -> None:
    config = SourceConfig(
        workspace_name="example",
        streams={"bookings": StreamConfig(name="bookings", official_filename="sheet.pdf")},
        pipeline={"acquisition": {"extractor": "test.pdf"}, "steps": []},
    )
    store = LocalLandingStore(
        root=tmp_path,
        clock=SimpleNamespace(now_utc=lambda: datetime(2026, 10, 28, tzinfo=UTC)),
        source_capture_ids=UuidSourceCaptureIdGenerator(),
    )
    deps = AcquisitionDeps(
        source_config=config, landing_store=store, execution_log_store=MagicMock()
    )
    registered = MagicMock()
    runtime = ActionRuntime(
        pipeline_run_id="pipeline_test", dataset_name="example", code_provenance=MagicMock()
    )
    context = SimpleNamespace(
        workspace=SimpleNamespace(source_config=config),
        acquisition=deps,
        metadata=SimpleNamespace(source_captures=registered),
        runtime=runtime,
        as_action_runtime=lambda: runtime,
    )
    registry = PipelineRegistry()

    def download(runtime, deps, capture, options):
        path = deps.landing_store.allocate_path(capture, "sheet.pdf")
        path.write_bytes(b"%PDF-1.4\nexample")
        return [
            LandedAsset(
                stream_name="bookings",
                path=path,
                source_url="https://example.org/sheet.pdf",
                source_capture_id=capture.source_capture_id,
                artifact_role="data",
            )
        ]

    registry.register_extractor("test.pdf", download)
    state = execute_configured_pipeline(context=context, registry=registry)

    assert state.source_capture is not None
    receipt = store.read_receipt(state.source_capture)
    assert receipt.pipeline_run_id == "pipeline_test"
    assert len(receipt.assets) == 1
    assert receipt.assets[0].relative_path == "sheet.pdf"
    assert receipt.assets[0].size_bytes == len(b"%PDF-1.4\nexample")
    assert state.bronze_batch is None
    assert state.action_outcomes == []
    registered.register_capture.assert_called_once_with(
        capture=state.source_capture, pipeline_run_id="pipeline_test", workspace_name="example"
    )


@pytest.mark.parametrize("steps", [None, {}, ""])
def test_capture_only_still_requires_an_explicit_steps_list(steps: object) -> None:
    with pytest.raises(RuntimeError, match="pipeline.steps must be a list"):
        parse_pipeline_spec({"acquisition": {"extractor": "test.pdf"}, "steps": steps})
