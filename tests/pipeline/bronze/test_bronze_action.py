from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from metrka_core.pipeline.acquisition.models import SourceCapture
from metrka_core.pipeline.actions.bronze import (
    BronzeIngestActionDeps,
    BronzeIngestOptions,
    ingest_bronze_action,
)
from metrka_core.pipeline.bronze.models import BronzeBatchResult, BronzeIngestResult
from metrka_core.pipeline.models import LandedAsset, PipelineRunState


def test_binds_every_source_file_to_one_bronze_batch(tmp_path: Path) -> None:
    capture_dir = tmp_path / "capture-1"
    capture_dir.mkdir()

    first = capture_dir / "cid0321__single-year__default__all__2002.xlsx"
    second = capture_dir / "cid0321__single-year__default__all__2025.xlsx"

    first.write_bytes(b"first")
    second.write_bytes(b"second")

    landed_assets = [
        LandedAsset(
            stream_name="county",
            path=first,
            source_url="manual_upload",
            source_capture_id="capture-1",
        ),
        LandedAsset(
            stream_name="county",
            path=second,
            source_url="manual_upload",
            source_capture_id="capture-1",
        ),
    ]

    bronze_result = BronzeIngestResult(
        dataset_file_id="dataset-file-1",
        dataset_id="fl_healthcharts.county",
        source_hash="a" * 64,
        bronze_run_id="bronze-run-1",
        is_new=True,
    )

    bronze_batch = BronzeBatchResult(
        by_stream={"county": bronze_result}, new_count=1, duplicate_count=0
    )
    bronze_deps = MagicMock()

    source_captures = MagicMock()

    state = PipelineRunState(
        source_capture=SourceCapture(
            source_capture_id="capture-1",
            captured_at=datetime(2026, 10, 3, 12, 0, tzinfo=UTC),
            directory=capture_dir,
            relative_path="2026-10-03/capture-1",
        ),
        landed_assets=landed_assets,
    )

    runtime = MagicMock()

    with patch(
        "metrka_core.pipeline.actions.bronze.ingest_landed_assets", return_value=bronze_batch
    ) as ingest:
        outcome = ingest_bronze_action(
            runtime=runtime,
            deps=BronzeIngestActionDeps(bronze=bronze_deps, source_captures=source_captures),
            state=state,
            options=BronzeIngestOptions(),
        )

    ingest.assert_called_once_with(runtime=runtime, deps=bronze_deps, assets=landed_assets)

    source_captures.bind_assets.assert_called_once()

    call = source_captures.bind_assets.call_args
    bindings = call.kwargs["assets"]

    assert call.kwargs["source_capture_id"] == "capture-1"
    assert [
        (binding.stream_name, binding.dataset_file_id, binding.relative_path)
        for binding in bindings
    ] == [
        ("county", "dataset-file-1", ("cid0321__single-year__default__all__2002.xlsx")),
        ("county", "dataset-file-1", ("cid0321__single-year__default__all__2025.xlsx")),
    ]

    assert outcome.status == "completed"
