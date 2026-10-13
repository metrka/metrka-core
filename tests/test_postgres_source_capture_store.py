from unittest.mock import MagicMock

from metrka_core.pipeline.acquisition.models import SourceCaptureAssetBinding
from metrka_core.pipeline.acquisition.postgres_source_capture_store import (
    PostgresSourceCaptureStore,
)


def _binding(relative_path: str) -> SourceCaptureAssetBinding:
    return SourceCaptureAssetBinding(
        stream_name="county",
        dataset_id="fl_healthcharts.county",
        dataset_file_id="dataset-file-1",
        relative_path=relative_path,
        source_url="manual_upload",
        artifact_role="data",
    )


def _stored_row(relative_path: str) -> dict[str, object]:
    return {
        "stream_name": "county",
        "dataset_id": "fl_healthcharts.county",
        "dataset_file_id": "dataset-file-1",
        "relative_path": relative_path,
        "source_url": "manual_upload",
        "artifact_role": "data",
        "source_last_modified": None,
    }


def test_binds_multiple_files_for_one_capture_stream() -> None:
    first_path = "cid0321__single-year__default__all__2002.xlsx"
    second_path = "cid0321__single-year__default__all__2025.xlsx"

    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [{"exists": 1}, _stored_row(first_path), _stored_row(second_path)]

    store = PostgresSourceCaptureStore(session)

    store.bind_assets(
        source_capture_id="capture-1", assets=(_binding(first_path), _binding(second_path))
    )

    insert_calls = [
        call
        for call in cursor.execute.call_args_list
        if "INSERT INTO meta.source_capture_assets" in call.args[0]
    ]

    assert len(insert_calls) == 2

    for call in insert_calls:
        normalized_sql = " ".join(call.args[0].split())

        assert (
            "ON CONFLICT ( source_capture_id, stream_name, relative_path ) DO NOTHING"
        ) in normalized_sql

    select_calls = [
        call
        for call in cursor.execute.call_args_list
        if "FROM meta.source_capture_assets" in call.args[0]
    ]

    assert [call.args[1] for call in select_calls] == [
        ("capture-1", "county", first_path),
        ("capture-1", "county", second_path),
    ]
