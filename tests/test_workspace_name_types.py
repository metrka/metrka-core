"""The source workspace name and the run name must stay distinct types."""

from __future__ import annotations

from pathlib import Path

from mypy import api as mypy_api

# The mix-up that once recorded `fl_healthcharts.beds` as a capture's workspace.
_RUN_NAME_AS_WORKSPACE = """
from metrka_core.pipeline.acquisition.models import SourceCapture
from metrka_core.pipeline.acquisition.postgres_source_capture_store import PostgresSourceCaptureStore
from metrka_core.pipeline.action_runtime import ActionRuntime


def register(store: PostgresSourceCaptureStore, runtime: ActionRuntime, capture: SourceCapture) -> None:
    store.register_capture(
        capture=capture, pipeline_run_id="run", workspace_name=runtime.dataset_name
    )
"""


def test_mypy_rejects_a_run_name_where_the_workspace_name_belongs(tmp_path: Path) -> None:
    module = tmp_path / "run_name_as_workspace.py"
    module.write_text(_RUN_NAME_AS_WORKSPACE, encoding="utf-8")

    stdout, stderr, exit_status = mypy_api.run([str(module)])

    assert exit_status == 1, stdout + stderr
    assert 'incompatible type "RunName"; expected "WorkspaceName"' in stdout
