"""Prove that nonchronological historical arrivals preserve the newest publication."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from metrka_core.catalog.postgres_publication_asset_store import (
    PostgresDatasetPublicationAssetStore,
)
from metrka_core.catalog.postgres_publication_candidate_store import (
    PostgresDatasetPublicationCandidateStore,
)
from metrka_core.catalog.postgres_publication_projection_store import (
    PostgresDatasetPublicationProjectionStateStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.bootstrap import open_pipeline_context
from metrka_core.pipeline.composition import runtime as runtime_composition
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.default_registry import create_core_registry
from metrka_core.pipeline.models import PipelineRunState
from metrka_core.pipeline.provenance import CodeProvenance
from metrka_core.pipeline.runner import execute_configured_pipeline
from metrka_core.pipeline.silver.approved_publication_unit_of_work import (
    ApprovedPublicationCommand,
    ApprovedPublicationResult,
)
from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.postgres_approved_publication_unit_of_work import (
    PostgresApprovedPublicationUnitOfWork,
)
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.process_models import SilverProcessResult
from metrka_core.pipeline.silver.publication_asset_integrity import (
    Sha256PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.publication_indexes import PublicationBackedSilverIndexService
from metrka_core.pipeline.silver.publication_projection import (
    refresh_current_publication_projection,
    refresh_history_publication_projection,
)
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.postgres_publication_gate_evidence_store import (
    PostgresPublicationGateEvidenceStore,
)
from metrka_core.storage.naming import pointer_file_name
from metrka_core.storage.silver_store import LocalSilverArtifactStore
from metrka_core.storage.workspace_layout import WorkspaceLayout

from .deterministic_pipeline_support import (
    DeterministicWorkspace,
    FixedPublicationIds,
    FrozenClock,
    fixed_code_provenance,
    runtime_services,
    write_test_capture_receipt,
)

TEST_DSN = os.environ.get("METRKA_MIGRATION_TEST_DSN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DSN, reason="METRKA_MIGRATION_TEST_DSN is not configured"),
]


@dataclass(frozen=True, slots=True)
class HistoricalCapture:
    """One landing event whose data period is independent of its arrival date."""

    target_date: str
    source_capture_id: str
    version_period: date
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class FixedWorkspaceLocationResolver:
    workspace_name: str
    workspace_root: Path

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        if workspace_name != self.workspace_name:
            raise KeyError(workspace_name)

        return WorkspaceLocation.portable(
            workspace_name=self.workspace_name, workspace_root=self.workspace_root
        )


class SequentialSilverBuildIds:
    """Return deterministic Silver build IDs in candidate-processing order."""

    def __init__(self, values: tuple[str, ...]) -> None:
        self._values = iter(values)

    def new_silver_build_id(self) -> str:
        return next(self._values)


class SequentialPublicationCandidateIds:
    """Return deterministic publication-candidate IDs in build order."""

    def __init__(self, values: tuple[str, ...]) -> None:
        self._values = iter(values)

    def new_publication_candidate_id(self) -> str:
        return next(self._values)


def _test_dsn() -> str:
    if TEST_DSN is None:
        raise RuntimeError("Migration test DSN is missing")

    return TEST_DSN


def _uuid(token: str, label: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"metrka-historical-integration:{token}:{label}"))


def _create_historical_workspace(
    *, base_dir: Path, token: str
) -> tuple[DeterministicWorkspace, tuple[HistoricalCapture, ...]]:
    workspace_name = f"historical_{token}"
    workspace_root = base_dir / workspace_name
    config_dir = workspace_root / "conf"
    config_dir.mkdir(parents=True)

    # Arrival order is intentionally different from logical period order.
    captures = (
        HistoricalCapture(
            target_date="2026-08-12",
            source_capture_id=f"capture_20260812T100000Z_{token[:8]}",
            version_period=date(2026, 3, 1),
            ingested_at=datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        ),
        HistoricalCapture(
            target_date="2026-08-13",
            source_capture_id=f"capture_20260813T100000Z_{token[:8]}",
            version_period=date(2026, 1, 1),
            ingested_at=datetime(2026, 8, 13, 10, 0, tzinfo=UTC),
        ),
        HistoricalCapture(
            target_date="2026-08-14",
            source_capture_id=f"capture_20260814T100000Z_{token[:8]}",
            version_period=date(2026, 2, 1),
            ingested_at=datetime(2026, 8, 14, 10, 0, tzinfo=UTC),
        ),
    )

    for index, capture in enumerate(captures, start=1):
        capture_dir = (
            workspace_root
            / "data"
            / "files"
            / "bronze"
            / "landing"
            / capture.target_date
            / capture.source_capture_id
        )
        capture_dir.mkdir(parents=True)
        observed_on = capture.version_period.replace(day=15)
        asset_path = capture_dir / "people.csv"
        asset_path.write_text(
            f"id,observed_on,name\n{index:03d},{observed_on.isoformat()},period-{index}\n",
            encoding="utf-8",
        )
        write_test_capture_receipt(
            workspace_root=workspace_root,
            target_date=capture.target_date,
            source_capture_id=capture.source_capture_id,
            captured_at=capture.ingested_at,
            pipeline_run_id=f"pipeline_{token}_capture_{index}",
            asset_path=asset_path,
        )

    (config_dir / "main.yaml").write_text(
        f"""
workspace_name: {workspace_name}

streams:
  people:
    official_filename: people.csv
    yaml_contract_name: people.yaml
    artifact_role: data
    silver:
      partition_by: version_period
      version_period:
        strategy: max_column
        grain: month
        column: observed_on
      input:
        format: csv
        options: {{}}
      outputs:
        - csv

pipeline:
  quality:
    config: quality.yaml
  acquisition:
    extractor: http.files
    options: {{}}
    backfill:
      source_url: https://example.test/people.csv
      match_mode: exact
      source_last_modified_from: target_date
  steps:
    - action: bronze.ingest
""".lstrip(),
        encoding="utf-8",
    )

    (config_dir / "people.yaml").write_text(
        """
meta:
  version: "1"
  category: health-medicine
  tags:
    - historical-integration

tables:
  people:
    columns:
      id:
        rename_to: id
        cast_to: string
      observed_on:
        rename_to: observed_on
        cast_to: date
        format_in: "%Y-%m-%d"
      name:
        rename_to: name
        cast_to: string
    canonical_order:
      - id
      - observed_on
      - name
""".lstrip(),
        encoding="utf-8",
    )

    (config_dir / "quality.yaml").write_text("version: 1\n", encoding="utf-8")

    return (
        DeterministicWorkspace(
            name=workspace_name,
            root=workspace_root,
            dataset_id=f"{workspace_name}.people",
            source_capture_id=captures[-1].source_capture_id,
        ),
        captures,
    )


def _enable_silver(workspace: DeterministicWorkspace) -> None:
    config_path = workspace.root / "conf" / "main.yaml"
    bronze_only = "  steps:\n    - action: bronze.ingest\n"
    bronze_and_silver = "  steps:\n    - action: bronze.ingest\n    - action: silver.process\n"
    config_text = config_path.read_text(encoding="utf-8")

    if config_text.count(bronze_only) != 1:
        raise AssertionError("Historical fixture contains an unexpected pipeline step block")

    config_path.write_text(config_text.replace(bronze_only, bronze_and_silver), encoding="utf-8")


def _run_capture(
    *,
    workspace: DeterministicWorkspace,
    capture: HistoricalCapture,
    services: RuntimeServices,
    run_silver: bool,
) -> PipelineRunState:
    overrides = None

    if run_silver:
        overrides = {
            "silver.process": {"force_rebuild": False, "target_dataset_id": workspace.dataset_id}
        }

    with open_pipeline_context(
        workspace_name=workspace.name,
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
        services=services,
        workspace_location_resolver=FixedWorkspaceLocationResolver(
            workspace_name=workspace.name, workspace_root=workspace.root
        ),
        metadata_conninfo=_test_dsn(),
    ) as context:
        state = execute_configured_pipeline(
            context=context,
            registry=create_core_registry(),
            target_date=capture.target_date,
            source_capture_id=capture.source_capture_id,
            action_option_overrides=overrides,
        )

    expected_statuses = ["completed", "completed"] if run_silver else ["completed"]
    assert [result.outcome.status for result in state.action_outcomes] == expected_statuses

    return state


def _queue_services(
    *,
    token: str,
    capture: HistoricalCapture,
    silver_build_ids: tuple[str, ...],
    candidate_ids: tuple[str, ...],
) -> RuntimeServices:
    base = runtime_services(
        token=token, run_label="queue", source_capture_id=capture.source_capture_id
    )

    return replace(
        base,
        clock=FrozenClock(capture.ingested_at),
        silver_build_ids=SequentialSilverBuildIds(silver_build_ids),
        publication_candidate_ids=SequentialPublicationCandidateIds(candidate_ids),
    )


def _publish_and_refresh(
    *,
    workspace: DeterministicWorkspace,
    candidate_id: str,
    publication_id: str,
    published_at: datetime,
) -> tuple[ApprovedPublicationResult, tuple[Path, ...]]:
    layout = WorkspaceLayout(
        location=WorkspaceLocation.portable(
            workspace_name=workspace.name, workspace_root=workspace.root
        )
    )
    silver_store = LocalSilverArtifactStore(
        workspace_root=layout.data_root,
        silver_root=layout.silver_dir,
        current_root=layout.current_dir,
    )

    with PostgresSession(_test_dsn()) as session:
        candidates = PostgresDatasetPublicationCandidateStore(session)
        publications = PostgresDatasetPublicationStore(session)
        publication_assets = PostgresDatasetPublicationAssetStore(session)
        projection_states = PostgresDatasetPublicationProjectionStateStore(session)
        integrity_evidence = PostgresAssetIntegrityEvidenceStore(session)
        approved = candidates.approve(
            candidate_id=candidate_id,
            approved_by="historical-period-integration-test",
            approved_at=published_at,
        )
        publisher = PostgresApprovedPublicationUnitOfWork(
            session=session,
            candidates=candidates,
            silver_builds=PostgresSilverBuildStore(session),
            publications=publications,
            publication_assets=publication_assets,
            publication_asset_integrity=Sha256PublicationAssetIntegrityVerifier(silver_store),
            asset_integrity_batches=integrity_evidence,
            publication_integrity=integrity_evidence,
            publication_gate_evidence=PostgresPublicationGateEvidenceStore(session),
            projection_states=projection_states,
            silver_store=silver_store,
            publication_ids=FixedPublicationIds(publication_id),
        )
        result = publisher.commit(
            ApprovedPublicationCommand(
                candidate_id=approved.candidate_id, published_at=published_at
            )
        )
        indexes = PublicationBackedSilverIndexService(
            publications=publications,
            publication_assets=publication_assets,
            silver_store=silver_store,
            clock=FrozenClock(published_at),
        )
        current_result = refresh_current_publication_projection(
            dataset_id=workspace.dataset_id,
            publication=result.current_publication,
            checked_at=published_at,
            publication_indexes=indexes,
            projection_states=projection_states,
        )
        history_paths = refresh_history_publication_projection(
            dataset_id=workspace.dataset_id,
            expected_publication_id=result.publication.publication_id,
            checked_at=published_at,
            publication_indexes=indexes,
            projection_states=projection_states,
        )

    assert current_result.current_publication == result.current_publication
    return result, history_paths


def _dataset_counts(dataset_id: str) -> tuple[int, int, int]:
    with PostgresSession(_test_dsn()) as session, session.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM logs.silver_build_attempts
                    WHERE dataset_id = %s
                ) AS build_count,
                (
                    SELECT COUNT(*)
                    FROM catalog.dataset_publication_candidates
                    WHERE dataset_id = %s
                ) AS candidate_count,
                (
                    SELECT COUNT(*)
                    FROM catalog.dataset_publications
                    WHERE dataset_id = %s
                ) AS publication_count
            """,
            (dataset_id, dataset_id, dataset_id),
        )
        row = cursor.fetchone()

    if row is None:
        raise AssertionError("PostgreSQL returned no historical queue counts")

    return (int(row["build_count"]), int(row["candidate_count"]), int(row["publication_count"]))


def test_nonchronological_historical_queue_keeps_newest_period_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Process March, January, February arrivals without regressing current."""

    token = uuid4().hex[:12]
    workspace, captures = _create_historical_workspace(base_dir=tmp_path, token=token)
    provenance = fixed_code_provenance()

    def collect_test_provenance(*, definition_path: Path) -> CodeProvenance:
        assert definition_path == workspace.root.resolve()
        return provenance

    monkeypatch.setattr(runtime_composition, "collect_code_provenance", collect_test_provenance)

    # Land March and January without running Silver yet.
    for run_label, capture in zip(("march", "january"), captures[:2], strict=True):
        services = runtime_services(
            token=token, run_label=run_label, source_capture_id=capture.source_capture_id
        )
        _run_capture(
            workspace=workspace,
            capture=capture,
            services=replace(services, clock=FrozenClock(capture.ingested_at)),
            run_silver=False,
        )

    _enable_silver(workspace)

    build_ids = tuple(_uuid(token, f"build-{index}") for index in range(1, 4))
    candidate_ids = tuple(f"candidate_{token}_{index}" for index in range(1, 4))
    queue_state = _run_capture(
        workspace=workspace,
        capture=captures[2],
        services=_queue_services(
            token=token,
            capture=captures[2],
            silver_build_ids=build_ids,
            candidate_ids=candidate_ids,
        ),
        run_silver=True,
    )
    process_result = queue_state.action_results["silver.process"]

    assert isinstance(process_result, SilverProcessResult)
    assert process_result.finalized_count == 3
    assert process_result.skipped_count == 0

    expected_periods = [date(2026, 3, 1), date(2026, 1, 1), date(2026, 2, 1)]
    expected_partitions = ["2026-03", "2026-01", "2026-02"]

    with PostgresSession(_test_dsn()) as session:
        builds_by_id = PostgresSilverBuildStore(session).find_by_ids(build_ids)
        candidates = PostgresDatasetPublicationCandidateStore(session)
        ordered_builds = [builds_by_id[build_id] for build_id in build_ids]
        ordered_candidates = [candidates.get_by_id(candidate_id) for candidate_id in candidate_ids]

    assert [build.version_period for build in ordered_builds] == expected_periods
    assert [build.partition_value for build in ordered_builds] == expected_partitions
    assert all(build.status is SilverBuildStatus.SUCCEEDED for build in ordered_builds)
    assert all(candidate is not None for candidate in ordered_candidates)
    assert [
        candidate.version_period for candidate in ordered_candidates if candidate is not None
    ] == expected_periods

    publication_results: list[ApprovedPublicationResult] = []
    history_paths: tuple[Path, ...] = ()
    publication_time = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    for index, candidate_id in enumerate(candidate_ids, start=1):
        result, history_paths = _publish_and_refresh(
            workspace=workspace,
            candidate_id=candidate_id,
            publication_id=f"publication_{token}_{index}",
            published_at=publication_time + timedelta(minutes=index),
        )
        publication_results.append(result)

    # March arrived first and remains current after older January and February publish.
    assert [result.publication.version_period for result in publication_results] == expected_periods
    assert all(
        result.current_publication.publication_id == f"publication_{token}_1"
        for result in publication_results
    )
    assert all(
        result.current_publication.version_period == date(2026, 3, 1)
        for result in publication_results
    )

    with PostgresSession(_test_dsn()) as session:
        publications = PostgresDatasetPublicationStore(session)
        current = publications.find_current(workspace.dataset_id)
        active = publications.list_active(dataset_id=workspace.dataset_id)

    assert current is not None
    assert current.publication_id == f"publication_{token}_1"
    assert current.version_period == date(2026, 3, 1)
    assert {publication.version_period for publication in active} == {
        date(2026, 1, 1),
        date(2026, 2, 1),
        date(2026, 3, 1),
    }

    layout = WorkspaceLayout(
        location=WorkspaceLocation.portable(
            workspace_name=workspace.name, workspace_root=workspace.root
        )
    )
    pointer = _read_latest_pointer(layout, workspace.dataset_id)

    assert pointer is not None
    assert pointer["publication_id"] == f"publication_{token}_1"
    assert pointer["version_period"] == "2026-03-01"
    assert len(history_paths) == 1

    history_sql = history_paths[0].read_text(encoding="utf-8")
    for partition_value in expected_partitions:
        assert f"version_period={partition_value}" in history_sql

    counts_before_repeat = _dataset_counts(workspace.dataset_id)
    repeat_services = runtime_services(
        token=token, run_label="repeat", source_capture_id=captures[2].source_capture_id
    )
    repeat_state = _run_capture(
        workspace=workspace,
        capture=captures[2],
        services=replace(
            repeat_services, clock=FrozenClock(datetime(2026, 8, 15, 10, 0, tzinfo=UTC))
        ),
        run_silver=True,
    )
    repeat_result = repeat_state.action_results["silver.process"]

    assert isinstance(repeat_result, SilverProcessResult)
    assert repeat_result.finalized_count == 0
    assert repeat_result.skipped_count == 3
    assert _dataset_counts(workspace.dataset_id) == counts_before_repeat == (3, 3, 3)

    repeated_pointer = _read_latest_pointer(layout, workspace.dataset_id)
    assert repeated_pointer == pointer


def _read_latest_pointer(layout: WorkspaceLayout, dataset_id: str) -> dict[str, object]:
    path = layout.silver_latest_dir / pointer_file_name(dataset_id)
    return json.loads(path.read_text(encoding="utf-8"))
