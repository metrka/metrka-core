"""Deterministic services and workspace fixture for full-pipeline tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import count
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from metrka_core.pipeline.acquisition.models import (
    SourceCapture,
    SourceCaptureAssetReceipt,
    SourceCaptureReceipt,
)
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.provenance import CodeProvenance, GitCodeRevision
from metrka_core.storage.landing_store import LocalLandingStore

PIPELINE_TIME = datetime(2026, 8, 14, 10, 0, tzinfo=UTC)
TARGET_DATE = "2026-08-14"


@dataclass(frozen=True)
class FrozenClock:
    """Return one fixed UTC timestamp."""

    value: datetime = PIPELINE_TIME

    def now_utc(self) -> datetime:
        return self.value


@dataclass(frozen=True)
class FixedPipelineRunIds:
    value: str

    def new_pipeline_run_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class FixedSourceCaptureIds:
    value: str

    def new_source_capture_id(self, *, captured_at: datetime) -> str:
        _ = captured_at
        return self.value


@dataclass(frozen=True)
class FixedDatasetFileIds:
    value: str

    def new_dataset_file_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class FixedBronzeRunIds:
    value: str

    def new_bronze_run_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class FixedSilverBuildIds:
    value: str

    def new_silver_build_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class FixedPublicationCandidateIds:
    value: str

    def new_publication_candidate_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class FixedSourceSchemaSnapshotIds:
    value: str

    def new_source_schema_snapshot_id(self) -> str:
        return self.value


class SequentialTransformationImpactIds:
    """Return stable, unique impact IDs within one run."""

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix
        self._numbers = count(1)

    def new_transformation_impact_id(self) -> str:
        return f"impact_{self._prefix}_{next(self._numbers)}"


@dataclass(frozen=True)
class FixedPublicationIds:
    value: str

    def new_publication_id(self) -> str:
        return self.value


@dataclass(frozen=True)
class DeterministicWorkspace:
    name: str
    root: Path
    dataset_id: str
    source_capture_id: str


def _uuid(token: str, label: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"metrka-integration:{token}:{label}"))


def runtime_services(*, token: str, run_label: str, source_capture_id: str) -> RuntimeServices:
    """Build deterministic operational services for one test run."""

    return RuntimeServices(
        clock=FrozenClock(),
        pipeline_run_ids=FixedPipelineRunIds(f"pipeline_{token}_{run_label}"),
        source_capture_ids=FixedSourceCaptureIds(source_capture_id),
        dataset_file_ids=FixedDatasetFileIds(_uuid(token, f"dataset-file-{run_label}")),
        bronze_run_ids=FixedBronzeRunIds(f"bronze_{token}_{run_label}"),
        silver_build_ids=FixedSilverBuildIds(_uuid(token, f"silver-build-{run_label}")),
        publication_candidate_ids=FixedPublicationCandidateIds(
            f"publication_candidate_{token}_{run_label}"
        ),
        transformation_impact_ids=SequentialTransformationImpactIds(f"{token}_{run_label}"),
        source_schema_snapshot_ids=FixedSourceSchemaSnapshotIds(
            _uuid(token, f"source-schema-{run_label}")
        ),
    )


def fixed_code_provenance() -> CodeProvenance:
    """Return stable source revisions without depending on a sibling test repository."""

    return CodeProvenance(
        metrka_core=GitCodeRevision(
            repository="metrka-core",
            commit_sha="a" * 40,
            branch="integration-test",
            package_version="1.0.0",
        ),
        dataset_repository=GitCodeRevision(
            repository="metrka-datasets",
            commit_sha="b" * 40,
            branch="integration-test",
            package_version="1.0.0",
        ),
        dirty=False,
    )


def write_test_capture_receipt(
    *,
    workspace_root: Path,
    target_date: str,
    source_capture_id: str,
    captured_at: datetime,
    pipeline_run_id: str,
    asset_path: Path,
) -> None:
    """Write a valid immutable receipt for one pre-created test capture."""

    landing_root = workspace_root / "data" / "files" / "bronze" / "landing"
    capture_dir = asset_path.parent
    capture = SourceCapture(
        source_capture_id=source_capture_id,
        captured_at=captured_at,
        directory=capture_dir.resolve(),
        relative_path=capture_dir.relative_to(landing_root).as_posix(),
    )
    landing_store = LocalLandingStore(
        root=landing_root,
        clock=FrozenClock(captured_at),
        source_capture_ids=FixedSourceCaptureIds(source_capture_id),
    )
    receipt = SourceCaptureReceipt(
        source_capture_id=source_capture_id,
        pipeline_run_id=pipeline_run_id,
        captured_at=captured_at,
        assets=(
            SourceCaptureAssetReceipt(
                stream_name="people",
                relative_path=asset_path.relative_to(capture_dir).as_posix(),
                source_url="https://example.test/people.csv",
                artifact_role="data",
                size_bytes=asset_path.stat().st_size,
                source_last_modified=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC),
            ),
        ),
    )

    landing_store.write_receipt(capture, receipt)


def create_test_workspace(*, base_dir: Path, token: str) -> DeterministicWorkspace:
    """Create one minimal but complete dataset workspace."""

    workspace_name = f"deterministic_{token}"
    workspace_root = base_dir / workspace_name
    source_capture_id = f"capture_20260814T100000Z_{token[:8]}"
    capture_dir = (
        workspace_root / "data" / "files" / "bronze" / "landing" / TARGET_DATE / source_capture_id
    )
    config_dir = workspace_root / "conf"

    capture_dir.mkdir(parents=True)
    config_dir.mkdir(parents=True)

    asset_path = capture_dir / "people.csv"
    asset_path.write_text("id,year,name\n001,2025,Alice\n002,2025,Bob\n", encoding="utf-8")
    write_test_capture_receipt(
        workspace_root=workspace_root,
        target_date=TARGET_DATE,
        source_capture_id=source_capture_id,
        captured_at=PIPELINE_TIME,
        pipeline_run_id=f"pipeline_{token}_capture",
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
        grain: year
        column: year
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
    - action: silver.process
""".lstrip(),
        encoding="utf-8",
    )

    (config_dir / "people.yaml").write_text(
        """
meta:
  version: "1"
  category: health-medicine
  tags:
    - deterministic-integration

tables:
  people:
    columns:
      id:
        rename_to: id
        cast_to: string
      year:
        rename_to: year
        cast_to: int
      name:
        rename_to: name
        cast_to: string
    canonical_order:
      - id
      - year
      - name
""".lstrip(),
        encoding="utf-8",
    )

    (config_dir / "quality.yaml").write_text("version: 1\n", encoding="utf-8")

    return DeterministicWorkspace(
        name=workspace_name,
        root=workspace_root,
        dataset_id=f"{workspace_name}.people",
        source_capture_id=source_capture_id,
    )
