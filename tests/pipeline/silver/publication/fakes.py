from __future__ import annotations

from collections.abc import Collection, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from metrka_core.catalog.publication_asset_models import (
    DatasetPublicationAsset,
    DatasetPublicationAssetRequest,
)
from metrka_core.catalog.publication_models import DatasetPublication, DatasetPublicationRequest
from metrka_core.catalog.publication_projection_models import (
    DatasetPublicationProjectionState,
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.observability.execution_events import ExecutionEvent
from metrka_core.pipeline.silver.artifact_models import (
    SilverArtifactDeletionError,
    SilverBuildArtifactDeletionResult,
    SilverBuildArtifactQuery,
)
from metrka_core.pipeline.silver.build_models import (
    RebuildMode,
    RebuildReason,
    SilverBuild,
    SilverBuildStatus,
)
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)
from metrka_core.pipeline.silver.publication_indexes import SilverPublicationIndexResult

FIXED_NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
DATASET_ID = "wi_dhs_adult_lead.county"
PROCESSING_HASH = "a" * 64
QUALITY_HASH = "b" * 64
LOGICAL_HASH = "d" * 64
SCHEMA_HASH = "e" * 64


def make_publication(
    *,
    publication_id: str = "publication-1",
    pipeline_run_id: str = "pipeline-1",
    dataset_id: str = DATASET_ID,
    version_period: date = date(2025, 1, 1),
    partition_value: str = "2025",
    revision: int = 1,
    silver_build_id: str = "build-1",
    engine_release_id: str = "engine-1",
    processing_config_hash: str = PROCESSING_HASH,
    quality_config_hash: str = QUALITY_HASH,
    fingerprint_version: int = 1,
    logical_hash_algorithm: str = LOGICAL_DATA_HASH_ALGORITHM,
    schema_hash_algorithm: str = SCHEMA_HASH_ALGORITHM,
    logical_data_hash: str = LOGICAL_HASH,
    schema_hash: str = SCHEMA_HASH,
    manifest_path: str = "data/files/silver/manifests/build-1.json",
    published_at: datetime = FIXED_NOW,
    is_active_revision: bool = True,
    is_current: bool = True,
    supersedes_publication_id: str | None = None,
) -> DatasetPublication:
    return DatasetPublication(
        publication_id=publication_id,
        pipeline_run_id=pipeline_run_id,
        dataset_id=dataset_id,
        version_period=version_period,
        partition_key="version_period",
        partition_value=partition_value,
        revision=revision,
        silver_build_id=silver_build_id,
        engine_release_id=engine_release_id,
        processing_config_hash=processing_config_hash,
        quality_config_hash=quality_config_hash,
        fingerprint_version=fingerprint_version,
        logical_hash_algorithm=logical_hash_algorithm,
        schema_hash_algorithm=schema_hash_algorithm,
        logical_data_hash=logical_data_hash,
        schema_hash=schema_hash,
        manifest_path=manifest_path,
        published_at=published_at,
        is_active_revision=is_active_revision,
        is_current=is_current,
        supersedes_publication_id=supersedes_publication_id,
    )


def make_asset_request(
    *,
    table_key: str = "adult-lead-county",
    file_path: str = (
        "data/files/silver/tables/adult-lead-county/"
        "version_period=2025/silver_build_id=build-1/data.parquet"
    ),
) -> DatasetPublicationAssetRequest:
    return DatasetPublicationAssetRequest(
        table_key=table_key,
        file_path=file_path,
        file_format="parquet",
        row_count=2044,
        column_count=2,
        columns=("county_name", "count"),
        size_bytes=12345,
        checksum="sha256:asset-checksum",
    )


def make_asset(
    *,
    publication: DatasetPublication | None = None,
    request: DatasetPublicationAssetRequest | None = None,
) -> DatasetPublicationAsset:
    publication = publication or make_publication()
    request = request or make_asset_request()
    return DatasetPublicationAsset(
        publication_id=publication.publication_id,
        dataset_id=publication.dataset_id,
        version_period=publication.version_period,
        revision=publication.revision,
        partition_key=publication.partition_key,
        partition_value=publication.partition_value,
        silver_build_id=publication.silver_build_id,
        table_key=request.table_key,
        file_path=request.file_path,
        file_format=request.file_format,
        row_count=request.row_count,
        column_count=request.column_count,
        columns=request.columns,
        size_bytes=request.size_bytes,
        checksum=request.checksum,
    )


def make_build(
    *,
    silver_build_id: str = "build-1",
    dataset_id: str = DATASET_ID,
    status: SilverBuildStatus = SilverBuildStatus.RUNNING,
    started_at: datetime = FIXED_NOW,
    completed_at: datetime | None = None,
    version_period: date | None = date(2025, 1, 1),
    partition_value: str | None = "2025",
    manifest_path: str | None = "data/files/silver/manifests/build-1.json",
    logical_data_hash: str | None = None,
    schema_hash: str | None = None,
    output_hash: str | None = None,
) -> SilverBuild:
    if status is SilverBuildStatus.SUCCEEDED:
        logical_data_hash = logical_data_hash or LOGICAL_HASH
        schema_hash = schema_hash or SCHEMA_HASH
        output_hash = output_hash or "1" * 64

    return SilverBuild(
        silver_build_id=silver_build_id,
        pipeline_run_id="pipeline-1",
        silver_run_id="silver-run-1",
        dataset_file_id="bronze-file-1",
        dataset_id=dataset_id,
        contract_hash="c" * 64,
        engine_release_id="engine-1",
        processing_config_hash=PROCESSING_HASH,
        quality_config_hash=QUALITY_HASH,
        build_signature="f" * 64,
        fingerprint_version=1,
        logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
        schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
        status=status,
        rebuild_mode=RebuildMode.AUTOMATIC,
        rebuild_reasons=(RebuildReason.INITIAL_BUILD,),
        started_at=started_at,
        version_period=version_period,
        partition_key="version_period" if version_period is not None else None,
        partition_value=partition_value,
        logical_data_hash=logical_data_hash,
        schema_hash=schema_hash,
        manifest_path=manifest_path,
        output_hash=output_hash,
        output_file_count=1 if status is SilverBuildStatus.SUCCEEDED else None,
        output_byte_count=12345 if status is SilverBuildStatus.SUCCEEDED else None,
        completed_at=completed_at,
    )


def make_manifest(
    *,
    publication: DatasetPublication | None = None,
    contract_snapshot_path: str = "data/contracts/example/contract.yaml",
    contract_checksum: str = "sha256:" + "c" * 64,
) -> dict[str, Any]:
    publication = publication or make_publication()
    request = make_asset_request()
    return {
        "schema_version": 1,
        "artifact_type": "silver_build_manifest",
        "dataset_id": publication.dataset_id,
        "silver_build_id": publication.silver_build_id,
        "bronze_run_id": "bronze-run-1",
        "silver_run_id": "silver-run-1",
        "version_period": publication.version_period.isoformat(),
        "version_period_grain": "year",
        "version_period_source": "column:year",
        "partition_key": publication.partition_key,
        "partition_value": publication.partition_value,
        "fingerprints": {
            "fingerprint_version": publication.fingerprint_version,
            "logical_data_algorithm": publication.logical_hash_algorithm,
            "schema_algorithm": publication.schema_hash_algorithm,
            "logical_data_hash": publication.logical_data_hash,
            "schema_hash": publication.schema_hash,
            "tables": [],
        },
        "contract": {
            "name": "contract.yaml",
            "path": "conf/contract.yaml",
            "snapshot_path": contract_snapshot_path,
            "version": None,
            "checksum": contract_checksum,
        },
        "tables": [
            {
                "dataset_id": publication.dataset_id,
                "table_key": request.table_key,
                "partition_key": publication.partition_key,
                "partition_value": publication.partition_value,
                "silver_build_id": publication.silver_build_id,
                "path": request.file_path,
                "format": request.file_format,
                "row_count": request.row_count,
                "column_count": request.column_count,
                "columns": list(request.columns),
                "size_bytes": request.size_bytes,
                "checksum": request.checksum,
            }
        ],
    }


class RecordingExecutionLogStore:
    def __init__(self, *, fail_on_event_type: str | None = None) -> None:
        self.records: list[ExecutionEvent] = []
        self.fail_on_event_type = fail_on_event_type

    def insert_execution_log(self, event: ExecutionEvent) -> None:
        self.records.append(event)

        if event.event_type == self.fail_on_event_type:
            raise RuntimeError("execution log unavailable")


class FakePublicationStore:
    def __init__(self, publications: list[DatasetPublication] | None = None) -> None:
        self.publications = list(publications or [])

    def publish(self, request: DatasetPublicationRequest) -> DatasetPublication:
        raise NotImplementedError

    def get_by_id(self, publication_id: str) -> DatasetPublication | None:
        return next(
            (item for item in self.publications if item.publication_id == publication_id), None
        )

    def find_current(self, dataset_id: str) -> DatasetPublication | None:
        return next(
            (
                item
                for item in self.publications
                if item.dataset_id == dataset_id and item.is_current
            ),
            None,
        )

    def find_active(self, *, dataset_id: str, partition_value: str) -> DatasetPublication | None:
        return next(
            (
                item
                for item in self.publications
                if item.dataset_id == dataset_id
                and item.partition_value == partition_value
                and item.is_active_revision
            ),
            None,
        )

    def list_active(self, *, dataset_id: str) -> list[DatasetPublication]:
        return [
            item
            for item in self.publications
            if item.dataset_id == dataset_id and item.is_active_revision
        ]

    def list_all(self, *, dataset_id: str) -> list[DatasetPublication]:
        return [item for item in self.publications if item.dataset_id == dataset_id]


class InMemoryPublicationAssetStore:
    def __init__(self, *, publications: FakePublicationStore) -> None:
        self.publications = publications
        self.assets: dict[str, tuple[DatasetPublicationAsset, ...]] = {}
        self.register_calls: list[str] = []

    def register(
        self, *, publication_id: str, assets: tuple[DatasetPublicationAssetRequest, ...]
    ) -> tuple[DatasetPublicationAsset, ...]:
        publication = self.publications.get_by_id(publication_id)
        if publication is None:
            raise RuntimeError("unknown publication")
        registered = tuple(
            make_asset(publication=publication, request=request) for request in assets
        )
        self.assets[publication_id] = registered
        self.register_calls.append(publication_id)
        return registered

    def list_for_publication(self, *, publication_id: str) -> tuple[DatasetPublicationAsset, ...]:
        return self.assets.get(publication_id, ())

    def list_active(self, *, dataset_id: str) -> tuple[DatasetPublicationAsset, ...]:
        active_ids = {
            item.publication_id for item in self.publications.list_active(dataset_id=dataset_id)
        }
        return tuple(
            asset
            for publication_id, assets in self.assets.items()
            if publication_id in active_ids
            for asset in assets
        )


class InMemoryPublicationProjectionStateStore:
    def __init__(self) -> None:
        self.states: dict[
            tuple[str, PublicationProjectionKind], DatasetPublicationProjectionState
        ] = {}

    def mark_pending(
        self,
        *,
        dataset_id: str,
        current_publication_id: str,
        history_publication_id: str,
        changed_at: datetime,
    ) -> tuple[DatasetPublicationProjectionState, ...]:
        results: list[DatasetPublicationProjectionState] = []

        for projection_kind in PublicationProjectionKind:
            previous = self.states.get((dataset_id, projection_kind))
            expected_publication_id = (
                current_publication_id
                if projection_kind is PublicationProjectionKind.CURRENT
                else history_publication_id
            )

            if (
                projection_kind is PublicationProjectionKind.CURRENT
                and previous is not None
                and previous.expected_publication_id == expected_publication_id
            ):
                results.append(previous)
                continue

            state = DatasetPublicationProjectionState(
                dataset_id=dataset_id,
                projection_kind=projection_kind,
                expected_publication_id=expected_publication_id,
                projected_publication_id=(
                    previous.projected_publication_id if previous is not None else None
                ),
                status=PublicationProjectionStatus.PENDING,
                status_changed_at=changed_at,
                last_synchronized_at=(
                    previous.last_synchronized_at if previous is not None else None
                ),
            )
            self.states[(dataset_id, projection_kind)] = state
            results.append(state)

        return tuple(results)

    def mark_synchronized(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        publication_id: str,
        checked_at: datetime,
    ) -> DatasetPublicationProjectionState:
        previous = self.states.get((dataset_id, projection_kind))

        if previous is not None and previous.expected_publication_id != publication_id:
            return previous

        state = DatasetPublicationProjectionState(
            dataset_id=dataset_id,
            projection_kind=projection_kind,
            expected_publication_id=publication_id,
            projected_publication_id=publication_id,
            status=PublicationProjectionStatus.SYNCHRONIZED,
            status_changed_at=checked_at,
            last_attempted_at=checked_at,
            last_synchronized_at=checked_at,
        )
        self.states[(dataset_id, projection_kind)] = state
        return state

    def mark_stale(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        expected_publication_id: str,
        checked_at: datetime,
        error: Mapping[str, Any],
    ) -> DatasetPublicationProjectionState:
        previous = self.states.get((dataset_id, projection_kind))

        if previous is not None and previous.expected_publication_id != expected_publication_id:
            return previous

        state = DatasetPublicationProjectionState(
            dataset_id=dataset_id,
            projection_kind=projection_kind,
            expected_publication_id=expected_publication_id,
            projected_publication_id=(
                previous.projected_publication_id if previous is not None else None
            ),
            status=PublicationProjectionStatus.STALE,
            status_changed_at=checked_at,
            last_attempted_at=checked_at,
            last_synchronized_at=(previous.last_synchronized_at if previous is not None else None),
            error=dict(error),
        )
        self.states[(dataset_id, projection_kind)] = state
        return state

    def get(
        self, *, dataset_id: str, projection_kind: PublicationProjectionKind
    ) -> DatasetPublicationProjectionState | None:
        return self.states.get((dataset_id, projection_kind))


class FakeSilverArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.manifests: dict[str, dict[str, Any]] = {}
        self.manifest_reads: list[str] = []
        self.pointer_payloads: dict[str, dict[str, Any]] = {}
        self.pointer_error: Exception | None = None
        self.written_views: list[tuple[str, str, str | None, str]] = []
        self.build_directories: dict[str, tuple[Path, ...]] = {}
        self.build_directory_requests: list[frozenset[str] | None] = []
        self.deletion_attempted_build_ids: list[str] = []
        self.deleted_build_ids: list[str] = []
        self.deletion_errors_by_build: dict[str, tuple[SilverArtifactDeletionError, ...]] = {}

    def read_manifest(self, *, path: str) -> dict[str, Any]:
        self.manifest_reads.append(path)
        return dict(self.manifests[path])

    def relative_path(self, path: str | Path) -> str:
        candidate = Path(path)
        try:
            return candidate.relative_to(self.root).as_posix()
        except ValueError:
            return candidate.as_posix()

    def write_latest_pointer(self, *, dataset_id: str, payload: dict[str, Any]) -> Path:
        if self.pointer_error is not None:
            raise self.pointer_error

        self.pointer_payloads[dataset_id] = dict(payload)
        return self.root / "current" / f"{dataset_id}.json"

    def write_latest_view(self, *, table_key: str, publication_id: str, content: str) -> Path:
        self.written_views.append((table_key, "latest", publication_id, content))
        return self.root / "views" / table_key / f"publication={publication_id}" / "latest.sql"

    def write_history_view(self, *, table_key: str, content: str) -> Path:
        self.written_views.append((table_key, "history", None, content))
        return self.root / "views" / table_key / "history.sql"

    def list_build_artifact_directories(
        self, *, builds: Collection[SilverBuildArtifactQuery] | None = None
    ) -> dict[str, tuple[Path, ...]]:
        requested = None if builds is None else frozenset(build.silver_build_id for build in builds)
        self.build_directory_requests.append(requested)

        if requested is None:
            return dict(self.build_directories)

        return {
            silver_build_id: directories
            for silver_build_id, directories in self.build_directories.items()
            if silver_build_id in requested
        }

    def delete_build_artifact_directories(
        self, *, silver_build_id: str, artifact_directories: Collection[Path] | None = None
    ) -> SilverBuildArtifactDeletionResult:
        self.deletion_attempted_build_ids.append(silver_build_id)
        requested = (
            tuple(artifact_directories)
            if artifact_directories is not None
            else self.build_directories.get(silver_build_id, ())
        )
        errors = self.deletion_errors_by_build.get(silver_build_id, ())
        failed_paths = {error.artifact_directory for error in errors}
        deleted_directories = tuple(path for path in requested if path not in failed_paths)

        if not errors:
            self.deleted_build_ids.append(silver_build_id)

        return SilverBuildArtifactDeletionResult(
            silver_build_id=silver_build_id,
            requested_directories=requested,
            deleted_directories=deleted_directories,
            errors=errors,
        )


class FakeSilverBuildStore:
    def __init__(self, builds: list[SilverBuild] | None = None) -> None:
        self.builds = {build.silver_build_id: build for build in builds or []}
        self.find_by_ids_requests: list[frozenset[str]] = []
        self.dataset_requests: list[str] = []

    def get_by_id(self, silver_build_id: str) -> SilverBuild | None:
        return self.builds.get(silver_build_id)

    def find_by_ids(self, silver_build_ids: Collection[str]) -> dict[str, SilverBuild]:
        requested = frozenset(silver_build_ids)
        self.find_by_ids_requests.append(requested)
        return {
            silver_build_id: build
            for silver_build_id, build in self.builds.items()
            if silver_build_id in requested
        }

    def list_for_dataset(self, *, dataset_id: str) -> tuple[SilverBuild, ...]:
        self.dataset_requests.append(dataset_id)
        return tuple(build for build in self.builds.values() if build.dataset_id == dataset_id)


class FakePublicationIndexService:
    def __init__(
        self,
        *,
        current_result: SilverPublicationIndexResult,
        history_paths: tuple[Path, ...] = (),
        current_error: Exception | None = None,
        history_error: Exception | None = None,
    ) -> None:
        self.current_result = current_result
        self.history_paths = history_paths
        self.current_error = current_error
        self.history_error = history_error
        self.current_calls = 0
        self.history_calls = 0

    def refresh_current(self, *, dataset_id: str) -> SilverPublicationIndexResult:
        self.current_calls += 1
        if self.current_error is not None:
            raise self.current_error
        return self.current_result

    def rebuild_history(self, *, dataset_id: str) -> tuple[Path, ...]:
        self.history_calls += 1
        if self.history_error is not None:
            raise self.history_error
        return self.history_paths
