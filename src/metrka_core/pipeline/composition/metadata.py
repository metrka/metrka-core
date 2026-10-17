"""Compose PostgreSQL-backed metadata collaborators."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.catalog.postgres_dataset_catalog import PostgresDatasetCatalogStore
from metrka_core.catalog.postgres_publication_asset_store import (
    PostgresDatasetPublicationAssetStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.catalog.publication_asset_store import DatasetPublicationAssetStore
from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.metadata.postgres_contract_metadata import PostgresContractMetadataStore
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.metadata.postgres_source_schema import PostgresSourceSchemaStore
from metrka_core.metadata.source_schema_store import SourceSchemaStore
from metrka_core.observability.postgres_stores import (
    PostgresExecutionLogStore,
    PostgresPipelineRunStore,
)
from metrka_core.observability.stores import ExecutionLogStore, PipelineRunStore
from metrka_core.pipeline.acquisition.postgres_source_capture_store import (
    PostgresSourceCaptureStore,
)
from metrka_core.pipeline.runtime_services import Clock, SourceSchemaSnapshotIdGenerator
from metrka_core.quality.postgres_store import PostgresQualityCheckStore
from metrka_core.quality.store import QualityCheckStore


@dataclass(frozen=True)
class MetadataComposition:
    """Metadata collaborators shared by one pipeline execution."""

    source_captures: PostgresSourceCaptureStore
    file_marshal_store: PostgresFileMarshalStore
    marshal: FileMarshal
    source_schemas: SourceSchemaStore
    pipeline_runs: PipelineRunStore
    execution_logs: ExecutionLogStore
    quality_checks: QualityCheckStore
    contract_metadata: PostgresContractMetadataStore
    dataset_catalog: PostgresDatasetCatalogStore
    dataset_publications: DatasetPublicationStore
    dataset_publication_assets: DatasetPublicationAssetStore
    transformation_impacts: PostgresTransformationImpactStore


def build_metadata_composition(
    *,
    session: PostgresSession,
    pipeline_run_id: str,
    clock: Clock,
    source_schema_ids: SourceSchemaSnapshotIdGenerator,
) -> MetadataComposition:
    """Create PostgreSQL-backed metadata collaborators."""

    source_captures = PostgresSourceCaptureStore(session)

    file_marshal_store = PostgresFileMarshalStore(session)
    marshal = FileMarshal(store=file_marshal_store, clock=clock)

    source_schemas = PostgresSourceSchemaStore(
        session=session, file_marshal_store=file_marshal_store, source_schema_ids=source_schema_ids
    )

    pipeline_runs = PostgresPipelineRunStore(session)

    execution_logs = PostgresExecutionLogStore(session, pipeline_run_id=pipeline_run_id)

    quality_checks = PostgresQualityCheckStore(session, pipeline_run_id=pipeline_run_id)

    contract_metadata = PostgresContractMetadataStore(session)
    dataset_catalog = PostgresDatasetCatalogStore(session)
    dataset_publications = PostgresDatasetPublicationStore(session)
    dataset_publication_assets = PostgresDatasetPublicationAssetStore(session)
    transformation_impacts = PostgresTransformationImpactStore(session)

    return MetadataComposition(
        source_captures=source_captures,
        file_marshal_store=file_marshal_store,
        marshal=marshal,
        source_schemas=source_schemas,
        pipeline_runs=pipeline_runs,
        execution_logs=execution_logs,
        quality_checks=quality_checks,
        contract_metadata=contract_metadata,
        dataset_catalog=dataset_catalog,
        dataset_publications=dataset_publications,
        dataset_publication_assets=dataset_publication_assets,
        transformation_impacts=transformation_impacts,
    )
