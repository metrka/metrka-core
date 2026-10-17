"""Narrow dependency groups used by Silver processing."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.catalog.postgres_dataset_catalog import PostgresDatasetCatalogStore
from metrka_core.datasets.source_config import SourceConfig
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.postgres_contract_metadata import PostgresContractMetadataStore
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.runtime_services import (
    Clock,
    SilverBuildIdGenerator,
    TransformationImpactIdGenerator,
)
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.engine_models import SilverEngineRuntime
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.postgres_engine_store import PostgresSilverEngineReleaseStore
from metrka_core.pipeline.silver.postgres_publication_decision_unit_of_work import (
    PostgresSilverPublicationDecisionUnitOfWork,
)
from metrka_core.quality.config import QualityConfig
from metrka_core.quality.store import QualityCheckStore
from metrka_core.storage.bronze_store import BronzeArtifactStore
from metrka_core.storage.config_store import ConfigStore
from metrka_core.storage.contract_store import ContractSnapshotStore


@dataclass(frozen=True)
class SilverInputDeps:
    """Dependencies used to locate and read Bronze inputs."""

    bronze_store: BronzeArtifactStore
    config_store: ConfigStore
    marshal: FileMarshal
    file_marshal_store: PostgresFileMarshalStore


@dataclass(frozen=True)
class SilverContractDeps:
    """Dependencies used for contracts and catalog metadata."""

    contract_store: ContractSnapshotStore
    contract_metadata_store: PostgresContractMetadataStore
    dataset_catalog_store: PostgresDatasetCatalogStore


@dataclass(frozen=True)
class SilverOutputDeps:
    """Dependencies used to persist Silver materializations."""

    silver_store: SilverArtifactStore
    silver_build_store: PostgresSilverBuildStore
    publication_decision_uow: PostgresSilverPublicationDecisionUnitOfWork


@dataclass(frozen=True)
class SilverEvidenceDeps:
    """Dependencies used to record execution evidence."""

    execution_log_store: ExecutionLogStore
    quality_store: QualityCheckStore
    transformation_impact_store: PostgresTransformationImpactStore
    transformation_impact_ids: TransformationImpactIdGenerator


@dataclass(frozen=True)
class SilverEngineDeps:
    """Dependencies used by the Silver engine gate."""

    runtime: SilverEngineRuntime
    release_store: PostgresSilverEngineReleaseStore


@dataclass(frozen=True)
class SilverProcessDeps:
    """Complete dependency projection for the Silver use case."""

    clock: Clock
    build_ids: SilverBuildIdGenerator
    source_config: SourceConfig
    quality_config: QualityConfig
    engine: SilverEngineDeps
    inputs: SilverInputDeps
    contracts: SilverContractDeps
    outputs: SilverOutputDeps
    evidence: SilverEvidenceDeps
