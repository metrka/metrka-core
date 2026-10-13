"""Stable public entry points for running Metrka pipelines."""

from metrka_core.catalog.contract_snapshot_reader import (
    ContractSnapshotFailure,
    ContractSnapshotReader,
    ContractSnapshotReadError,
)
from metrka_core.catalog.publication_manifest_reader import (
    PublicationManifestFailure,
    PublicationManifestReader,
    PublicationManifestReadError,
)
from metrka_core.datasets.dataset_identity import (
    DatasetIdentity,
    DatasetIdentityError,
    RunName,
    WorkspaceName,
    parse_dataset_id,
)
from metrka_core.datasets.main_templates import MainTemplateError, load_main_config
from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.scaffolding import WorkspaceInitializationResult, initialize_workspace
from metrka_core.datasets.workspace_export import (
    WorkspaceExportContentPolicyError,
    WorkspaceExportIntegrityError,
    WorkspaceExportPolicyViolation,
    WorkspaceExportResult,
    WorkspaceExportVerificationResult,
    export_workspace,
    verify_workspace_export,
)
from metrka_core.datasets.workspace_import import WorkspaceImportResult, import_workspace
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement
from metrka_core.pipeline.bootstrap import open_pipeline_context
from metrka_core.pipeline.composition.runtime_services import RuntimeServices
from metrka_core.pipeline.composition.workspace_locations import create_workspace_location_resolver
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.default_registry import create_core_registry
from metrka_core.pipeline.models import PipelineRunState
from metrka_core.pipeline.registry import PipelineRegistry
from metrka_core.pipeline.run import PipelineBootstrapOptions, PipelineRunResult, run_pipeline
from metrka_core.pipeline.runner import execute_configured_pipeline
from metrka_core.pipeline.runtime_services import (
    BronzeRunIdGenerator,
    Clock,
    DatasetFileIdGenerator,
    PipelineRunIdGenerator,
    SilverBuildIdGenerator,
)
from metrka_core.pipeline.workspace_validation import WorkspaceValidationResult, validate_workspace
from metrka_core.storage.contract_snapshot_reader import create_contract_snapshot_reader
from metrka_core.storage.publication_manifest_reader import create_publication_manifest_reader
from metrka_core.transform.profiles import ProfileError, load_contract

__all__ = [
    "BronzeRunIdGenerator",
    "Clock",
    "ContractSnapshotFailure",
    "ContractSnapshotReadError",
    "ContractSnapshotReader",
    "DatasetFileIdGenerator",
    "DatasetIdentity",
    "DatasetIdentityError",
    "MainTemplateError",
    "PipelineBootstrapOptions",
    "PipelineRegistry",
    "PipelineRunIdGenerator",
    "PipelineRunResult",
    "PipelineRunState",
    "ProfileError",
    "PublicationManifestFailure",
    "PublicationManifestReadError",
    "PublicationManifestReader",
    "RuntimeEnvironment",
    "RunName",
    "RuntimeServices",
    "SilverBuildIdGenerator",
    "WorkspaceExportContentPolicyError",
    "WorkspaceExportIntegrityError",
    "WorkspaceExportPolicyViolation",
    "WorkspaceExportResult",
    "WorkspaceExportVerificationResult",
    "WorkspaceImportResult",
    "WorkspaceInitializationResult",
    "WorkspaceLocation",
    "WorkspaceLocationResolver",
    "WorkspaceName",
    "WorkspacePlacement",
    "WorkspaceValidationResult",
    "create_contract_snapshot_reader",
    "create_core_registry",
    "create_publication_manifest_reader",
    "create_workspace_location_resolver",
    "execute_configured_pipeline",
    "export_workspace",
    "import_workspace",
    "initialize_workspace",
    "load_contract",
    "load_main_config",
    "open_pipeline_context",
    "parse_dataset_id",
    "run_pipeline",
    "validate_workspace",
    "verify_workspace_export",
]
