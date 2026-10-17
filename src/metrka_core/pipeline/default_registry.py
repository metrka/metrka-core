"""Default composition of reusable Metrka pipeline components."""

from __future__ import annotations

from typing import TYPE_CHECKING

from metrka_core.pipeline.acquisition.http_files import extract_http_files
from metrka_core.pipeline.action_models import ActionDefinition
from metrka_core.pipeline.actions.bronze import (
    BronzeIngestActionDeps,
    ingest_bronze_action,
    parse_bronze_ingest_options,
)
from metrka_core.pipeline.actions.documentation import (
    DocumentationBindDeps,
    bind_documentation_action,
    parse_documentation_bind_options,
)
from metrka_core.pipeline.actions.silver import (
    SilverProcessActionDeps,
    parse_silver_process_options,
    process_silver_action,
)
from metrka_core.pipeline.registry import PipelineRegistry

if TYPE_CHECKING:
    from metrka_core.pipeline.context import PipelineContext


def _resolve_bronze_dependencies(context: PipelineContext) -> BronzeIngestActionDeps:
    return BronzeIngestActionDeps(
        bronze=context.bronze, source_captures=context.metadata.source_captures
    )


def _resolve_documentation_dependencies(context: PipelineContext) -> DocumentationBindDeps:
    return DocumentationBindDeps(
        source_config=context.workspace.source_config,
        execution_logs=context.metadata.execution_logs,
    )


def _resolve_silver_dependencies(context: PipelineContext) -> SilverProcessActionDeps:
    return SilverProcessActionDeps(silver=context.silver)


def create_core_registry() -> PipelineRegistry:
    """Create the standard registry used by Metrka pipelines."""

    registry = PipelineRegistry()

    registry.register_extractor("http.files", extract_http_files)

    registry.register_action(
        ActionDefinition(
            key="bronze.ingest",
            parse_options=parse_bronze_ingest_options,
            resolve_dependencies=_resolve_bronze_dependencies,
            handler=ingest_bronze_action,
        )
    )
    registry.register_action(
        ActionDefinition(
            key="documentation.bind",
            parse_options=parse_documentation_bind_options,
            resolve_dependencies=_resolve_documentation_dependencies,
            handler=bind_documentation_action,
        )
    )
    registry.register_action(
        ActionDefinition(
            key="silver.process",
            parse_options=parse_silver_process_options,
            resolve_dependencies=_resolve_silver_dependencies,
            handler=process_silver_action,
        )
    )

    return registry
