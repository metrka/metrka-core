"""Compose runtime identity and policy for one pipeline execution."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.provenance import CodeProvenance, collect_code_provenance
from metrka_core.pipeline.runtime_services import PipelineRunIdGenerator
from metrka_core.pipeline.silver.engine_fingerprint import calculate_silver_engine_identity
from metrka_core.pipeline.silver.engine_models import SilverEngineIdentity, SilverEnginePolicy

logger = logging.getLogger(__name__)


class DirtyWorkingTreeError(RuntimeError):
    """Production execution was attempted from a dirty checkout."""


def require_runtime_preconditions(
    *, runtime_environment: RuntimeEnvironment, code_provenance: CodeProvenance
) -> None:
    """Reject invalid runtime states before infrastructure is opened."""

    if runtime_environment is RuntimeEnvironment.PRODUCTION and code_provenance.dirty:
        raise DirtyWorkingTreeError(
            "Production pipeline execution requires clean "
            "metrka-core and dataset repositories. "
            "Commit, remove or ignore all local changes "
            "before running the pipeline."
        )


@dataclass(frozen=True)
class RuntimeComposition:
    """Runtime identity and execution policy for one pipeline run."""

    pipeline_run_id: str
    code_provenance: CodeProvenance
    runtime_environment: RuntimeEnvironment
    silver_engine_identity: SilverEngineIdentity
    silver_engine_policy: SilverEnginePolicy


def build_runtime_composition(
    *,
    definition_path: Path,
    runtime_environment: RuntimeEnvironment,
    silver_engine_policy: SilverEnginePolicy,
    run_ids: PipelineRunIdGenerator,
) -> RuntimeComposition:
    """Resolve runtime identity, provenance and Silver execution policy."""

    code_provenance = collect_code_provenance(definition_path=definition_path)

    require_runtime_preconditions(
        runtime_environment=runtime_environment, code_provenance=code_provenance
    )

    pipeline_run_id = run_ids.new_pipeline_run_id()

    if not pipeline_run_id.strip():
        raise ValueError("PipelineRunIdGenerator returned an empty pipeline run ID")

    silver_engine_identity = calculate_silver_engine_identity()

    logger.info(
        (
            "Initialized pipeline run %s: "
            "metrka-core=%s@%s branch=%s; "
            "dataset-repository=%s version=%s@%s branch=%s; "
            "dirty=%s"
        ),
        pipeline_run_id,
        code_provenance.metrka_core.package_version,
        code_provenance.metrka_core.commit_sha[:12],
        code_provenance.metrka_core.branch or "detached",
        code_provenance.dataset_repository.repository,
        code_provenance.dataset_repository.package_version or "unversioned",
        code_provenance.dataset_repository.commit_sha[:12],
        code_provenance.dataset_repository.branch or "detached",
        code_provenance.dirty,
    )

    return RuntimeComposition(
        pipeline_run_id=pipeline_run_id,
        code_provenance=code_provenance,
        runtime_environment=runtime_environment,
        silver_engine_identity=silver_engine_identity,
        silver_engine_policy=silver_engine_policy,
    )
