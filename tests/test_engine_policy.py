from __future__ import annotations

from datetime import UTC, datetime

import pytest

from metrka_core.pipeline.config import RuntimeConfigError, RuntimeEnvironment
from metrka_core.pipeline.silver.engine_models import (
    SilverEngineIdentity,
    SilverEnginePolicy,
    SilverEngineRelease,
    SilverEngineReleaseStatus,
    SilverEngineRuntime,
)
from metrka_core.pipeline.silver.engine_policy import (
    SilverEngineGateStatus,
    evaluate_silver_engine_gate,
    resolve_silver_engine_policy,
)


def _identity(seed: str) -> SilverEngineIdentity:
    return SilverEngineIdentity(
        release_hash=seed * 64,
        engine_hash=seed * 64,
        engine_fingerprint_version=1,
        runtime_hash=seed * 64,
        runtime_fingerprint_version=1,
        component_hashes={"transform/schema.py": seed * 64},
        runtime_versions={"python": "3.12"},
    )


def _release(seed: str, status: SilverEngineReleaseStatus) -> SilverEngineRelease:
    identity = _identity(seed)
    return SilverEngineRelease(
        engine_release_id=identity.engine_release_id,
        identity=identity,
        core_commit_sha="commit-1",
        status=status,
        detected_at=datetime(2026, 8, 13, tzinfo=UTC),
    )


class ReleaseStore:
    def __init__(self, approved: SilverEngineRelease | None) -> None:
        self.approved = approved

    def find_approved(self) -> SilverEngineRelease | None:
        return self.approved


def test_development_policy_allows_candidate_engine() -> None:
    candidate = _release("a", SilverEngineReleaseStatus.CANDIDATE)
    runtime = SilverEngineRuntime(
        identity=candidate.identity, release=candidate, policy=SilverEnginePolicy.ALLOW_CANDIDATE
    )

    decision = evaluate_silver_engine_gate(
        runtime=runtime,
        release_store=ReleaseStore(None),  # type: ignore[arg-type]
    )

    assert decision.allowed


def test_production_policy_blocks_unapproved_engine() -> None:
    approved = _release("a", SilverEngineReleaseStatus.APPROVED)
    candidate = _release("b", SilverEngineReleaseStatus.CANDIDATE)
    runtime = SilverEngineRuntime(
        identity=candidate.identity, release=candidate, policy=SilverEnginePolicy.REQUIRE_APPROVED
    )

    decision = evaluate_silver_engine_gate(
        runtime=runtime,
        release_store=ReleaseStore(approved),  # type: ignore[arg-type]
    )

    assert decision.status is SilverEngineGateStatus.APPROVAL_REQUIRED
    assert not decision.allowed


def test_production_policy_allows_matching_approved_engine() -> None:
    approved = _release("a", SilverEngineReleaseStatus.APPROVED)
    runtime = SilverEngineRuntime(
        identity=approved.identity, release=approved, policy=SilverEnginePolicy.REQUIRE_APPROVED
    )

    decision = evaluate_silver_engine_gate(
        runtime=runtime,
        release_store=ReleaseStore(approved),  # type: ignore[arg-type]
    )

    assert decision.allowed


def test_development_defaults_to_candidate_policy() -> None:
    policy = resolve_silver_engine_policy(
        runtime_environment=RuntimeEnvironment.DEVELOPMENT, configured_policy=None
    )

    assert policy is SilverEnginePolicy.ALLOW_CANDIDATE


def test_production_defaults_to_approved_policy() -> None:
    policy = resolve_silver_engine_policy(
        runtime_environment=RuntimeEnvironment.PRODUCTION, configured_policy=None
    )

    assert policy is SilverEnginePolicy.REQUIRE_APPROVED


def test_production_cannot_disable_engine_approval() -> None:
    with pytest.raises(RuntimeConfigError, match="cannot disable"):
        resolve_silver_engine_policy(
            runtime_environment=RuntimeEnvironment.PRODUCTION,
            configured_policy=SilverEnginePolicy.ALLOW_CANDIDATE,
        )


def test_invalid_configured_policy_is_rejected() -> None:
    with pytest.raises(RuntimeConfigError, match="METRKA_SILVER_ENGINE_POLICY"):
        resolve_silver_engine_policy(
            runtime_environment=RuntimeEnvironment.DEVELOPMENT, configured_policy="unsafe"
        )
