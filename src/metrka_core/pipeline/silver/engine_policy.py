"""Approval policy for Silver engine execution."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from metrka_core.pipeline.config import RuntimeConfigError, RuntimeEnvironment
from metrka_core.pipeline.silver.engine_models import SilverEnginePolicy, SilverEngineRuntime
from metrka_core.pipeline.silver.postgres_engine_store import PostgresSilverEngineReleaseStore


class SilverEngineGateStatus(StrEnum):
    """Result of evaluating the Silver engine gate."""

    ALLOWED = "allowed"
    APPROVAL_REQUIRED = "approval_required"


@dataclass(frozen=True)
class SilverEngineGateDecision:
    """Decision made before any Silver materialization."""

    status: SilverEngineGateStatus
    candidate_engine_release_id: str
    approved_engine_release_id: str | None
    message: str

    @property
    def allowed(self) -> bool:
        return self.status is SilverEngineGateStatus.ALLOWED


def resolve_silver_engine_policy(
    *, runtime_environment: RuntimeEnvironment, configured_policy: SilverEnginePolicy | str | None
) -> SilverEnginePolicy:
    """Validate the Silver engine policy."""

    if configured_policy is None:
        return (
            SilverEnginePolicy.REQUIRE_APPROVED
            if runtime_environment is RuntimeEnvironment.PRODUCTION
            else SilverEnginePolicy.ALLOW_CANDIDATE
        )

    if isinstance(configured_policy, SilverEnginePolicy):
        policy = configured_policy
    else:
        normalized_policy = configured_policy.strip().lower()

        try:
            policy = SilverEnginePolicy(normalized_policy)
        except ValueError as exc:
            raise RuntimeConfigError(
                "METRKA_SILVER_ENGINE_POLICY must be 'allow_candidate' or 'require_approved'"
            ) from exc

    if (
        runtime_environment is RuntimeEnvironment.PRODUCTION
        and policy is not SilverEnginePolicy.REQUIRE_APPROVED
    ):
        raise RuntimeConfigError("Production cannot disable Silver engine approval")

    return policy


def evaluate_silver_engine_gate(
    *, runtime: SilverEngineRuntime, release_store: PostgresSilverEngineReleaseStore
) -> SilverEngineGateDecision:
    """Decide whether the installed engine may run Silver."""

    approved_release = release_store.find_approved()

    approved_release_id = (
        approved_release.engine_release_id if approved_release is not None else None
    )

    if runtime.policy is SilverEnginePolicy.ALLOW_CANDIDATE:
        return SilverEngineGateDecision(
            status=SilverEngineGateStatus.ALLOWED,
            candidate_engine_release_id=runtime.release.engine_release_id,
            approved_engine_release_id=approved_release_id,
            message="Candidate Silver engine is allowed by the development policy.",
        )

    if approved_release is None:
        return SilverEngineGateDecision(
            status=SilverEngineGateStatus.APPROVAL_REQUIRED,
            candidate_engine_release_id=runtime.release.engine_release_id,
            approved_engine_release_id=None,
            message="Silver processing was deferred because no Silver engine is approved.",
        )

    if approved_release.identity.release_hash != runtime.identity.release_hash:
        return SilverEngineGateDecision(
            status=SilverEngineGateStatus.APPROVAL_REQUIRED,
            candidate_engine_release_id=runtime.release.engine_release_id,
            approved_engine_release_id=approved_release.engine_release_id,
            message=(
                "Silver processing was deferred because "
                "the installed engine differs from the "
                "approved engine."
            ),
        )

    return SilverEngineGateDecision(
        status=SilverEngineGateStatus.ALLOWED,
        candidate_engine_release_id=runtime.release.engine_release_id,
        approved_engine_release_id=approved_release.engine_release_id,
        message="The installed Silver engine is approved.",
    )
