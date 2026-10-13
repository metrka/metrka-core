"""Models describing versioned Silver processing engines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType


class SilverEngineReleaseStatus(StrEnum):
    """Lifecycle status of one Silver engine release."""

    CANDIDATE = "candidate"
    APPROVED = "approved"
    REJECTED = "rejected"
    RETIRED = "retired"


class SilverEnginePolicy(StrEnum):
    """Policy controlling execution of candidate Silver engines."""

    ALLOW_CANDIDATE = "allow_candidate"
    REQUIRE_APPROVED = "require_approved"


@dataclass(frozen=True)
class SilverEngineIdentity:
    """Deterministic identity of Silver code and its runtime."""

    release_hash: str
    engine_hash: str
    engine_fingerprint_version: int
    runtime_hash: str
    runtime_fingerprint_version: int
    component_hashes: Mapping[str, str]
    runtime_versions: Mapping[str, str]

    def __post_init__(self) -> None:
        hash_fields = {
            "release_hash": self.release_hash,
            "engine_hash": self.engine_hash,
            "runtime_hash": self.runtime_hash,
        }

        for field_name, value in hash_fields.items():
            if len(value) != 64:
                raise ValueError(f"{field_name} must be a SHA-256 hash")

        if self.engine_fingerprint_version < 1:
            raise ValueError("engine_fingerprint_version must be positive")

        if self.runtime_fingerprint_version < 1:
            raise ValueError("runtime_fingerprint_version must be positive")

        if not self.component_hashes:
            raise ValueError("component_hashes must not be empty")

        if not self.runtime_versions:
            raise ValueError("runtime_versions must not be empty")

        object.__setattr__(
            self, "component_hashes", MappingProxyType(dict(sorted(self.component_hashes.items())))
        )

        object.__setattr__(
            self, "runtime_versions", MappingProxyType(dict(sorted(self.runtime_versions.items())))
        )

    @property
    def engine_release_id(self) -> str:
        """Return a readable deterministic release identifier."""

        return f"silver_engine_{self.release_hash[:24]}"


@dataclass(frozen=True)
class SilverEngineRelease:
    """Persisted lifecycle record for one engine identity."""

    engine_release_id: str
    identity: SilverEngineIdentity
    core_commit_sha: str
    status: SilverEngineReleaseStatus
    detected_at: datetime
    approved_at: datetime | None = None
    approved_by: str | None = None
    rejected_at: datetime | None = None
    rejected_by: str | None = None
    rejection_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.engine_release_id.strip():
            raise ValueError("engine_release_id must not be empty")

        if not self.core_commit_sha.strip():
            raise ValueError("core_commit_sha must not be empty")

        timestamps = {
            "detected_at": self.detected_at,
            "approved_at": self.approved_at,
            "rejected_at": self.rejected_at,
        }

        for field_name, value in timestamps.items():
            if value is not None and value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True)
class SilverEngineRuntime:
    """Engine identity and policy resolved for one pipeline run."""

    identity: SilverEngineIdentity
    release: SilverEngineRelease
    policy: SilverEnginePolicy
