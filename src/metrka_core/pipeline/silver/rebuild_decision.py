"""Pure functions used to decide whether Silver must be rebuilt."""

from __future__ import annotations

import hashlib
import json

from metrka_core.pipeline.silver.build_models import (
    RebuildDecision,
    RebuildMode,
    RebuildReason,
    SilverBuild,
    SilverBuildStatus,
)

SILVER_BUILD_SIGNATURE_VERSION = 1


def calculate_silver_build_signature(
    *,
    dataset_file_id: str,
    contract_hash: str,
    engine_release_id: str,
    processing_config_hash: str,
    quality_config_hash: str,
) -> str:
    """Return a deterministic identity for one requested Silver build."""

    values = {
        "dataset_file_id": dataset_file_id,
        "contract_hash": contract_hash,
        "engine_release_id": engine_release_id,
        "processing_config_hash": processing_config_hash,
        "quality_config_hash": quality_config_hash,
    }

    for field_name, value in values.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")

    payload = {
        "signature_version": SILVER_BUILD_SIGNATURE_VERSION,
        "dataset_file_id": dataset_file_id.strip(),
        "contract_hash": contract_hash.strip(),
        "engine_release_id": engine_release_id.strip(),
        "processing_config_hash": processing_config_hash.strip(),
        "quality_config_hash": quality_config_hash.strip(),
    }

    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def decide_silver_rebuild(
    *,
    dataset_file_id: str,
    contract_hash: str,
    engine_release_id: str,
    processing_config_hash: str,
    quality_config_hash: str,
    matching_successful_build: SilverBuild | None,
    latest_successful_build: SilverBuild | None,
    latest_build_attempt: SilverBuild | None,
    force_rebuild: bool = False,
) -> RebuildDecision:
    """Decide whether one Bronze asset requires a new Silver build."""

    build_signature = calculate_silver_build_signature(
        dataset_file_id=dataset_file_id,
        contract_hash=contract_hash,
        engine_release_id=engine_release_id,
        processing_config_hash=processing_config_hash,
        quality_config_hash=quality_config_hash,
    )

    if matching_successful_build is not None and not force_rebuild:
        return RebuildDecision(
            required=False,
            mode=RebuildMode.AUTOMATIC,
            build_signature=build_signature,
            matching_silver_build_id=matching_successful_build.silver_build_id,
        )

    reasons: list[RebuildReason] = []

    def add_reason(reason: RebuildReason) -> None:
        if reason not in reasons:
            reasons.append(reason)

    if latest_successful_build is None:
        add_reason(RebuildReason.INITIAL_BUILD)
    else:
        if latest_successful_build.dataset_file_id != dataset_file_id:
            add_reason(RebuildReason.BRONZE_FILE_CHANGED)

        if latest_successful_build.contract_hash != contract_hash:
            add_reason(RebuildReason.CONTRACT_CHANGED)

        if latest_successful_build.engine_release_id != engine_release_id:
            add_reason(RebuildReason.SILVER_ENGINE_CHANGED)

        if latest_successful_build.processing_config_hash != processing_config_hash:
            add_reason(RebuildReason.PROCESSING_CONFIG_CHANGED)

        if latest_successful_build.quality_config_hash != quality_config_hash:
            add_reason(RebuildReason.QUALITY_CONFIG_CHANGED)

    if (
        latest_build_attempt is not None
        and latest_build_attempt.status is SilverBuildStatus.FAILED
        and latest_build_attempt.build_signature == build_signature
        and matching_successful_build is None
    ):
        add_reason(RebuildReason.PREVIOUS_BUILD_FAILED)

    if force_rebuild:
        add_reason(RebuildReason.MANUAL_FORCE)

    if not reasons:
        raise RuntimeError("Silver rebuild is required, but no rebuild reason could be determined")

    return RebuildDecision(
        required=True,
        mode=RebuildMode.MANUAL if force_rebuild else RebuildMode.AUTOMATIC,
        build_signature=build_signature,
        reasons=tuple(reasons),
    )
