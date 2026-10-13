"""Shared models passed between pipeline actions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from metrka_core.metadata.artifact import ArtifactRole
from metrka_core.pipeline.acquisition.models import SourceCapture
from metrka_core.pipeline.action_models import ActionExecutionResult
from metrka_core.pipeline.bronze.models import BronzeBatchResult


@dataclass(frozen=True)
class LandedAsset:
    """
    One source asset successfully written to the landing zone.

    Acquisition actions produce LandedAsset objects. Downstream pipeline
    actions, such as Bronze ingestion, consume them.
    """

    stream_name: str
    path: Path
    source_url: str
    source_capture_id: str
    artifact_role: ArtifactRole = "data"
    source_last_modified: datetime | None = None

    def __post_init__(self) -> None:
        if not self.stream_name.strip():
            raise ValueError("LandedAsset.stream_name must not be empty")

        if not self.source_url.strip():
            raise ValueError("LandedAsset.source_url must not be empty")

        if not self.source_capture_id.strip():
            raise ValueError("LandedAsset.source_capture_id must not be empty")

        if self.source_last_modified is not None and self.source_last_modified.utcoffset() is None:
            raise ValueError("LandedAsset.source_last_modified must be timezone-aware")


@dataclass(frozen=True)
class AcquisitionResult:
    """Structured result returned by source acquisition."""

    source_capture: SourceCapture
    landed_assets: tuple[LandedAsset, ...]


PipelineMatchMode = Literal["exact", "pattern"]

BackfillSourceLastModifiedMode = Literal["none", "file_mtime", "target_date"]


@dataclass(frozen=True)
class PipelineBackfillSpec:
    """Configuration for collecting existing landing files."""

    source_url: str = "manual_upload"
    match_mode: PipelineMatchMode = "exact"
    source_last_modified_from: BackfillSourceLastModifiedMode = "none"
    source_last_modified_overrides: dict[str, BackfillSourceLastModifiedMode] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class PipelineAcquisitionSpec:
    """Configuration for the scheduled extractor and backfill behavior."""

    extractor: str
    options: dict[str, Any] = field(default_factory=dict)
    backfill: PipelineBackfillSpec = field(default_factory=PipelineBackfillSpec)


@dataclass(frozen=True)
class PipelineStepSpec:
    """One ordered action in a configured pipeline."""

    action: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineSpec:
    """Validated acquisition and processing sequence."""

    acquisition: PipelineAcquisitionSpec
    steps: tuple[PipelineStepSpec, ...]


def parse_pipeline_spec(raw: dict[str, Any]) -> PipelineSpec:
    """Validate a raw pipeline mapping loaded from YAML."""

    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("Pipeline configuration must be a non-empty mapping")

    acquisition_raw = raw.get("acquisition")

    if not isinstance(acquisition_raw, dict):
        raise RuntimeError("pipeline.acquisition must be a mapping")

    extractor = acquisition_raw.get("extractor")

    if not isinstance(extractor, str) or not extractor.strip():
        raise RuntimeError("pipeline.acquisition.extractor must be a non-empty string")

    extractor_options = acquisition_raw.get("options", {})

    if not isinstance(extractor_options, dict):
        raise RuntimeError("pipeline.acquisition.options must be a mapping")

    backfill_raw = acquisition_raw.get("backfill", {})

    if not isinstance(backfill_raw, dict):
        raise RuntimeError("pipeline.acquisition.backfill must be a mapping")

    source_url = backfill_raw.get("source_url", "manual_upload")

    if not isinstance(source_url, str) or not source_url.strip():
        raise RuntimeError("pipeline.acquisition.backfill.source_url must be a non-empty string")

    match_mode = backfill_raw.get("match_mode", "exact")

    if match_mode not in {"exact", "pattern"}:
        raise RuntimeError("pipeline.acquisition.backfill.match_mode must be 'exact' or 'pattern'")

    source_last_modified_from = backfill_raw.get("source_last_modified_from", "none")

    if source_last_modified_from not in {"none", "file_mtime", "target_date"}:
        raise RuntimeError(
            "pipeline.acquisition.backfill."
            "source_last_modified_from must be "
            "'none', 'file_mtime', or 'target_date'"
        )

    overrides_raw = backfill_raw.get("source_last_modified_overrides", {})

    if not isinstance(overrides_raw, dict):
        raise RuntimeError(
            "pipeline.acquisition.backfill.source_last_modified_overrides must be a mapping"
        )

    source_last_modified_overrides: dict[str, BackfillSourceLastModifiedMode] = {}

    for override_date, override_mode in overrides_raw.items():
        if not isinstance(override_date, str):
            raise RuntimeError(
                "pipeline.acquisition.backfill."
                "source_last_modified_overrides keys must be quoted "
                "YYYY-MM-DD strings"
            )

        try:
            datetime.strptime(override_date, "%Y-%m-%d")
        except ValueError as exc:
            raise RuntimeError(
                "pipeline.acquisition.backfill."
                "source_last_modified_overrides contains an invalid "
                f"date: {override_date!r}"
            ) from exc

        if override_mode not in {"none", "file_mtime", "target_date"}:
            raise RuntimeError(
                "pipeline.acquisition.backfill."
                "source_last_modified_overrides values must be "
                "'none', 'file_mtime', or 'target_date'"
            )

        source_last_modified_overrides[override_date] = override_mode

    steps_raw = raw.get("steps")

    if not isinstance(steps_raw, list) or not steps_raw:
        raise RuntimeError("pipeline.steps must be a non-empty list")

    steps: list[PipelineStepSpec] = []

    for index, step_raw in enumerate(steps_raw):
        if not isinstance(step_raw, dict):
            raise RuntimeError(f"pipeline.steps[{index}] must be a mapping")

        action = step_raw.get("action")

        if not isinstance(action, str) or not action.strip():
            raise RuntimeError(f"pipeline.steps[{index}].action must be a non-empty string")

        options = step_raw.get("options", {})

        if not isinstance(options, dict):
            raise RuntimeError(f"pipeline.steps[{index}].options must be a mapping")

        steps.append(PipelineStepSpec(action=action.strip(), options=dict(options)))

    return PipelineSpec(
        acquisition=PipelineAcquisitionSpec(
            extractor=extractor.strip(),
            options=dict(extractor_options),
            backfill=PipelineBackfillSpec(
                source_url=source_url.strip(),
                match_mode=match_mode,
                source_last_modified_from=source_last_modified_from,
                source_last_modified_overrides=source_last_modified_overrides,
            ),
        ),
        steps=tuple(steps),
    )


@dataclass
class PipelineRunState:
    """Values produced and consumed during one pipeline execution."""

    source_capture: SourceCapture | None = None
    landed_assets: list[LandedAsset] = field(default_factory=list)
    bronze_batch: BronzeBatchResult | None = None
    action_results: dict[str, Any] = field(default_factory=dict)
    action_outcomes: list[ActionExecutionResult] = field(default_factory=list)
