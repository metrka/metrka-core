"""Configuration models for one Silver processing task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from metrka_core.catalog.temporal_coverage import TemporalCoverageSpec
from metrka_core.pipeline.silver.version_period import VersionPeriodDiscovery


@dataclass
class SilverTaskConfig:
    """Resolved Silver configuration for one dataset stream."""

    dataset_id: str
    yaml_contract_name: str
    partition_key: str
    version_period_discovery_func: VersionPeriodDiscovery
    processing_config_hash: str

    input_format: str = "csv"
    input_kwargs: dict[str, Any] = field(default_factory=dict)
    output_formats: list[str] = field(default_factory=lambda: ["parquet"])
    catalog_highlights: list[dict[str, Any]] = field(default_factory=list)
    temporal_coverage: TemporalCoverageSpec | None = None

    def __post_init__(self) -> None:
        if not self.partition_key.strip():
            raise ValueError("SilverTaskConfig.partition_key must not be empty")
