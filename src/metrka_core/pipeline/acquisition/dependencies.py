"""Dependencies and handler contract for data acquisition."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.datasets.source_config import SourceConfig
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.storage.landing_store import LandingStore


@dataclass(frozen=True)
class AcquisitionDeps:
    """Infrastructure required by acquisition extractors."""

    source_config: SourceConfig
    landing_store: LandingStore
    execution_log_store: ExecutionLogStore
