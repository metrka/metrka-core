"""Public contracts for custom source extractors."""

from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.pipeline.acquisition.contracts import AssetExtractor
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.acquisition.models import SourceCapture
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import LandedAsset
from metrka_core.pipeline.runtime_services import Clock, MonotonicClock

__all__ = [
    "AcquisitionDeps",
    "ActionRuntime",
    "AssetExtractor",
    "LandedAsset",
    "SourceCapture",
    "run_step",
    "Clock",
    "ExecutionStepMeta",
    "MonotonicClock",
]
