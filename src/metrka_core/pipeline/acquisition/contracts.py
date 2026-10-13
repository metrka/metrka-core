"""Public callable contracts for source acquisition."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
    from metrka_core.pipeline.acquisition.models import SourceCapture
    from metrka_core.pipeline.action_runtime import ActionRuntime
    from metrka_core.pipeline.models import LandedAsset


class AssetExtractor(Protocol):
    """Acquire source assets using explicit runtime dependencies."""

    def __call__(
        self,
        runtime: ActionRuntime,
        deps: AcquisitionDeps,
        capture: SourceCapture,
        options: dict[str, Any],
        /,
    ) -> list[LandedAsset]: ...
