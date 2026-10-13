"""Typed definitions for configurable pipeline actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar

from metrka_core.pipeline.action_runtime import ActionRuntime

if TYPE_CHECKING:
    from metrka_core.pipeline.context import PipelineContext
    from metrka_core.pipeline.models import PipelineRunState


DependenciesTCo = TypeVar("DependenciesTCo", covariant=True)
DependenciesTContra = TypeVar("DependenciesTContra", contravariant=True)
OptionsTContra = TypeVar("OptionsTContra", contravariant=True)

type JsonScalar = str | int | float | bool | None

type ActionStatus = Literal["completed", "skipped"]


@dataclass(frozen=True)
class ArtifactRef:
    """Stable identity of one artifact produced by an action."""

    kind: str
    identifier: str
    dataset_id: str | None = None

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("ArtifactRef.kind must not be empty")

        if not self.identifier.strip():
            raise ValueError("ArtifactRef.identifier must not be empty")


@dataclass(frozen=True)
class ActionOutcome:
    """Common result returned by every successful action."""

    status: ActionStatus
    message: str | None = None
    produced_artifacts: tuple[ArtifactRef, ...] = ()
    metrics: Mapping[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in {"completed", "skipped"}:
            raise ValueError(f"Unsupported action status: {self.status!r}")

        normalized_metrics = dict(self.metrics)

        for key, value in normalized_metrics.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError("ActionOutcome metric names must be non-empty strings")

            if not isinstance(value, (str, int, float, bool, type(None))):
                raise TypeError("ActionOutcome metrics must contain JSON scalar values")

        object.__setattr__(self, "metrics", MappingProxyType(normalized_metrics))


@dataclass(frozen=True)
class ActionExecutionResult:
    """Outcome associated with one registered action key."""

    action_key: str
    outcome: ActionOutcome


class ActionDependencyResolver(Protocol[DependenciesTCo]):
    """Project the pipeline context into narrow action dependencies."""

    def __call__(self, context: PipelineContext, /) -> DependenciesTCo: ...


class ActionHandler(Protocol[DependenciesTContra, OptionsTContra]):
    """Execute one action using only its declared dependencies."""

    def __call__(
        self,
        *,
        runtime: ActionRuntime,
        deps: DependenciesTContra,
        state: PipelineRunState,
        options: OptionsTContra,
    ) -> ActionOutcome: ...


@dataclass(frozen=True)
class ResolvedAction:
    """One action with validated options and dependency binding."""

    key: str
    resolve_dependencies: ActionDependencyResolver[Any]
    handler: ActionHandler[Any, Any]
    options: Any

    def execute(self, *, context: PipelineContext, state: PipelineRunState) -> ActionOutcome:
        dependencies = self.resolve_dependencies(context)

        return self.handler(
            runtime=context.as_action_runtime(),
            deps=dependencies,
            state=state,
            options=self.options,
        )


@dataclass(frozen=True)
class ActionDefinition[OptionsT, DependenciesT]:
    """Registration contract for one configurable action."""

    key: str
    parse_options: Callable[[Mapping[str, Any]], OptionsT]
    resolve_dependencies: ActionDependencyResolver[DependenciesT]
    handler: ActionHandler[DependenciesT, OptionsT]

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not self.key.strip():
            raise ValueError("ActionDefinition.key must be a non-empty string")

        if self.key != self.key.strip():
            raise ValueError("ActionDefinition.key must not contain surrounding whitespace")

    def resolve(self, raw_options: Mapping[str, Any]) -> ResolvedAction:
        """Validate raw options and bind them to this action."""

        try:
            parsed_options = self.parse_options(raw_options)
        except (TypeError, ValueError, RuntimeError) as exc:
            raise ValueError(f"Invalid options for pipeline action {self.key!r}: {exc}") from exc

        return ResolvedAction(
            key=self.key,
            resolve_dependencies=self.resolve_dependencies,
            handler=self.handler,
            options=parsed_options,
        )
