"""Registry of configured pipeline extractors and actions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from metrka_core.pipeline.acquisition.contracts import AssetExtractor
from metrka_core.pipeline.action_models import ActionDefinition, ResolvedAction

ExtractorHandler = AssetExtractor


class PipelineRegistry:
    """Map YAML identifiers to typed component definitions."""

    def __init__(self) -> None:
        self._extractors: dict[str, ExtractorHandler] = {}
        self._actions: dict[str, ActionDefinition[Any, Any]] = {}

    def register_extractor(self, name: str, handler: ExtractorHandler) -> None:
        normalized_name = self._validate_name(name)

        if normalized_name in self._extractors:
            raise ValueError(f"Extractor is already registered: {normalized_name}")

        self._extractors[normalized_name] = handler

    def register_action(self, definition: ActionDefinition[Any, Any]) -> None:
        normalized_name = self._validate_name(definition.key)

        if normalized_name in self._actions:
            raise ValueError(f"Action is already registered: {normalized_name}")

        self._actions[normalized_name] = definition

    def get_extractor(self, name: str) -> ExtractorHandler:
        normalized_name = self._validate_name(name)

        try:
            return self._extractors[normalized_name]
        except KeyError as exc:
            available = ", ".join(sorted(self._extractors)) or "none"

            raise KeyError(
                f"Unknown pipeline extractor {normalized_name!r}. Available extractors: {available}"
            ) from exc

    def get_action(self, name: str) -> ActionDefinition[Any, Any]:
        normalized_name = self._validate_name(name)

        try:
            return self._actions[normalized_name]
        except KeyError as exc:
            available = ", ".join(sorted(self._actions)) or "none"

            raise KeyError(
                f"Unknown pipeline action {normalized_name!r}. Available actions: {available}"
            ) from exc

    def resolve_action(self, name: str, raw_options: Mapping[str, Any]) -> ResolvedAction:
        definition = self.get_action(name)

        return definition.resolve(raw_options)

    @staticmethod
    def _validate_name(name: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Pipeline registry name must be a non-empty string")

        return name.strip()
