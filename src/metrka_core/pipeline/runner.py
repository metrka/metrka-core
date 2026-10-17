"""Execution engine for YAML-configured pipelines."""

from __future__ import annotations

import logging
from typing import Any

from metrka_core.pipeline.acquisition.capture import acquire_and_register_capture
from metrka_core.pipeline.action_models import ActionExecutionResult
from metrka_core.pipeline.context import PipelineContext
from metrka_core.pipeline.models import PipelineRunState, parse_pipeline_spec
from metrka_core.pipeline.registry import PipelineRegistry

logger = logging.getLogger(__name__)


def execute_configured_pipeline(
    *,
    context: PipelineContext,
    registry: PipelineRegistry,
    target_date: str | None = None,
    source_capture_id: str | None = None,
    action_option_overrides: (dict[str, dict[str, Any]] | None) = None,
    run_without_landed_assets: bool = False,
) -> PipelineRunState:
    """Acquire assets and execute configured actions in YAML order."""
    if source_capture_id is not None and target_date is None:
        raise ValueError("source_capture_id requires target_date")

    runtime_overrides = action_option_overrides or {}

    if not isinstance(runtime_overrides, dict):
        raise TypeError("action_option_overrides must be a mapping")

    for action_name, action_options in runtime_overrides.items():
        if not isinstance(action_name, str) or not action_name.strip():
            raise ValueError("Runtime action override names must be non-empty strings")

        if not isinstance(action_options, dict):
            raise TypeError(f"Runtime options for action {action_name!r} must be a mapping")

    pipeline_spec = parse_pipeline_spec(context.workspace.source_config.pipeline)

    extractor = registry.get_extractor(pipeline_spec.acquisition.extractor)

    configured_action_names = {step.action for step in pipeline_spec.steps}

    unknown_override_actions = set(runtime_overrides) - configured_action_names

    if unknown_override_actions:
        raise RuntimeError(
            "Runtime options were provided for actions "
            "not configured in this pipeline: "
            f"{sorted(unknown_override_actions)}"
        )

    resolved_steps = []

    for step in pipeline_spec.steps:
        raw_options = dict(step.options)

        runtime_action_options = runtime_overrides.get(step.action, {})

        if runtime_action_options:
            raw_options.update(runtime_action_options)

            logger.info(
                "Applied runtime options to pipeline action %s: %s",
                step.action,
                sorted(runtime_action_options),
            )

        resolved_steps.append(registry.resolve_action(step.action, raw_options))

    backfill_spec = pipeline_spec.acquisition.backfill

    backfill_source_last_modified_from = backfill_spec.source_last_modified_from

    if target_date is not None:
        override_mode = backfill_spec.source_last_modified_overrides.get(target_date)

        if override_mode is not None:
            logger.info(
                "Using source_last_modified override for %s: %s", target_date, override_mode
            )

            backfill_source_last_modified_from = override_mode

    acquisition_result = acquire_and_register_capture(
        runtime=context.as_action_runtime(),
        deps=context.acquisition,
        source_captures=context.metadata.source_captures,
        target_date=target_date,
        target_source_capture_id=source_capture_id,
        scheduled_extractor=extractor,
        extractor_options=dict(pipeline_spec.acquisition.options),
        backfill_source_url=pipeline_spec.acquisition.backfill.source_url,
        backfill_match_mode=pipeline_spec.acquisition.backfill.match_mode,
        backfill_source_last_modified_from=backfill_source_last_modified_from,
    )

    landed_assets = list(acquisition_result.landed_assets)

    state = PipelineRunState(
        source_capture=acquisition_result.source_capture, landed_assets=landed_assets
    )

    if not landed_assets and not run_without_landed_assets:
        logger.info(
            "Pipeline acquisition produced no landed assets. No configured actions will run."
        )
        return state

    if not landed_assets:
        logger.info(
            "Pipeline acquisition produced no landed assets, "
            "but configured actions will run because a runtime "
            "rebuild was requested."
        )

    for resolved_action in resolved_steps:
        logger.info("Running configured pipeline action: %s", resolved_action.key)

        outcome = resolved_action.execute(context=context, state=state)

        state.action_outcomes.append(
            ActionExecutionResult(action_key=resolved_action.key, outcome=outcome)
        )

        logger.info(
            "Pipeline action %s completed with status=%s", resolved_action.key, outcome.status
        )

    return state
