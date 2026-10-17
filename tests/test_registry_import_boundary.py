"""Tests for the lightweight pipeline registry boundary."""

from __future__ import annotations

import json
import subprocess
import sys

from metrka_core.pipeline.default_registry import create_core_registry


def test_registry_import_does_not_load_execution_layers() -> None:
    probe = """
import json
import sys

import metrka_core.pipeline.registry

blocked_prefixes = (
    "numpy",
    "pandas",
    "pyarrow",
    "metrka_core.pipeline.actions.bronze",
    "metrka_core.pipeline.actions.documentation",
    "metrka_core.pipeline.actions.silver",
    "metrka_core.pipeline.bronze",
    "metrka_core.pipeline.silver",
)

loaded = sorted(
    module_name
    for module_name in sys.modules
    if any(
        module_name == prefix
        or module_name.startswith(f"{prefix}.")
        for prefix in blocked_prefixes
    )
)

print(json.dumps(loaded))
"""

    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe], check=True, capture_output=True, text=True
    )

    loaded_modules = json.loads(completed.stdout)

    assert loaded_modules == []


def test_default_registry_contains_core_components() -> None:
    registry = create_core_registry()

    extractor = registry.get_extractor("http.files")

    assert callable(extractor)

    assert registry.get_action("bronze.ingest").key == "bronze.ingest"
    assert registry.get_action("documentation.bind").key == "documentation.bind"
    assert registry.get_action("silver.process").key == "silver.process"
