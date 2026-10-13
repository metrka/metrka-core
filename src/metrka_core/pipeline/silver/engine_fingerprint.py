"""Calculate deterministic fingerprints of Silver processing code."""

from __future__ import annotations

import ast
import hashlib
import json
import platform
from importlib import metadata
from pathlib import Path
from typing import Any

from metrka_core.pipeline.silver.engine_models import SilverEngineIdentity

SILVER_ENGINE_FINGERPRINT_VERSION = 1
SILVER_RUNTIME_FINGERPRINT_VERSION = 1

_ENGINE_SOURCE_PATHS = (
    "transform",
    "pipeline/silver/silver_builder.py",
    "pipeline/silver/version_period.py",
    "storage/save.py",
)

_RUNTIME_DISTRIBUTIONS = ("pandas", "numpy", "pyarrow", "openpyxl", "PyYAML", "lxml")


def _sha256_json(value: Any) -> str:
    canonical_json = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _remove_docstrings(tree: ast.AST) -> None:
    docstring_owners = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    for node in ast.walk(tree):
        if not isinstance(node, docstring_owners):
            continue

        if not node.body:
            continue

        first_statement = node.body[0]

        if (
            isinstance(first_statement, ast.Expr)
            and isinstance(first_statement.value, ast.Constant)
            and isinstance(first_statement.value.value, str)
        ):
            node.body = node.body[1:]


def _normalized_ast_hash(file_path: Path) -> str:
    source = file_path.read_text(encoding="utf-8")

    tree = ast.parse(source, filename=str(file_path))

    _remove_docstrings(tree)

    normalized = ast.dump(tree, annotate_fields=True, include_attributes=False)

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _engine_source_files(package_root: Path) -> list[Path]:
    discovered: set[Path] = set()

    for relative_name in _ENGINE_SOURCE_PATHS:
        target = package_root / relative_name

        if target.is_dir():
            discovered.update(path for path in target.rglob("*.py") if path.is_file())

        elif target.is_file():
            discovered.add(target)

        else:
            raise RuntimeError(f"Silver engine fingerprint source does not exist: {target}")

    return sorted(discovered)


def _runtime_versions() -> dict[str, str]:
    versions = {
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
    }

    for distribution_name in _RUNTIME_DISTRIBUTIONS:
        try:
            distribution_version = metadata.version(distribution_name)
        except metadata.PackageNotFoundError:
            distribution_version = "not-installed"

        versions[distribution_name] = distribution_version

    return versions


def calculate_silver_engine_identity() -> SilverEngineIdentity:
    """Return the identity of the installed Silver engine."""

    package_root = Path(__file__).resolve().parents[2]

    component_hashes = {
        file_path.relative_to(package_root).as_posix(): _normalized_ast_hash(file_path)
        for file_path in _engine_source_files(package_root)
    }

    engine_hash = _sha256_json(
        {
            "algorithm": "metrka-silver-normalized-ast-v1",
            "version": SILVER_ENGINE_FINGERPRINT_VERSION,
            "components": component_hashes,
        }
    )

    runtime_versions = _runtime_versions()

    runtime_hash = _sha256_json(
        {
            "algorithm": "metrka-silver-runtime-v1",
            "version": SILVER_RUNTIME_FINGERPRINT_VERSION,
            "runtime": runtime_versions,
        }
    )

    release_hash = _sha256_json(
        {
            "engine_hash": engine_hash,
            "engine_fingerprint_version": SILVER_ENGINE_FINGERPRINT_VERSION,
            "runtime_hash": runtime_hash,
            "runtime_fingerprint_version": SILVER_RUNTIME_FINGERPRINT_VERSION,
        }
    )

    return SilverEngineIdentity(
        release_hash=release_hash,
        engine_hash=engine_hash,
        engine_fingerprint_version=SILVER_ENGINE_FINGERPRINT_VERSION,
        runtime_hash=runtime_hash,
        runtime_fingerprint_version=SILVER_RUNTIME_FINGERPRINT_VERSION,
        component_hashes=component_hashes,
        runtime_versions=runtime_versions,
    )
