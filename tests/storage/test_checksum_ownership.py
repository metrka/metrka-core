"""Architectural guard for centralized physical-file hashing."""

from __future__ import annotations

import ast
from pathlib import Path


def test_physical_file_sha256_has_one_implementation() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src" / "metrka_core"
    private_helpers: list[Path] = []
    file_digest_owners: list[Path] = []

    for source_path in source_root.rglob("*.py"):
        relative_path = source_path.relative_to(source_root)
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))

        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "_sha256_file"
            ):
                private_helpers.append(relative_path)

            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "hashlib"
                and node.func.attr == "file_digest"
            ):
                file_digest_owners.append(relative_path)

    assert private_helpers == []
    assert file_digest_owners == [Path("storage/checksums.py")]
