"""Ratchet on the number of Protocol classes, so new indirection is a visible decision."""

from __future__ import annotations

import ast
from pathlib import Path

# Lower this when you remove a Protocol. Raise it only together with the reason in the pull
# request: a second implementation, the public API, or a hand-written test double.
PROTOCOL_LIMIT = 34

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src" / "metrka_core"


def _is_protocol_base(base: ast.expr) -> bool:
    target = base.value if isinstance(base, ast.Subscript) else base

    if isinstance(target, ast.Attribute):
        return target.attr == "Protocol"

    return isinstance(target, ast.Name) and target.id == "Protocol"


def _protocol_classes() -> list[str]:
    found: list[str] = []

    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        found.extend(
            f"{path.relative_to(SOURCE_ROOT).as_posix()}:{node.name}"
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef) and any(map(_is_protocol_base, node.bases))
        )

    return found


def test_protocol_count_does_not_change_silently() -> None:
    protocols = _protocol_classes()

    assert len(protocols) == PROTOCOL_LIMIT, (
        f"metrka-core has {len(protocols)} Protocol classes; the limit is {PROTOCOL_LIMIT}. "
        "If you removed one, lower PROTOCOL_LIMIT. If you added one, prefer a concrete class "
        "and raise the limit only with the reason in the pull request."
    )
