"""Path formatting helpers used across metrka-core."""

from __future__ import annotations

from pathlib import Path


def to_rel_posix(path: str | Path, *, base: str | Path) -> str:
    """Return POSIX relative  path under base. Raise if outside base."""
    p = Path(path).expanduser().resolve()
    b = Path(base).expanduser().resolve()
    try:
        return p.relative_to(b).as_posix()
    except Exception as e:
        raise ValueError("path is outside base dir") from e
