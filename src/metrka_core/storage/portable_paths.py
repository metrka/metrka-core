"""Canonical cross-platform relative-path validation."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Final

_INVALID_PORTABLE_COMPONENT: Final = re.compile(r'[<>:"|?*\x00-\x1f]')
_WINDOWS_RESERVED_NAMES: Final = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{number}" for number in range(1, 10)),
        *(f"LPT{number}" for number in range(1, 10)),
    }
)


def validate_portable_relative_path(path: str) -> None:
    """Require one canonical relative path safe on Windows and POSIX."""

    if not isinstance(path, str) or not path:
        raise ValueError("Portable relative path must not be empty")
    if "\\" in path:
        raise ValueError("Portable relative paths must use POSIX separators")

    pure_path = PurePosixPath(path)

    if pure_path.is_absolute() or pure_path.as_posix() != path:
        raise ValueError(f"Portable relative path is not canonical: {path!r}")
    if not pure_path.parts or any(part in {"", ".", ".."} for part in pure_path.parts):
        raise ValueError(f"Portable relative path is unsafe: {path!r}")

    for component in pure_path.parts:
        if _INVALID_PORTABLE_COMPONENT.search(component) is not None:
            raise ValueError(f"Portable relative path is not cross-platform safe: {path!r}")
        if component.endswith((" ", ".")):
            raise ValueError(f"Portable relative path is not cross-platform safe: {path!r}")
        if component.split(".", maxsplit=1)[0].upper() in _WINDOWS_RESERVED_NAMES:
            raise ValueError(f"Portable relative path uses a reserved filename: {path!r}")
