"""Hash a JSON file for duplicate detection, leaving out values that change on their own.

A source's description of a file can carry counters, such as views and downloads, that change
every day although nothing about the file did. Bronze keeps the file exactly as received; only
the hash it compares with earlier files sets the named keys to null first, so a new download
whose only difference is such a counter is recognised as the file already kept.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_json_ignoring(path: Path, ignored_keys: tuple[str, ...]) -> str:
    """Return the SHA-256 of a JSON document with ``ignored_keys`` set to null.

    A key is a dotted path from the top of the document, such as ``viewCount`` or
    ``owner.displayName``; a path that the document lacks changes nothing. The document is
    written with sorted keys and no spaces before hashing, so layout changes do not count.
    """

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"{path.name} is not a JSON document, so its ignored keys cannot be applied: {error}"
        ) from error

    for key in ignored_keys:
        _set_null(document, key.split("."))

    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _set_null(node: Any, parts: list[str]) -> None:
    for part in parts[:-1]:
        if not isinstance(node, dict) or part not in node:
            return
        node = node[part]

    if isinstance(node, dict) and parts[-1] in node:
        node[parts[-1]] = None
