"""YAML loading for configuration files written by hand."""

from __future__ import annotations

from typing import IO, Any

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode

_MERGE_TAG = "tag:yaml.org,2002:merge"


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe loader that refuses a mapping which repeats a key.

    PyYAML keeps the last value of a repeated key, so a block written twice by
    mistake silently replaces the first one.
    """

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen: set[Any] = set()
        for key_node, _ in node.value:
            if key_node.tag == _MERGE_TAG:
                continue
            key = self.construct_object(key_node, deep=deep)
            try:
                repeated = key in seen
            except TypeError:
                continue
            if repeated:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def safe_load(stream: str | bytes | IO[str] | IO[bytes]) -> Any:
    """Parse one YAML document like ``yaml.safe_load``, refusing repeated mapping keys.

    A repeated key raises ``yaml.constructor.ConstructorError``, a ``yaml.YAMLError``.
    """

    return yaml.load(stream, Loader=_UniqueKeyLoader)  # noqa: S506
