"""Shared structures and descriptions that contracts reuse by name.

A source often publishes many tables of one shape, described in the same words. Profiles
in ``profiles/<name>.yaml`` hold what repeats, once.

A table profile gives a table's structure, and the methodology that structure brings::

    version: 1
    renamable: [Count, Rate]
    retypable: [Denom]
    table:
      columns: {...}
      canonical_order: [...]
      parent_child_reconciliation: {...}
    methodology:
      limitations: [...]

A contract table names it with ``uses: <name>``. The contract may then change the words:
``display_name``, ``info`` and each column's ``meta``. It may also give a new published
name (``rename_to``) to a column the profile lists as ``renamable``, so a count can read
``licensed_bed_count`` in one dataset and ``children_tested_count`` in another while
shared keys such as the county keep one name everywhere. A new name replaces the old one
wherever the profile refers to it. A column the profile lists as ``retypable`` may also
take its own type (``cast_to``), for a source that gives one indicator's values in a finer
type than the rest, such as a population it does not round. Everything else comes from
the profile, so every table of one profile is built the same way, except in the types its
profile marks retypable; a different shape is a different profile.

A meta profile gives what every dataset of a source says about itself::

    version: 1
    meta:
      category: ...
      source: {publisher: ..., update_frequency: ...}
      methodology: {timing: ...}

and a contract names it with ``meta: {uses: <name>, ...}``. The dataset's ``meta`` is the
meta profile, then the methodology of its table profiles, then the contract's own
``meta``. Mappings merge key by key and the later value wins; limitations add up in that
order, each sentence once; any other list is replaced. The contract's own keys keep the
order it wrote them in, and what only a profile adds follows, so a dataset reads in the
order its author chose.

Profiles are looked up from the contract upward: first beside ``conf`` (a dataset folder
or a flat workspace), then one level higher (the source folder above dataset folders).
The expanded contract is what every reader sees and what a published snapshot keeps.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROFILE_DIRECTORY = "profiles"

_PROFILE_NAME = re.compile(r"[a-z][a-z0-9_]*")
_PROFILE_FIELDS = frozenset({"version", "renamable", "retypable", "table", "methodology", "meta"})
# What a contract table may set beside ``uses``; the structure is the profile's.
_TABLE_WORDS = frozenset({"uses", "display_name", "info", "columns"})
# The one list that collects sentences from every profile instead of being replaced.
_COLLECTED_LIST = ("methodology", "limitations")


class ProfileError(ValueError):
    """Raised when a contract cannot be expanded with its profiles."""


@dataclass(frozen=True)
class _Profile:
    name: str
    path: Path
    table: dict[str, Any] | None
    methodology: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] | None = None
    # Source columns whose published name a contract may choose.
    renamable: frozenset[str] = frozenset()
    # Source columns whose type a contract may choose.
    retypable: frozenset[str] = frozenset()


def uses_profiles(contract: Mapping[str, Any]) -> bool:
    """Whether the contract's meta or any of its tables names a profile."""

    meta = contract.get("meta")
    if isinstance(meta, Mapping) and "uses" in meta:
        return True

    tables = contract.get("tables")
    return isinstance(tables, Mapping) and any(
        isinstance(table, Mapping) and "uses" in table for table in tables.values()
    )


def expand_profiles(contract: dict[str, Any], *, contract_path: Path) -> dict[str, Any]:
    """Return the contract with its profiles applied to its tables and its meta.

    A contract without ``uses`` is returned unchanged. Every problem is reported at once.
    """

    if not uses_profiles(contract):
        return contract

    expanded = copy.deepcopy(contract)
    problems: list[str] = []
    profiles = _ProfileCache(contract_path=contract_path, problems=problems)
    table_methodologies: list[dict[str, Any]] = []

    tables = expanded.get("tables")
    if not isinstance(tables, dict):
        tables = {}
    for table_key, table in tables.items():
        if not isinstance(table, Mapping) or "uses" not in table:
            continue

        where = f"{contract_path.name} tables.{table_key}"
        profile = profiles.get(table["uses"], where=where)
        if profile is None:
            continue
        if profile.table is None:
            problems.append(f"{where}: profile {profile.name!r} has no 'table'")
            continue

        merged = _apply_contract(profile, table, where=where, problems=problems)
        if merged is not None:
            tables[table_key] = {"profile": profile.name, **merged}
            if profile.methodology:
                table_methodologies.append(profile.methodology)

    meta = expanded.get("meta")
    meta = dict(meta) if isinstance(meta, Mapping) else {}
    meta_profile: _Profile | None = None
    if "uses" in meta:
        where = f"{contract_path.name} meta"
        meta_profile = profiles.get(meta.pop("uses"), where=where)
        if meta_profile is not None and meta_profile.meta is None:
            problems.append(f"{where}: profile {meta_profile.name!r} has no 'meta'")
            meta_profile = None

    if problems:
        raise ProfileError("Profile errors:\n- " + "\n- ".join(problems))

    if meta_profile is not None or table_methodologies:
        combined: dict[str, Any] = copy.deepcopy(meta_profile.meta or {}) if meta_profile else {}
        for methodology in table_methodologies:
            combined = _merge(combined, {"methodology": methodology})
        combined = _merge(combined, meta)
        if meta_profile is not None:
            combined = {"profile": meta_profile.name, **combined}
        expanded["meta"] = combined

    return expanded


def load_contract(path: Path) -> dict[str, Any]:
    """Read a contract file and expand its profiles."""

    contract = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(contract, dict):
        raise ProfileError(f"{path}: top-level YAML must be a mapping")

    return expand_profiles(contract, contract_path=path)


def profile_search_paths(name: str, *, contract_path: Path) -> tuple[Path, ...]:
    """Where a contract looks for one profile, nearest first."""

    conf = contract_path.resolve().parent
    return (
        conf.parent / PROFILE_DIRECTORY / f"{name}.yaml",
        conf.parent.parent / PROFILE_DIRECTORY / f"{name}.yaml",
    )


@dataclass
class _ProfileCache:
    """Each profile is read once per contract; a failure is reported where it was named."""

    contract_path: Path
    problems: list[str]
    loaded: dict[str, _Profile | None] = field(default_factory=dict)

    def get(self, name: object, *, where: str) -> _Profile | None:
        if not isinstance(name, str) or _PROFILE_NAME.fullmatch(name) is None:
            self.problems.append(f"{where}: uses {name!r} must name a profile such as county_count")
            return None

        if name not in self.loaded:
            try:
                self.loaded[name] = _load_profile(name, contract_path=self.contract_path)
            except ProfileError as error:
                self.problems.append(f"{where}: {error}")
                self.loaded[name] = None

        return self.loaded[name]


def _load_profile(name: str, *, contract_path: Path) -> _Profile:
    candidates = profile_search_paths(name, contract_path=contract_path)
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        looked = ", ".join(str(candidate) for candidate in candidates)
        raise ProfileError(f"profile {name!r} was not found; looked in {looked}")

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise ProfileError(f"{path}: YAML parse error: {error}") from error

    if not isinstance(document, Mapping) or document.get("version") != 1:
        raise ProfileError(
            f"{path}: a profile must be a mapping with 'version: 1' and 'table' or 'meta'"
        )

    unexpected = sorted(set(document) - _PROFILE_FIELDS)
    if unexpected:
        raise ProfileError(f"{path}: unsupported profile fields {unexpected}")

    table = document.get("table")
    meta = document.get("meta")
    methodology = document.get("methodology", {})

    if table is None and meta is None:
        raise ProfileError(f"{path}: a profile needs 'table', 'meta' or both")

    if table is not None and (
        not isinstance(table, Mapping) or not isinstance(table.get("columns"), Mapping)
    ):
        raise ProfileError(f"{path}: 'table' must be a mapping with 'columns'")

    if meta is not None and not isinstance(meta, Mapping):
        raise ProfileError(f"{path}: 'meta' must be a mapping")

    if not isinstance(methodology, Mapping):
        raise ProfileError(f"{path}: 'methodology' must be a mapping")

    if (isinstance(table, Mapping) and "uses" in table) or (
        isinstance(meta, Mapping) and "uses" in meta
    ):
        raise ProfileError(f"{path}: a profile cannot use another profile")

    columns = table["columns"] if isinstance(table, Mapping) else {}
    freed: dict[str, list[str]] = {}
    for word in ("renamable", "retypable"):
        listed = document.get(word, [])
        if not isinstance(listed, list) or not all(isinstance(item, str) for item in listed):
            raise ProfileError(f"{path}: '{word}' must list source columns")
        unknown = sorted(set(listed) - set(columns))
        if unknown:
            raise ProfileError(f"{path}: {word} columns {unknown} are not in the table")
        freed[word] = listed

    return _Profile(
        name=name,
        path=path,
        table=dict(table) if isinstance(table, Mapping) else None,
        methodology=dict(methodology),
        meta=dict(meta) if isinstance(meta, Mapping) else None,
        renamable=frozenset(freed["renamable"]),
        retypable=frozenset(freed["retypable"]),
    )


def _merge(base: Any, over: Any, path: tuple[str, ...] = ()) -> Any:
    """Lay ``over`` on ``base``: mappings key by key, limitations added up, the rest replaced.

    A merged mapping lists ``over``'s keys first, in its order, then those only ``base`` has.
    """

    if isinstance(base, Mapping) and isinstance(over, Mapping):
        merged = {
            key: _merge(base[key], value, (*path, key)) if key in base else value
            for key, value in over.items()
        }
        merged.update((key, value) for key, value in base.items() if key not in over)
        return copy.deepcopy(merged)

    if path == _COLLECTED_LIST and isinstance(base, list) and isinstance(over, list):
        collected: list[Any] = []
        for sentence in (*base, *over):
            if sentence not in collected:
                collected.append(sentence)
        return collected

    return copy.deepcopy(over)


def _apply_contract(
    profile: _Profile, table: Mapping[str, Any], *, where: str, problems: list[str]
) -> dict[str, Any] | None:
    """Lay a contract table's words, new names and new types over its profile; refuse the rest."""

    if profile.table is None:
        raise ProfileError(f"{where}: profile {profile.name!r} has no 'table'")
    count_before = len(problems)

    structural = sorted(set(table) - _TABLE_WORDS)
    if structural:
        problems.append(
            f"{where}: {structural} come from profile {profile.name!r}; "
            "change the profile or use another one"
        )

    merged = copy.deepcopy(profile.table)
    for word in ("display_name", "info"):
        if word in table:
            merged[word] = table[word]

    columns = table.get("columns", {})
    if not isinstance(columns, Mapping):
        problems.append(f"{where}: columns must map source columns to their meta")
        columns = {}

    renamed: dict[str, str] = {}
    profile_columns = merged["columns"]
    for source_column, column in columns.items():
        if source_column not in profile_columns:
            problems.append(f"{where}: column {source_column!r} is not in profile {profile.name!r}")
            continue

        allowed = {"meta"}
        if source_column in profile.renamable:
            allowed.add("rename_to")
        if source_column in profile.retypable:
            allowed.add("cast_to")
        if not isinstance(column, Mapping) or set(column) - allowed:
            problems.append(
                f"{where}: column {source_column!r} may set only {sorted(allowed)}; "
                f"the rest comes from profile {profile.name!r}"
            )
            continue

        meta = column.get("meta", {})
        if not isinstance(meta, Mapping):
            problems.append(f"{where}: column {source_column!r} meta must be a mapping")
            continue

        profile_column = dict(profile_columns[source_column])
        profile_column["meta"] = {**dict(profile_column.get("meta") or {}), **dict(meta)}

        new_name = column.get("rename_to")
        if new_name is not None:
            if not isinstance(new_name, str) or not new_name.strip():
                problems.append(f"{where}: column {source_column!r} rename_to must be a name")
                continue
            renamed[str(profile_column["rename_to"])] = new_name
            profile_column["rename_to"] = new_name

        new_type = column.get("cast_to")
        if new_type is not None:
            if not isinstance(new_type, str) or not new_type.strip():
                problems.append(f"{where}: column {source_column!r} cast_to must be a type")
                continue
            profile_column["cast_to"] = new_type

        profile_columns[source_column] = profile_column

    if len(problems) != count_before:
        return None

    return _rename_references(merged, renamed)


def _rename_references(table: dict[str, Any], renamed: Mapping[str, str]) -> dict[str, Any]:
    """Carry new published names into every part of the table that names a column."""

    if not renamed:
        return table

    def name(value: Any) -> Any:
        return renamed.get(value, value) if isinstance(value, str) else value

    if isinstance(table.get("canonical_order"), list):
        table["canonical_order"] = [name(column) for column in table["canonical_order"]]

    reconciliation = table.get("parent_child_reconciliation")
    if isinstance(reconciliation, dict):
        if isinstance(reconciliation.get("group_by"), list):
            reconciliation["group_by"] = [name(column) for column in reconciliation["group_by"]]
        reconciliation["measure_column"] = name(reconciliation.get("measure_column"))
        for part, key in (("parent", "column"), ("residual", "label_column")):
            if isinstance(reconciliation.get(part), dict):
                reconciliation[part][key] = name(reconciliation[part].get(key))

    return table
