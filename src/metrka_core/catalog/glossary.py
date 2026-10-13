"""Shared definitions of the words that dataset descriptions use.

A contract marks a defined word in any of its texts as ``[[key]]``, or ``[[key|shown text]]``
when the sentence needs another form, such as ``[[adult|adults]]``. A definition lives in
exactly one place:

* Metrka's own terms, for rows the platform itself creates, such as ``unallocated``;
* the workspace's ``glossary.yaml``, for words of one source shared by its datasets;
* the contract's ``meta.glossary``, for words of one dataset.

A published contract snapshot carries the definitions its texts use, so a later change to a
glossary changes new versions only and never the meaning of one already published.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from metrka_core.transform.profiles import expand_profiles, uses_profiles

GLOSSARY_FILE_NAME = "glossary.yaml"

# The top-level contract key under which a published snapshot records its definitions.
PUBLISHED_GLOSSARY_KEY = "glossary"

_TERM_KEY = re.compile(r"[a-z][a-z0-9_]*")
_TERM_REFERENCE = re.compile(r"\[\[([^\[\]]*)\]\]")
_TERM_FIELDS = {"label", "definition"}


class TermScope(StrEnum):
    """Where one definition is kept."""

    METRKA = "metrka"
    SOURCE = "source"
    DATASET = "dataset"


@dataclass(frozen=True, slots=True)
class GlossaryTerm:
    """One defined word."""

    key: str
    label: str
    definition: str
    scope: TermScope

    def published(self) -> dict[str, str]:
        return {"label": self.label, "definition": self.definition, "scope": str(self.scope)}


METRKA_TERMS: Mapping[str, GlossaryTerm] = {
    "unallocated": GlossaryTerm(
        key="unallocated",
        label="Unallocated",
        definition=(
            "A row added by Metrka: the statewide total minus the sum of the counties. "
            "It is not a county."
        ),
        scope=TermScope.METRKA,
    )
}


@dataclass(frozen=True, slots=True)
class Glossary:
    """The definitions every contract of one workspace can use: Metrka's and its source's."""

    terms: Mapping[str, GlossaryTerm] = field(default_factory=lambda: dict(METRKA_TERMS))

    def terms_used_by(self, contract: Mapping[str, Any], *, where: str) -> dict[str, GlossaryTerm]:
        """Return the definitions a contract uses, its own included, sorted by key.

        Raises ``ValueError`` listing every unknown, malformed or twice-defined term.
        """

        problems: list[str] = []

        if PUBLISHED_GLOSSARY_KEY in contract:
            problems.append(
                f"top-level '{PUBLISHED_GLOSSARY_KEY}' is reserved for published definitions; "
                "define dataset terms under meta.glossary"
            )

        meta = contract.get("meta")
        own_terms = _parse_terms(
            meta.get("glossary") if isinstance(meta, Mapping) else None,
            scope=TermScope.DATASET,
            where=f"{where} meta.glossary",
            problems=problems,
        )

        for key in sorted(own_terms.keys() & self.terms.keys()):
            problems.append(
                f"meta.glossary term {key!r} is already defined as a "
                f"{self.terms[key].scope} term; keep one definition"
            )

        available = {**self.terms, **own_terms}
        used: dict[str, GlossaryTerm] = dict(own_terms)
        pending = list(_term_keys(contract, where=where, problems=problems))

        while pending:
            key = pending.pop()
            if key in used:
                continue
            term = available.get(key)
            if term is None:
                problems.append(f"{where}: [[{key}]] is not defined in any glossary")
                continue
            used[key] = term
            # A definition may use other terms; the contract's own were read with its texts.
            pending.extend(_term_keys(term.definition, where=where, problems=problems))

        if problems:
            raise ValueError("Glossary errors:\n- " + "\n- ".join(dict.fromkeys(problems)))

        return {key: used[key] for key in sorted(used)}


def validate_contract_terms(
    glossary: Glossary, contracts: Mapping[Path, Mapping[str, Any]]
) -> tuple[str, ...]:
    """Check the terms of every contract at once; return the keys they use."""

    problems: list[str] = []
    used: set[str] = set()

    for path, contract in contracts.items():
        try:
            used.update(glossary.terms_used_by(contract, where=path.name))
        except ValueError as error:
            problems.append(str(error))

    if problems:
        raise ValueError("\n".join(problems))

    return tuple(sorted(used))


def load_glossary(path: Path) -> Glossary:
    """Load a workspace glossary; a workspace without ``glossary.yaml`` has Metrka's terms only."""

    if not path.is_file():
        return Glossary()

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise ValueError(f"{path}: YAML parse error: {error}") from error

    if not isinstance(document, Mapping) or document.get("version") != 1:
        raise ValueError(f"{path}: a glossary must be a mapping with 'version: 1' and 'terms'")

    unexpected = sorted(set(document) - {"version", "terms"})
    if unexpected:
        raise ValueError(f"{path}: unsupported glossary fields {unexpected}")

    problems: list[str] = []
    source_terms = _parse_terms(
        document.get("terms"), scope=TermScope.SOURCE, where=str(path), problems=problems
    )
    problems.extend(
        f"{path}: term {key!r} is a Metrka term; remove it from the source glossary"
        for key in sorted(source_terms.keys() & METRKA_TERMS.keys())
    )

    if problems:
        raise ValueError("Glossary errors:\n- " + "\n- ".join(problems))

    return Glossary(terms={**METRKA_TERMS, **source_terms})


def published_contract_bytes(contract_path: Path, glossary: Glossary) -> bytes:
    """Return a contract as published: its tables as built, then the definitions it uses.

    A table that uses a profile is published expanded, so the snapshot shows the table
    as it was built even after the profile changes. A contract that uses no profile and
    no term is published unchanged.
    """

    source = contract_path.read_bytes()
    written = yaml.safe_load(source) or {}
    contract = expand_profiles(written, contract_path=contract_path)
    if uses_profiles(written):
        source = yaml.safe_dump(contract, sort_keys=False, allow_unicode=True, width=1000).encode(
            "utf-8"
        )

    terms = glossary.terms_used_by(contract, where=contract_path.name)

    if not terms:
        return source

    block = yaml.safe_dump(
        {PUBLISHED_GLOSSARY_KEY: {key: term.published() for key, term in terms.items()}},
        sort_keys=True,
        allow_unicode=True,
        width=1000,
    )
    separator = b"\n" if source.endswith(b"\n") else b"\n\n"
    return source + separator + block.encode("utf-8")


def default_term_label(key: str) -> str:
    """``variability_measure`` reads as ``Variability measure``."""

    words = key.replace("_", " ")
    return words[:1].upper() + words[1:]


def _parse_terms(
    value: object, *, scope: TermScope, where: str, problems: list[str]
) -> dict[str, GlossaryTerm]:
    if value is None:
        return {}

    if not isinstance(value, Mapping):
        problems.append(f"{where} must map each term key to its definition")
        return {}

    terms: dict[str, GlossaryTerm] = {}

    for key, entry in value.items():
        if not isinstance(key, str) or _TERM_KEY.fullmatch(key) is None:
            problems.append(f"{where}: term key {key!r} must use lowercase letters, digits and '_'")
            continue

        label = default_term_label(key)
        definition: object = entry

        if isinstance(entry, Mapping):
            unexpected = sorted(set(entry) - _TERM_FIELDS)
            if unexpected:
                problems.append(f"{where}: term {key!r} has unsupported fields {unexpected}")
                continue
            label = entry.get("label", label)
            definition = entry.get("definition")

        if not isinstance(label, str) or not label.strip():
            problems.append(f"{where}: term {key!r} needs a non-empty label")
            continue

        if not isinstance(definition, str) or not definition.strip():
            problems.append(f"{where}: term {key!r} needs a non-empty definition")
            continue

        terms[key] = GlossaryTerm(
            key=key, label=label.strip(), definition=" ".join(definition.split()), scope=scope
        )

    return terms


def _term_keys(value: object, *, where: str, problems: list[str]) -> Iterator[str]:
    """Yield the term keys referenced in every text of a value, in order."""

    if isinstance(value, str):
        for match in _TERM_REFERENCE.finditer(value):
            key = match.group(1).split("|", 1)[0].strip()
            if _TERM_KEY.fullmatch(key) is None:
                problems.append(f"{where}: {match.group(0)} must name a term key such as [[adult]]")
                continue
            yield key
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _term_keys(item, where=where, problems=problems)
    elif isinstance(value, list):
        for item in value:
            yield from _term_keys(item, where=where, problems=problems)
