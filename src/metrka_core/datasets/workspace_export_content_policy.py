"""Fail-closed content policy for customer workspace exports."""

from __future__ import annotations

import json
import re
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

import yaml

_PRIVATE_KEY_SUFFIXES: Final = frozenset({".jks", ".key", ".keystore", ".p12", ".pfx"})
_PRIVATE_KEY_FILE_NAMES: Final = frozenset({"id_dsa", "id_ecdsa", "id_ed25519", "id_rsa"})
_PRIVATE_KEY_PEM_MARKERS: Final = (
    b"-----BEGIN PRIVATE KEY-----",
    b"-----BEGIN RSA PRIVATE KEY-----",
    b"-----BEGIN EC PRIVATE KEY-----",
    b"-----BEGIN OPENSSH PRIVATE KEY-----",
)
_DOCUMENTATION_SUFFIXES: Final = frozenset({".md", ".rst"})
_SENSITIVE_NAME_TOKENS: Final = frozenset({"credential", "credentials", "secret", "secrets"})
_STRUCTURED_CONFIGURATION_SUFFIXES: Final = frozenset({".json", ".toml", ".yaml", ".yml"})
_RESERVED_RUNTIME_CONFIGURATION_KEYS: Final = frozenset(
    {"metadata_config_path", "metadata_database", "metadata_dsn"}
)

__all__ = ["WorkspaceExportContentPolicyError", "WorkspaceExportPolicyViolation"]


@dataclass(frozen=True, slots=True)
class WorkspaceExportPolicyViolation:
    path: str
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path.strip():
            raise ValueError("Workspace export policy violation path must not be empty")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("Workspace export policy violation reason must not be empty")


class WorkspaceExportContentPolicyError(ValueError):
    """The definition root contains deployment-only or sensitive content."""

    def __init__(self, violations: Iterable[WorkspaceExportPolicyViolation]) -> None:
        ordered = tuple(sorted(violations, key=lambda item: (item.path.casefold(), item.reason)))
        if not ordered:
            raise ValueError("WorkspaceExportContentPolicyError requires at least one violation")

        self.violations = ordered
        details = "\n".join(f"- {item.path}: {item.reason}" for item in ordered)
        super().__init__(
            "Workspace export blocked by the customer content policy. "
            "definition_root is customer-visible; move deployment-only or sensitive files "
            f"outside it:\n{details}"
        )


def validate_workspace_export_definition_files(files: Iterable[tuple[str, Path]]) -> None:
    """Reject content that must not cross the customer-export boundary."""

    violations: list[WorkspaceExportPolicyViolation] = []
    for portable_path, source_path in files:
        filename_reason = _sensitive_path_reason(portable_path)
        if filename_reason is not None:
            violations.append(
                WorkspaceExportPolicyViolation(path=portable_path, reason=filename_reason)
            )

        if filename_reason is None and _contains_private_key_pem(source_path):
            violations.append(
                WorkspaceExportPolicyViolation(
                    path=portable_path,
                    reason="private-key material must not be included in a customer export",
                )
            )

        if source_path.suffix.casefold() not in _STRUCTURED_CONFIGURATION_SUFFIXES:
            continue

        try:
            documents = _load_structured_configuration(source_path)
        except (
            OSError,
            UnicodeError,
            json.JSONDecodeError,
            tomllib.TOMLDecodeError,
            yaml.YAMLError,
        ) as error:
            violations.append(
                WorkspaceExportPolicyViolation(
                    path=portable_path,
                    reason=(
                        f"structured definition cannot be inspected safely ({type(error).__name__})"
                    ),
                )
            )
            continue

        reserved_keys = _find_reserved_runtime_keys(documents)
        if reserved_keys:
            rendered_keys = ", ".join(repr(key) for key in reserved_keys)
            violations.append(
                WorkspaceExportPolicyViolation(
                    path=portable_path,
                    reason=f"contains deployment-only configuration key(s): {rendered_keys}",
                )
            )

    if violations:
        raise WorkspaceExportContentPolicyError(violations)


def _sensitive_path_reason(portable_path: str) -> str | None:
    path = PurePosixPath(portable_path)
    lowered_parts = tuple(part.casefold() for part in path.parts)
    filename = lowered_parts[-1]
    suffix = path.suffix.casefold()

    if filename == ".env" or filename.startswith(".env.") or suffix == ".env":
        return "environment files are deployment-only and may contain secrets"
    if suffix in _PRIVATE_KEY_SUFFIXES or filename in _PRIVATE_KEY_FILE_NAMES:
        return "private-key material must not be included in a customer export"

    for part in lowered_parts:
        stem = PurePosixPath(part).stem
        tokens = frozenset(token for token in re.split(r"[^a-z0-9]+", stem) if token)
        if tokens & _SENSITIVE_NAME_TOKENS and suffix not in _DOCUMENTATION_SUFFIXES:
            return "credential or secret files must not be included in a customer export"
        if {"private", "key"}.issubset(tokens) and suffix not in _DOCUMENTATION_SUFFIXES:
            return "private-key material must not be included in a customer export"

    return None


def _load_structured_configuration(path: Path) -> tuple[object, ...]:
    content = path.read_text(encoding="utf-8-sig")
    suffix = path.suffix.casefold()
    if suffix in {".yaml", ".yml"}:
        return tuple(yaml.safe_load_all(content))
    if suffix == ".json":
        return (json.loads(content),)
    if suffix == ".toml":
        return (tomllib.loads(content),)
    raise ValueError(f"Unsupported structured configuration suffix: {suffix}")


def _contains_private_key_pem(path: Path) -> bool:
    if path.suffix.casefold() != ".pem":
        return False
    content = path.read_bytes()
    return any(marker in content for marker in _PRIVATE_KEY_PEM_MARKERS)


def _find_reserved_runtime_keys(documents: Iterable[object]) -> tuple[str, ...]:
    found: set[str] = set()
    stack = list(documents)
    visited_containers: set[int] = set()

    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            identity = id(current)
            if identity in visited_containers:
                continue
            visited_containers.add(identity)
            for key, value in current.items():
                if isinstance(key, str):
                    normalized_key = key.strip().casefold()
                    if normalized_key in _RESERVED_RUNTIME_CONFIGURATION_KEYS:
                        found.add(normalized_key)
                stack.append(value)
        elif isinstance(current, (list, tuple, set, frozenset)):
            identity = id(current)
            if identity in visited_containers:
                continue
            visited_containers.add(identity)
            stack.extend(current)

    return tuple(sorted(found))
