"""Code provenance for source checkouts and installed production wheels."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tomllib
from dataclasses import asdict, dataclass
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import cast

_BUILD_PROVENANCE_FILENAME = "_build_provenance.json"
_BUILD_PROVENANCE_SCHEMA_VERSION = 1
_CORE_DISTRIBUTION_NAME = "metrka-core"
_CORE_REPOSITORY_NAME = "metrka-core"
_COMMIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40,64}$")


class BuildProvenanceError(RuntimeError):
    """Raised when installed code cannot prove which source revision produced it."""


@dataclass(frozen=True, slots=True)
class GitCodeRevision:
    """Exact revision of one code repository."""

    repository: str
    commit_sha: str
    branch: str | None
    package_version: str | None


@dataclass(frozen=True, slots=True)
class CodeProvenance:
    """Code revisions used by one pipeline run."""

    metrka_core: GitCodeRevision
    dataset_repository: GitCodeRevision
    dirty: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _run_git(repo_path: Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), *arguments], capture_output=True, text=True, check=False
        )
    except OSError as error:
        raise RuntimeError(f"Could not execute Git for {repo_path}: {error}") from error

    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"Git command failed for {repo_path}: git {' '.join(arguments)}: {error_message}"
        )

    return result.stdout.strip()


def find_git_root(path: Path) -> Path:
    """Ask Git for the repository containing path."""

    search_path = path.parent if path.is_file() else path
    root = _run_git(search_path.resolve(), "rev-parse", "--show-toplevel")
    return Path(root).resolve()


def _try_find_git_root(path: Path) -> Path | None:
    try:
        return find_git_root(path)
    except RuntimeError:
        return None


def _validate_commit_sha(value: object) -> str:
    if not isinstance(value, str) or _COMMIT_SHA_PATTERN.fullmatch(value) is None:
        raise BuildProvenanceError(
            "Build provenance commit_sha must contain a complete 40-64 character Git SHA"
        )

    return value


def _read_git_revision(
    *, repository: str, git_root: Path, package_version_value: str | None
) -> GitCodeRevision:
    branch = _run_git(git_root, "branch", "--show-current")

    return GitCodeRevision(
        repository=repository,
        commit_sha=_validate_commit_sha(_run_git(git_root, "rev-parse", "HEAD")),
        branch=branch or None,
        package_version=package_version_value,
    )


def _is_core_source_checkout(*, package_directory: Path, git_root: Path) -> bool:
    """Reject an enclosing consumer repository around an installed wheel."""

    expected_package_directory = git_root / "src" / "metrka_core"
    return expected_package_directory.resolve() == package_directory.resolve()


def _require_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)

    if not isinstance(value, str) or not value.strip():
        raise BuildProvenanceError(f"Build provenance field {key!r} must be a non-empty string")

    return value


def _read_embedded_core_provenance(
    *, metadata_path: Path, installed_package_version: str
) -> tuple[GitCodeRevision, bool]:
    if not metadata_path.is_file():
        raise BuildProvenanceError(
            "metrka-core is not running from its verified source checkout and the installed "
            f"package does not contain {_BUILD_PROVENANCE_FILENAME}. Build production wheels "
            "with python -m metrka_core.build_provenance before packaging."
        )

    try:
        raw_payload: object = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BuildProvenanceError(
            f"Could not read embedded build provenance from {metadata_path}: {error}"
        ) from error

    if not isinstance(raw_payload, dict) or any(not isinstance(key, str) for key in raw_payload):
        raise BuildProvenanceError("Embedded build provenance must be a JSON object")

    payload = cast(dict[str, object], raw_payload)
    schema_version = payload.get("schema_version")

    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != _BUILD_PROVENANCE_SCHEMA_VERSION
    ):
        raise BuildProvenanceError(
            "Unsupported build provenance schema_version: "
            f"{schema_version!r}; expected {_BUILD_PROVENANCE_SCHEMA_VERSION}"
        )

    repository = _require_string(payload, "repository")
    commit_sha = _validate_commit_sha(payload.get("commit_sha"))
    package_version_value = _require_string(payload, "package_version")
    branch_value = payload.get("branch")
    dirty_value = payload.get("dirty")

    if repository != _CORE_REPOSITORY_NAME:
        raise BuildProvenanceError(
            f"Embedded build provenance describes {repository!r}, not {_CORE_REPOSITORY_NAME!r}"
        )

    if branch_value is not None and not isinstance(branch_value, str):
        raise BuildProvenanceError("Build provenance field 'branch' must be a string or null")

    if not isinstance(dirty_value, bool):
        raise BuildProvenanceError("Build provenance field 'dirty' must be a boolean")

    if package_version_value != installed_package_version:
        raise BuildProvenanceError(
            "Embedded build provenance package_version does not match the installed distribution: "
            f"{package_version_value!r} != {installed_package_version!r}"
        )

    return (
        GitCodeRevision(
            repository=repository,
            commit_sha=commit_sha,
            branch=branch_value or None,
            package_version=package_version_value,
        ),
        dirty_value,
    )


def _collect_core_code_revision(
    *, package_directory: Path, installed_package_version: str
) -> tuple[GitCodeRevision, bool]:
    possible_git_root = _try_find_git_root(package_directory)

    if possible_git_root is not None and _is_core_source_checkout(
        package_directory=package_directory, git_root=possible_git_root
    ):
        revision = _read_git_revision(
            repository=_CORE_REPOSITORY_NAME,
            git_root=possible_git_root,
            package_version_value=installed_package_version,
        )
        dirty = bool(
            _run_git(possible_git_root, "status", "--porcelain", "--untracked-files=normal")
        )
        return revision, dirty

    return _read_embedded_core_provenance(
        metadata_path=package_directory / _BUILD_PROVENANCE_FILENAME,
        installed_package_version=installed_package_version,
    )


def collect_core_code_revision() -> tuple[GitCodeRevision, bool]:
    """Collect core revision from a verified checkout or an installed wheel."""

    package_directory = Path(__file__).resolve().parents[1]
    return _collect_core_code_revision(
        package_directory=package_directory,
        installed_package_version=distribution_version(_CORE_DISTRIBUTION_NAME),
    )


def _read_dataset_repository_identity(git_root: Path) -> tuple[str, str | None]:
    """Read an optional PEP 621 identity without requiring an installed distribution."""

    fallback_name = git_root.name.strip()

    if not fallback_name:
        raise RuntimeError(f"Dataset repository has no usable name: {git_root}")

    pyproject_path = git_root / "pyproject.toml"

    if not pyproject_path.is_file():
        return fallback_name, None

    try:
        with pyproject_path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(
            f"Could not read dataset repository identity: {pyproject_path}"
        ) from error

    project = document.get("project")

    if not isinstance(project, dict):
        return fallback_name, None

    raw_name = project.get("name")
    raw_version = project.get("version")
    repository = (
        raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else fallback_name
    )
    package_version_value = (
        raw_version.strip() if isinstance(raw_version, str) and raw_version.strip() else None
    )

    return repository, package_version_value


def collect_dataset_code_revision(definition_path: Path) -> tuple[GitCodeRevision, bool]:
    """Collect identity directly from the Git repository containing a workspace."""

    git_root = find_git_root(definition_path)
    repository, package_version_value = _read_dataset_repository_identity(git_root)
    revision = _read_git_revision(
        repository=repository, git_root=git_root, package_version_value=package_version_value
    )
    dirty = bool(_run_git(git_root, "status", "--porcelain", "--untracked-files=normal"))
    return revision, dirty


def write_core_build_provenance(
    *,
    repository_root: Path,
    package_version_value: str,
    branch: str | None = None,
    require_clean: bool = True,
) -> Path:
    """Write immutable source identity that setuptools will include in a wheel."""

    git_root = find_git_root(repository_root)
    package_directory = git_root / "src" / "metrka_core"

    if not _is_core_source_checkout(package_directory=package_directory, git_root=git_root):
        raise BuildProvenanceError(f"Not a metrka-core source checkout: {git_root}")

    normalized_package_version = package_version_value.strip()

    if not normalized_package_version:
        raise BuildProvenanceError("package_version_value must not be empty")

    status = _run_git(git_root, "status", "--porcelain", "--untracked-files=normal")
    dirty = bool(status)

    if require_clean and dirty:
        raise BuildProvenanceError(
            "Refusing to create production build provenance from a dirty working tree"
        )

    resolved_branch = branch.strip() if branch is not None else ""

    if not resolved_branch:
        resolved_branch = _run_git(git_root, "branch", "--show-current")

    payload = {
        "schema_version": _BUILD_PROVENANCE_SCHEMA_VERSION,
        "repository": _CORE_REPOSITORY_NAME,
        "commit_sha": _validate_commit_sha(_run_git(git_root, "rev-parse", "HEAD")),
        "branch": resolved_branch or None,
        "package_version": normalized_package_version,
        "dirty": dirty,
    }

    destination = package_directory / _BUILD_PROVENANCE_FILENAME
    temporary_path = destination.with_name(f".{destination.name}.tmp")

    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, destination)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

    return destination


def collect_code_provenance(*, definition_path: Path) -> CodeProvenance:
    """Collect core and workspace-repository provenance for one pipeline run."""

    core_revision, core_dirty = collect_core_code_revision()
    dataset_revision, dataset_dirty = collect_dataset_code_revision(definition_path)

    return CodeProvenance(
        metrka_core=core_revision,
        dataset_repository=dataset_revision,
        dirty=core_dirty or dataset_dirty,
    )
