"""Generate immutable metrka-core source identity before building a wheel."""

from __future__ import annotations

import argparse
import os
import tomllib
from pathlib import Path
from typing import Any

from metrka_core.pipeline.provenance import write_core_build_provenance

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _project_version(repository_root: Path) -> str:
    pyproject_path = repository_root / "pyproject.toml"

    with pyproject_path.open("rb") as handle:
        document: dict[str, Any] = tomllib.load(handle)

    project = document.get("project")

    if not isinstance(project, dict):
        raise RuntimeError(f"Missing [project] table in {pyproject_path}")

    version = project.get("version")

    if not isinstance(version, str) or not version.strip():
        raise RuntimeError(f"Missing static project.version in {pyproject_path}")

    return version.strip()


def _default_branch() -> str | None:
    branch = os.environ.get("METRKA_BUILD_BRANCH")
    return branch.strip() if branch and branch.strip() else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write metrka-core build provenance for inclusion in a production wheel."
    )
    parser.add_argument(
        "--branch",
        default=_default_branch(),
        help="Source branch label. Git is queried when omitted.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Allow a dirty development build. Production builds must not use this option.",
    )
    args = parser.parse_args()

    destination = write_core_build_provenance(
        repository_root=REPOSITORY_ROOT,
        package_version_value=_project_version(REPOSITORY_ROOT),
        branch=args.branch,
        require_clean=not args.allow_dirty,
    )

    print(f"Wrote build provenance: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
