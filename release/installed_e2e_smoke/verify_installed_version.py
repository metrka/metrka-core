"""Verify the installed CLI reports the wheel's exact package and source identity."""

from __future__ import annotations

import subprocess
import sys
from importlib.metadata import version as distribution_version
from pathlib import Path

from metrka_core.pipeline.provenance import collect_core_code_revision


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: verify_installed_version.py METRKA_COMMAND WORKING_DIRECTORY")

    command_path = Path(sys.argv[1]).resolve()
    working_directory = Path(sys.argv[2]).resolve()

    if not command_path.is_file():
        raise SystemExit(f"Installed metrka command does not exist: {command_path}")

    revision, dirty = collect_core_code_revision()
    qualifiers = [f"commit {revision.commit_sha[:12]}"]

    if dirty:
        qualifiers.append("dirty")

    expected = f"metrka-core {distribution_version('metrka-core')} ({', '.join(qualifiers)})"
    completed = subprocess.run(
        [str(command_path), "--version"],
        cwd=working_directory,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        raise SystemExit(
            "Installed version command failed with exit code "
            f"{completed.returncode}: {completed.stderr.strip()}"
        )

    actual = completed.stdout.strip()

    if actual != expected:
        raise SystemExit(
            f"Installed version output does not match wheel provenance: {actual!r} != {expected!r}"
        )

    if completed.stderr.strip():
        raise SystemExit(
            f"Installed version command wrote unexpected stderr: {completed.stderr.strip()}"
        )

    print(actual)
    print("Installed version and provenance check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
