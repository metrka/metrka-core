"""Prove that metrka_core resolves from the isolated wheel installation."""

from __future__ import annotations

from pathlib import Path

import metrka_core


def main() -> int:
    module_file = metrka_core.__file__

    if module_file is None:
        raise SystemExit("metrka_core does not expose an import file")

    module_path = Path(module_file).resolve()
    print(module_path)

    if "site-packages" not in {part.casefold() for part in module_path.parts}:
        raise SystemExit(f"metrka_core was not imported from site-packages: {module_path}")

    print("Installed-package import isolation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
