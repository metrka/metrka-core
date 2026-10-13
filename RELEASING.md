# Releasing metrka-core

This document describes the supported release procedure. Run the commands
from a clean `main` checkout using Python 3.12 or newer.

## 1. Verify the release source

Confirm that `project.version` in `pyproject.toml` has a matching section in
`CHANGELOG.md`, then run the complete verification suite:

```powershell
python -m pip install -e ".[dev]"
python -m ruff format --check src tests release
python -m ruff check src tests release
python -m mypy src
python -m pytest -m "not integration" -q
python -m pytest -m integration -q
python -m metrka_core.metadata.migrations check
git status --short
```

The integration tests and migration check require the PostgreSQL environment
described in the repository workflow. `git status --short` must be empty.

## 2. Build from the exact release commit

Create immutable provenance before building. The generated JSON is ignored by
Git and belongs to the wheel, not to the source commit. `PYTHONPATH` is scoped
to provenance generation and restored before the wheel smoke test.

```powershell
$hadPythonPath = Test-Path Env:PYTHONPATH
$previousPythonPath = $env:PYTHONPATH

try {
    $env:PYTHONPATH = Join-Path $PWD "src"
    python -m metrka_core.build_provenance --branch main

    if ($LASTEXITCODE -ne 0) {
        throw "Build provenance generation failed."
    }
} finally {
    if ($hadPythonPath) {
        $env:PYTHONPATH = $previousPythonPath
    } else {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    }
}

python -m pip install --upgrade build
python -m build
```

Verify that exactly one wheel was produced, that its filename contains the
declared version, and that it passes the release audit. The audit requires
embedded provenance, PEP 639 Apache-2.0 metadata, the exact non-empty license
file, and the absence of development-only modules:

```powershell
$version = python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])"
$wheels = @(Get-ChildItem -LiteralPath dist -Filter "metrka_core-$version-*.whl")

if ($wheels.Count -ne 1) {
    throw "Expected one wheel for $version; found $($wheels.Count)."
}

python .\release\installed_e2e_smoke\audit_wheel.py $wheels[0].FullName

if ($LASTEXITCODE -ne 0) {
    throw "The release wheel failed its provenance and license audit."
}
```

## 3. Test the wheel outside the checkout

Use a temporary virtual environment so the smoke test cannot import the
working tree accidentally. The smoke process temporarily removes any existing
`PYTHONPATH`, then restores the caller's environment.

```powershell
$smokeRoot = Join-Path `
    $env:TEMP `
    ("metrka-core-release-smoke-" + [guid]::NewGuid().ToString("N"))
$hadPythonPath = Test-Path Env:PYTHONPATH
$previousPythonPath = $env:PYTHONPATH

try {
    Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    python -m venv $smokeRoot

    $smokePython = Join-Path $smokeRoot "Scripts\python.exe"
    $smokeMetrka = Join-Path $smokeRoot "Scripts\metrka.exe"

    & $smokePython -m pip install $wheels[0].FullName

    if ($LASTEXITCODE -ne 0) {
        throw "Release wheel installation failed."
    }

    if (-not (Test-Path -LiteralPath $smokeMetrka -PathType Leaf)) {
        throw "The installed wheel did not create the metrka command."
    }

    Push-Location $env:TEMP

    try {
        & $smokePython -c "import pathlib, sys, metrka_core; package_path = pathlib.Path(metrka_core.__file__).resolve(); venv_path = pathlib.Path(sys.prefix).resolve(); assert package_path.is_relative_to(venv_path), (package_path, venv_path); print(f'wheel import: {package_path}')"
        & $smokeMetrka --help
        & $smokePython -c "from metrka_core.api import initialize_workspace, run_pipeline; print('public API import passed')"
    } finally {
        Pop-Location
    }
} finally {
    if ($hadPythonPath) {
        $env:PYTHONPATH = $previousPythonPath
    } else {
        Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    }
}
```

The printed `wheel import` path must be inside the temporary virtual
environment. A path under the source checkout means the smoke test is invalid.

## 4. Run the installed pipeline end-to-end smoke

The unit, integration, and basic wheel checks do not prove that an installed
wheel can execute a complete dataset workflow, or that it ships every file the
workflow needs. Run the smoke after the candidate release commit has been merged
to a clean `main` branch:

```powershell
foreach ($placement in @("portable", "managed")) {
    & .elease\installed_e2e_smokeun-installed-e2e-smoke.ps1 `
        -WorkspacePlacement $placement

    if (-not $?) {
        throw "Installed E2E smoke failed for placement: $placement"
    }
}
```

The smoke needs nothing outside this repository. Its workspace and source file
live in `release/installed_e2e_smoke/fixture`; a local HTTP server serves the
source, and a disposable PostgreSQL container holds the metadata. Docker Desktop
must be running, and the test needs network access for Python dependencies and
the PostgreSQL image (`-PostgresImage`, default `postgres:17`). The container and
the temporary files are removed when the smoke passes.

The workspace uses a `main.yaml` template, table and meta profiles, and a
glossary, so the smoke also proves that the installed wheel fills them in and
that a customer export carries them.

The smoke also exports the completed workspace, verifies the ZIP, extracts it
below a new root, and reopens it through a new portable workspace configuration.
The extracted definitions, evidence, pointers, views, CSV, and Parquet files
must remain self-contained and independently verifiable.

Both placements are release requirements. The managed run binds the smoke
definitions to a detached temporary `data_root` and proves that the complete
installed pipeline, publication workflow, reconciliation, export, and portable
re-import never write runtime state below `definition_root`.

The test must finish with:

```text
Installed end-to-end smoke passed (placement=portable)
Installed end-to-end smoke passed (placement=managed)
```

## 5. Tag the final main commit

For the first public release, remove any obsolete local development tag before
creating the real tag. Never move a tag after consumers have started using it.

```powershell
$version = python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])"
$tag = "v$version"

git tag -d $tag 2>$null
git tag -a $tag -m "metrka-core $version"
git push origin main
git push origin $tag
```

Create the GitHub Release from that tag and use the matching section of
`CHANGELOG.md` as its release notes. Attach the wheel and source distribution
from `dist/`. Do not use `git push --tags`: it can publish obsolete local tags.
