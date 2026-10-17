# metrka-core
[![Verify metrka-core](https://github.com/metrka/metrka-core/actions/workflows/static-analysis.yml/badge.svg?branch=main)](https://github.com/metrka/metrka-core/actions/workflows/static-analysis.yml)
[![Release](https://img.shields.io/github/v/release/metrka/metrka-core?display_name=tag&sort=semver)](https://github.com/metrka/metrka-core/releases/latest)
[![Python](https://img.shields.io/badge/python-%3E%3D3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/github/license/metrka/metrka-core)](https://github.com/metrka/metrka-core/blob/main/LICENSE)


[![Commits](https://img.shields.io/github/commit-activity/t/metrka/metrka-core?label=commits)](https://github.com/metrka/metrka-core/commits/main)
[![Commits since release](https://img.shields.io/github/commits-since/metrka/metrka-core/latest?sort=semver&label=since%20release)](https://github.com/metrka/metrka-core/compare/v1.0.0...main)
[![Code size](https://img.shields.io/github/languages/code-size/metrka/metrka-core?label=code%20size)](https://github.com/metrka/metrka-core)
[![Contributors](https://img.shields.io/github/contributors/metrka/metrka-core)](https://github.com/metrka/metrka-core/graphs/contributors)
[![Last commit](https://img.shields.io/github/last-commit/metrka/metrka-core?label=last%20commit)](https://github.com/metrka/metrka-core/commits/main)
[![Delivered features](https://img.shields.io/github/issues-search?query=repo%3Ametrka%2Fmetrka-core%20is%3Aissue%20is%3Aclosed%20label%3Aenhancement&label=features&color=blue)](https://github.com/metrka/metrka-core/issues?q=is%3Aissue%20is%3Aclosed%20label%3Aenhancement)

Core transformation utilities for building reproducible, auditable datasets from public data sources.

## Purpose

`metrka-core` is a Python package that provides reusable building blocks for:
- cleaning and normalizing raw tabular data
- applying schema- and config-driven transformations
- producing analysis-ready outputs with clear lineage

The goal is not to provide a framework, but a **disciplined, low-magic foundation** that can be reused across datasets.

## Terminology

A **workspace** is the configuration and storage boundary for one related group
of data pipelines. It owns a definition root and a data root and contains one or
more configured streams. Portable workspaces keep both roots under one
transferable directory; managed workspaces may place them separately.

A **stream** is one configured data flow inside a workspace. A **dataset** is
the independently processed and published identity of that stream. Its
`dataset_id` combines the workspace and stream names as
`<workspace_name>.<stream_name>`; for example, workspace
`wi_dhs_adult_lead` and stream `county` form dataset
`wi_dhs_adult_lead.county`. Stream names never contain a dot, so the last dot
separates the two parts; read them with `parse_dataset_id()` instead of splitting
the identifier yourself.

A **table** is one tabular output produced by a dataset. A dataset may produce
one or more tables, and each table may be materialized as one or more files or
publication assets.

Workspace operations initialize, locate, validate, import, export, and run the
whole configured workspace. Silver builds, publication candidates,
publications, and published assets belong to a specific dataset.

## Design principles

- **Reproducibility over convenience**
  Transformations are deterministic and inspectable.

- **Clarity over abstraction**
  Logic is explicit; there is no hidden behavior or implicit inference.

- **Separation of concerns**
  Core transformation logic lives here; dataset-specific configuration and orchestration live elsewhere.

- **Auditability**
  Every output can be traced back to source data and transformation steps.

## What this package does

- Applies schema-driven cleaning and type normalization
- Handles text, date, and numeric standardization explicitly
- Enforces consistent key handling and grain
- Supports metadata-aware output generation

## What this package does NOT do

- No analysis or visualization
- No opinionated metrics or dashboards
- No hidden heuristics or auto-inference

## Typical usage

`metrka-core` is intended to be used by dataset-specific pipelines, for example:

```text
raw data
  → apply schema & cleaning
  → produce wide clean table
  → run quality gates
  → publish governed outputs
```

## Installation

Metrka Core requires Python 3.12 or newer. For a released installation, create
an isolated environment, download the verified wheel from the matching
[GitHub Release](https://github.com/metrka/metrka-core/releases), and install
that exact artifact:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install .\metrka_core-1.0.0-py3-none-any.whl
metrka --version
```

For development, clone the repository and install the checkout in editable
mode:

```powershell
git clone https://github.com/metrka/metrka-core.git
Set-Location .\metrka-core
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
metrka --version
```

## Running a configured pipeline

Full pipeline execution requires PostgreSQL with the current Metrka metadata
schema and a workspace provided by a separate dataset package or local
checkout.

The workspace must live in a Git repository so every run can record its exact
source revision. That repository does not have to be a Python package and does
not have to be named `metrka-datasets`. If it has a PEP 621 `pyproject.toml`,
Metrka records its project name and version; otherwise it records the Git root
directory name and commit.

Configure the runtime connections without exposing database passwords in the
`metrka` process argument list. Supply the actual values through your secret
manager or protected local environment:

```powershell
$env:METRKA_MIGRATION_DSN = "postgresql://migration-role:<password>@localhost/metrka"
python -m metrka_core.metadata.migrations upgrade head

$env:METRKA_METADATA_DSN = "postgresql://runtime-role:<password>@localhost/metrka"
$env:METRKA_OPERATIONS_DSN = "postgresql://operator-role:<password>@localhost/metrka"
$env:METRKA_WORKSPACES_CONFIG_PATH = "C:\metrka\workspaces.local.yaml"
```

### PostgreSQL roles

Metrka separates database access by responsibility. A fresh database must have
these roles before migrations are applied:

| Role | Used for | Connection setting |
| --- | --- | --- |
| `metrka_owner` | Owns the database schemas and objects. It cannot log in directly. | None |
| `metrka_migrator` | Applies Alembic migrations by assuming `metrka_owner`. It is not used by pipelines or operator commands. | `METRKA_MIGRATION_DSN` |
| `metrka_operator` | Reviews engine releases, approves or rejects publication candidates, publishes approved candidates, and runs publication reconciliation. It can read metadata and write only the governance/publication records required by those commands. | `METRKA_OPERATIONS_DSN` |
| `metrka_etl` | Runs pipelines and writes normal runtime metadata. | `METRKA_METADATA_DSN` |
| `metrka_web` | Reads published catalog metadata for the Catalog API. | The Catalog API `DATABASE_URL` |

Create the roles once as a PostgreSQL administrator with
`src/metrka_core/metadata/migrations/bootstrap/provision_metadata_roles.sql`,
assign separate passwords to the four login roles, and then apply migrations.
`metrka_migrator` is the only login role that belongs to `metrka_owner`;
`metrka_operator`, `metrka_etl`, and `metrka_web` never inherit owner access.

Create and register a first HTTP workspace when no dataset checkout exists yet:

```powershell
metrka workspace init example_dataset `
    --download-url https://example.org/data/example.csv `
    --workspace-root C:\metrka\workspaces\example_dataset
```

This creates `workspaces.local.yaml`, registers an explicit `portable`
placement, and creates the standard workspace directory tree,
`conf/main.yaml`, and `conf/quality.yaml`.
The generated pipeline is deliberately Bronze-ready: it can download and
preserve the source file, but it does not invent a Silver schema. Add a dataset
contract and an explicit `silver.process` step after describing the source
columns and intended published tables.

Paths in the placement file are resolved relative to that file. A portable
workspace keeps definitions and ignored data below one transferable root:

```yaml
schema_version: 1
workspaces:
  example_dataset:
    placement: portable
    workspace_root: workspaces/example_dataset
```

A managed deployment can keep tracked definitions and persistent data in
different locations:

```yaml
schema_version: 1
workspaces:
  example_dataset:
    placement: managed
    definition_root: definitions/example_dataset
    data_root: D:/metrka-data/example_dataset
```

Create that form with `metrka workspace init --placement managed` and explicit
`--definition-root` and `--data-root` values.

A source that keeps each dataset in its own folder adds `layout: dataset_folders`
to either placement. A managed source then keeps each dataset's data in a folder
of the same name under its `data_root`; see
[Publication artifact access](PUBLIC_API.md#publication-artifact-access) for both
forms.

Validate static workspace configuration before the first run or in CI:

```powershell
metrka workspace validate example_dataset
```

Validation resolves the registered workspace and checks `main.yaml`, registered
extractor and action names, action options, quality gates, Silver task settings,
and any contracts enabled by a `silver.process` step. It does not connect to
PostgreSQL, test source-network availability, download files, execute pipeline
actions, or create runtime directories.

Export either workspace placement as one customer-readable portable package:

```powershell
metrka workspace export example_dataset `
    --output C:\metrka\exports\example_dataset.zip

metrka workspace verify-export `
    C:\metrka\exports\example_dataset.zip

metrka workspace import `
    C:\metrka\exports\example_dataset.zip `
    --destination-directory C:\metrka\customer-workspaces `
    --workspaces-config-path C:\metrka\workspaces.local.yaml
```

The ZIP reconstructs the workspace definitions at its top level and places the
runtime data below `data/`. Its versioned `metrka-workspace-manifest.json`
records every payload path, role, size, and SHA-256 checksum without exposing
the original machine's absolute paths. Export is atomic and verifies the new
archive before publishing it at the requested destination. The verification
command checks the manifest, exact ZIP membership, safe relative paths, sizes,
and checksums without extracting the package or connecting to PostgreSQL. Export
refuses to run while a Bronze or Silver execution marker exists and omits
the operational Silver staging directory. Treat `definition_root` as
customer-visible: deployment-only database configuration, environment files,
credentials, secrets, and private keys must remain outside it. Export rejects
reserved metadata-database configuration and sensitive paths before creating the
ZIP, while ordinary Silver, Quality, documentation, and pipeline definition files
remain exportable. Version-control internals, virtual environments, tool
caches, and compiled Python cache files are not copied.
The import command verifies the package before writing, installs it below a new
directory named after the workspace, and appends a portable placement to the
selected workspace configuration. It never replaces an existing workspace or
registration and removes newly extracted files if registration fails.

Run a workspace through the installed command:

```powershell
metrka run wi_dhs_adult_lead
```

Backfill one landing date:

```powershell
metrka run wi_dhs_adult_lead --date 2026-08-17
```

Force one Silver dataset to rebuild:

```powershell
metrka run wi_dhs_adult_lead `
    --dataset-id wi_dhs_adult_lead.county `
    --force-rebuild
```

The equivalent module invocation is:

```powershell
python -m metrka_core run wi_dhs_adult_lead
```

Use `metrka run --help` for all runtime options. Dataset packages that register
custom actions or extractors should call `metrka_core.api.run_pipeline()` with
their custom `PipelineRegistry`; the standard CLI deliberately uses only the
core registry.

Operate the metadata schema and governed Silver lifecycle through the same
installed command:

```powershell
metrka operations metadata check
metrka operations engine-releases list
metrka operations publication-candidates list
metrka operations reconcile-publications --dataset-id wi_dhs_adult_lead.county
```

Use `metrka operations --help` and each nested command's `--help` for mutation
options and required identities. Operator secrets remain in protected
environment or configuration values; they are never accepted directly on the
command line.

Production wheels carry an embedded build-provenance record. Release builders
must generate it from a clean checkout before building:

```powershell
$env:PYTHONPATH = (Join-Path $PWD "src")
python -m metrka_core.build_provenance
python -m build --wheel
```

See `RELEASING.md` for the complete verification, wheel smoke-test, and tagging
procedure.

## Scope and stability
This package is intentionally small and conservative.
Changes prioritize correctness and backward compatibility over feature growth.

## License

Metrka Core is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE).

This license applies to the `metrka-core` repository and its distributed
packages. Dataset repositories, external plugins, hosted services, and customer
workspaces may be governed by separate licenses and agreements.
