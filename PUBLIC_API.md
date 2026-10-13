# Public API

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

## Supported namespaces

The following namespaces are supported extension points for
metrka-datasets and other Metrka integrations:

- `metrka_core.api`
- `metrka_core.extensions.action`
- `metrka_core.extensions.acquisition`
- `metrka_core.extensions.source_schema`

Modules outside these namespaces are implementation details unless
explicitly documented otherwise.

Internal modules may move during refactoring. Public imports must remain
compatible within the declared package version range.

## Public symbol reference

Every name below is exported by `metrka_core.api`. The table is the canonical
symbol inventory for that module; removing or changing one of these contracts
requires the corresponding compatibility decision for the package version.

| Symbol | Category | Contract |
|---|---|---|
| `Clock` | Runtime protocol | Supplies timezone-aware UTC timestamps to injected runtime services and deterministic tests. |
| `ContractSnapshotFailure` | Publication failure enum | Identifies why a contract snapshot read failed without requiring callers to inspect exception text. |
| `ContractSnapshotReader` | Publication protocol | Reads immutable contract snapshot JSON without exposing the concrete local-filesystem adapter. |
| `ContractSnapshotReadError` | Publication exception | Reports a structured contract snapshot failure with its stable reason and requested relative path. |
| `PipelineBootstrapOptions` | Pipeline configuration | Groups advanced workspace, environment, service, and metadata settings accepted by `run_pipeline()`. |
| `PipelineRegistry` | Extension registry | Maps YAML extractor and action identifiers to implementations for custom dataset pipelines. |
| `PipelineRunIdGenerator` | Runtime protocol | Generates pipeline execution identifiers through `RuntimeServices`. |
| `PipelineRunResult` | Pipeline result | Returns the completed pipeline run identifier together with its `PipelineRunState`. |
| `PipelineRunState` | Pipeline state | Carries values produced across acquisition and ordered actions and is exposed to action extensions. |
| `PublicationManifestFailure` | Publication failure enum | Identifies why a manifest read failed without requiring callers to inspect exception text. |
| `PublicationManifestReader` | Publication protocol | Reads immutable Silver publication manifests without exposing the concrete local-filesystem adapter. |
| `PublicationManifestReadError` | Publication exception | Reports a structured manifest read failure with its stable reason and requested relative path. |
| `RuntimeEnvironment` | Configuration enum | Selects development or production behavior, including production-safe configuration resolution. |
| `RuntimeServices` | Runtime configuration | Bundles the clock and identifier providers used to make programmatic runs deterministic. |
| `WorkspaceInitializationResult` | Workspace result | Reports the placement, roots, registry path, and generated configuration paths created by `initialize_workspace()`. |
| `WorkspaceImportResult` | Workspace result | Reports the verified package, installed portable roots, registry binding, and payload totals created by `import_workspace()`. |
| `WorkspaceExportContentPolicyError` | Export exception | Rejects customer-visible definitions containing sensitive files or deployment-only configuration and exposes structured violations. |
| `WorkspaceExportPolicyViolation` | Export evidence | Describes one rejected portable definition path and the content-policy reason. |
| `WorkspaceExportIntegrityError` | Export exception | Rejects malformed, unsafe, incomplete, or checksum-inconsistent customer workspace packages. |
| `WorkspaceExportResult` | Workspace result | Reports the created package path, checksum, source placement, and payload totals. |
| `WorkspaceExportVerificationResult` | Workspace result | Reports the independently verified package identity, checksum, creation time, placement, and payload totals. |
| `WorkspaceValidationResult` | Workspace result | Reports resolved static workspace, stream, action, quality, and Silver-contract configuration. |
| `BronzeRunIdGenerator` | Runtime protocol | Generates Bronze run identifiers through `RuntimeServices`. |
| `DatasetFileIdGenerator` | Runtime protocol | Generates dataset-file identifiers through `RuntimeServices`. |
| `DatasetIdentity` | Dataset identity | Holds a canonical `dataset_id` with the workspace and stream names returned by `parse_dataset_id()`. |
| `DatasetIdentityError` | Dataset identity exception | Rejects a `dataset_id` that breaks the identity rule and exposes the stable `reason`. |
| `WorkspaceName` | Dataset identity | Types the source workspace name from `main.yaml`, the part of a `dataset_id` before the last dot, so type checkers reject a run name in its place. |
| `RunName` | Dataset identity | Types the name a run or validation targets: a workspace, or `workspace.dataset` for a dataset folder; it is not interchangeable with `WorkspaceName`. |
| `WorkspaceLocation` | Workspace model | Binds one logical workspace, or one dataset folder of a source workspace, to resolved definition and persistent data roots. |
| `WorkspaceLocationResolver` | Workspace protocol | Resolves a configured workspace name, `source.dataset` for a dataset folder, or a published `dataset_id` (`resolve_dataset`) to a `WorkspaceLocation`. |
| `WorkspacePlacement` | Configuration enum | Distinguishes portable workspaces from managed, independently placed definition and data roots. |
| `SilverBuildIdGenerator` | Runtime protocol | Generates Silver build identifiers through `RuntimeServices`. |
| `create_core_registry` | Registry factory | Creates a fresh registry containing the built-in acquisition extractors and pipeline actions. |
| `create_contract_snapshot_reader` | Publication factory | Creates a reader restricted to immutable contract snapshot storage below one resolved workspace data root. |
| `create_publication_manifest_reader` | Publication factory | Creates a reader restricted to Silver manifest storage below one resolved workspace data root. |
| `create_workspace_location_resolver` | Workspace factory | Creates the configured `WorkspaceLocationResolver` while keeping its concrete YAML adapter private. |
| `execute_configured_pipeline` | Advanced execution | Executes acquisition and configured YAML actions with an already opened context and composed registry. |
| `export_workspace` | Workspace operation | Builds and verifies one portable customer ZIP from a configured portable or managed workspace. |
| `initialize_workspace` | Workspace operation | Creates and registers a new Bronze-ready portable or managed workspace without overwriting existing state. |
| `import_workspace` | Workspace operation | Verifies, installs, and registers a customer workspace package as a new portable workspace. |
| `ProfileError` | Contract read error | Raised by `load_contract` when a table or meta profile is missing, malformed, or a contract changes what its profile fixes; the message lists every problem. |
| `MainTemplateError` | Configuration read error | Raised by `load_main_config` when a template is missing or malformed, an input is missing or unknown, or a main.yaml that uses a template sets anything besides `uses` and `with`; the message lists every problem. |
| `load_main_config` | Configuration read | Reads one workspace `main.yaml`, filled in from the template it names (`uses` and `with`), as validation and pipeline runs see it. A main.yaml without `uses` is returned as written. |
| `load_contract` | Contract read | Reads one contract file with its table and meta profiles applied, as validation, Silver builds and published snapshots see it. Raises `ProfileError` when a profile is missing or a contract changes what its profile fixes. |
| `open_pipeline_context` | Advanced execution | Opens the PostgreSQL-backed composition and lifecycle context for one pipeline run and closes it on exit. |
| `parse_dataset_id` | Dataset identity | Splits `<workspace_name>.<stream_name>` at the last dot, the one rule every Metrka reader must use instead of parsing identifiers itself. |
| `run_pipeline` | Pipeline operation | Runs acquisition and all configured actions for one workspace and returns `PipelineRunResult`. |
| `validate_workspace` | Workspace operation | Validates static workspace configuration without PostgreSQL, acquisition, action execution, or runtime writes. |
| `verify_workspace_export` | Workspace operation | Verifies manifest structure, safe membership, sizes, and checksums without extracting the package. |

## Publication artifact access

Applications may combine the workspace resolver with the publication readers
without importing Core storage implementations:

```python
from metrka_core.api import (
    RuntimeEnvironment,
    create_contract_snapshot_reader,
    create_publication_manifest_reader,
    create_workspace_location_resolver,
)

location = create_workspace_location_resolver(
    runtime_environment=RuntimeEnvironment.PRODUCTION,
).resolve_dataset("wi_dhs_adult_lead.county")

reader = create_publication_manifest_reader(data_root=location.data_root)
# Read this value from catalog.dataset_publications.manifest_path.
manifest = reader.read_manifest(path=manifest_path)

contract_reader = create_contract_snapshot_reader(data_root=location.data_root)
# Read this value from the selected publication's contract snapshot metadata.
contract = contract_reader.read_snapshot(path=contract_snapshot_json_path)
```

`resolve_dataset()` returns the folder that defines a published dataset. Use it
instead of resolving the workspace part of a `dataset_id` yourself: a source
registered with `layout: dataset_folders` keeps each dataset in its own folder.

```yaml
schema_version: 1
workspaces:
  fl_healthcharts:
    placement: portable
    workspace_root: datasets/fl_healthcharts
    layout: dataset_folders
```

Here `fl_healthcharts/adult_substance_abuse_beds/` holds its own `conf/` and
`data/`. Its `conf/main.yaml` declares `workspace_name: fl_healthcharts` and
exactly one stream named `adult_substance_abuse_beds`, so the dataset is
`fl_healthcharts.adult_substance_abuse_beds`. Run and validate it by that name.

Manifest paths are restricted to `files/silver/manifests/`. Contract snapshot
paths are restricted to `contracts/`. Every supplied path must be a canonical
POSIX relative path. Absolute paths, parent traversal, Windows drive
paths, backslashes, non-portable components, and symlink escapes are rejected.
The target must exist as a UTF-8 JSON file whose top-level value is an object.

Operational failures raise `PublicationManifestReadError` or
`ContractSnapshotReadError`. Their matching failure enums allow applications
to distinguish missing files,
unsafe paths, storage escapes, read failures, invalid JSON, and non-object JSON
without parsing exception messages. Passing a non-`Path` data root remains a
programming error and raises `TypeError`.

## Quality check extensibility

Metrka Core runs built-in checks at every pipeline gate automatically: the
landed file is not empty and has a recorded SHA-256, ZIP and XLSX files are
intact, tables have rows and match their contract columns, and output files
exist. A workspace's `conf/quality.yaml` (version 1) adds data rules per Silver
table and published column: `not_null`, `unique`, `min`, `max`, `between`, and
`allowed`, each with an optional `severity`. Workspace validation rejects rules
that name tables or columns the Silver contracts do not publish.

Custom Python quality-check implementations are not part of the public
extension API. The check functions and gate functions in `metrka_core.quality`
remain internal implementation details and may change without compatibility
guarantees.

Public Gold-layer and custom quality-check extension points may be introduced in
a future release together with bootstrap, workspace-validation, and runtime
composition support. They are not part of the public API in version 1.0.

## External plugin licensing

The Apache-2.0 license of `metrka-core` applies to this repository and its
distributed packages. A separately developed and distributed plugin that uses
the documented extension protocols is an independent distribution and may be
governed by its own license terms.

External extensions must use a protocol explicitly documented in this file when
they need a stable compatibility boundary. Version 1.0 documents action,
acquisition, and source-schema extension protocols. Placing code in an external
package does not by itself make an internal `metrka_core` module public or stable.

## Pipeline execution

`run_pipeline()` is the supported high-level entry point for executing one
configured workspace:

```python
from metrka_core.api import run_pipeline

result = run_pipeline("wi_dhs_adult_lead")
print(result.pipeline_run_id)
```

`PipelineBootstrapOptions` holds optional infrastructure configuration for
programmatic callers. `open_pipeline_context()` and
`execute_configured_pipeline()` remain supported advanced entry points for
integrations that need direct control over composition and execution.

Full pipeline execution requires a reachable PostgreSQL metadata database
with the current Metrka metadata schema.

Quality gates are configured as part of the pipeline and executed by
`metrka-core`. The internal quality gate functions are not independent public
execution APIs.


## Workspace initialization

`initialize_workspace()` is the supported programmatic entry point for creating
and registering a new local workspace:

```python
from pathlib import Path

from metrka_core.api import initialize_workspace

result = initialize_workspace(
    "example_dataset",
    download_url="https://example.org/data/example.csv",
    workspaces_config_path=Path("workspaces.local.yaml"),
    workspace_root=Path("workspaces/example_dataset"),
)
print(result.workspace_root)
print(result.definition_root)
print(result.data_root)
```

`WorkspaceInitializationResult` reports the placement, optional portable
workspace root, definition and data roots, registry, and configuration paths.
Pass `placement="managed"`, `definition_root`, and `data_root` to create a
detached managed workspace. Initialization never overwrites an existing
workspace or registration. The generated
configuration supports standard HTTP acquisition and Bronze ingestion only;
callers must add a real contract before enabling Silver processing.

Workspace registration is serialized per placement configuration file. The
read, conflict checks, filesystem commit, and atomic registry replacement run
under an adjacent interprocess `.lock` file, preventing concurrent `init` or
`import` commands from silently losing another workspace registration. The lock
file is operational state and may remain after the process releases the lock.


## Workspace validation

`validate_workspace()` is the supported read-only entry point for checking a
configured workspace before execution:

```python
from metrka_core.api import validate_workspace

result = validate_workspace("example_dataset")
print(result.pipeline_actions)
```

`WorkspaceValidationResult` reports the resolved configuration, streams,
actions, quality-check count, and validated Silver contract paths. Validation
does not connect to PostgreSQL, acquire data, execute actions, or write workspace
state. Integrations with custom actions or extractors may pass their composed
`PipelineRegistry` explicitly.


## Customer workspace export

`export_workspace()` reconstructs either a portable or managed workspace as one
portable ZIP package. Definitions keep their workspace-relative paths and the
configured data root is placed below `data/`, so the extracted directory has the
same physical contract as a portable workspace:

```python
from pathlib import Path

from metrka_core.api import export_workspace, import_workspace, verify_workspace_export

created = export_workspace(
    "example_dataset",
    Path("exports/example_dataset.zip"),
)
print(created.package_checksum)

verified = verify_workspace_export(created.package_path)
print(verified.file_count)

installed = import_workspace(
    created.package_path,
    destination_directory=Path("customer-workspaces"),
    workspaces_config_path=Path("workspaces.local.yaml"),
)
print(installed.workspace_root)
```

The package contains `metrka-workspace-manifest.json` below its single workspace
directory. The versioned manifest records the source placement, creation time,
canonical relative path, role, byte size, and SHA-256 checksum of every payload
file. It never records the source machine's absolute definition or data paths.

`definition_root` is a customer-visible product boundary: every retained file
below it is intended for the recipient. Deployment-only database configuration,
environment files, credentials, secrets, and private keys must live outside that
root. Export inspects structured definition files and fails before writing the
archive when it finds reserved metadata-database keys or a sensitive path. The
error lists every rejected portable path without exposing source-machine paths.

Export fails if a source root is unavailable, a linked file or directory is
encountered, a managed definition uses the reserved `data/` path, the destination
is inside either source root, or an existing destination would be overwritten
without explicit permission. An active Bronze or Silver execution marker
also blocks export so one package cannot silently combine two runtime states.
Operational Silver staging files are excluded. Version-control internals,
virtual environments, tool caches, and compiled Python cache files are also not
product content and are excluded. The archive is written atomically and verified
against its own manifest before it becomes visible at the destination.

`verify_workspace_export()` rejects malformed manifests, duplicate or unsafe ZIP
members, unrecorded or missing files, size changes, and checksum changes. It does
not extract the archive or connect to PostgreSQL. Successful results return the
checksum of the ZIP itself so it can be recorded or transmitted separately.

`import_workspace()` verifies the package, installs it below a new destination
directory, and registers the result as a portable workspace. The package's
workspace name determines the new directory and registry key. Existing roots and
registrations are never replaced. Extraction happens in a temporary directory;
the registry lock is acquired only for the final commit. The current registry is
then read again before installation, so a registration added during extraction is
preserved. If the registry cannot be written atomically, the newly installed root
is removed.

### Customer export error model

The two public export exceptions are separate `ValueError` subclasses because
they describe failures at different trust boundaries:

- `WorkspaceExportContentPolicyError` means export was stopped before a package
  was published because `definition_root` contains customer-visible content that
  is sensitive, deployment-only, or cannot be inspected safely. Its
  `violations` tuple contains `WorkspaceExportPolicyViolation` values with a
  portable `path` and human-readable `reason`. Integrations should consume these
  fields rather than parse the exception message; absolute source paths are not
  included.
- `WorkspaceExportIntegrityError` means a package manifest or ZIP failed
  structural, path-safety, membership, size, or checksum verification. It may be
  raised by `verify_workspace_export()`, by the verification phase of
  `export_workspace()`, or before `import_workspace()` writes the installed
  workspace.

Ordinary caller and filesystem errors continue to use standard exceptions such
as `ValueError`, `FileNotFoundError`, `FileExistsError`, and
`NotADirectoryError`. There is intentionally no broad catch-all Metrka export
exception: callers can distinguish a content-policy refusal from corrupted or
untrusted package evidence.


## Command-line execution

Installing `metrka-core` provides the stable `metrka` command. A configured
workspace using the standard core registry can be executed with:

```text
metrka run wi_dhs_adult_lead
```

`python -m metrka_core run wi_dhs_adult_lead` is equivalent. Connection secrets
are intentionally not accepted as command-line arguments; use
`METRKA_METADATA_DSN` or a protected metadata configuration file instead.

The stable command for creating a Bronze-ready workspace is:

```text
metrka workspace init example_dataset --download-url https://example.org/data.csv --workspace-root workspaces/example_dataset
```

Portable initialization requires `--workspace-root`. Managed initialization
requires `--placement managed`, `--definition-root`, and `--data-root`. The
command selects its placement file through `--workspaces-config-path`,
`METRKA_WORKSPACES_CONFIG_PATH`, or `workspaces.local.yaml` in development.
The same precedence and production safeguards apply when calling
`initialize_workspace()` directly.

Validate a workspace without executing it or requiring PostgreSQL:

```text
metrka workspace validate example_dataset
```

Create and independently verify a portable customer package:

```text
metrka workspace export example_dataset --output exports/example_dataset.zip
metrka workspace verify-export exports/example_dataset.zip
metrka workspace import exports/example_dataset.zip --destination-directory customer-workspaces
```

Use `--overwrite` only when replacement of an existing package is intentional.
The output path must be outside both configured workspace roots.
Import selects its registry through `--workspaces-config-path`,
`METRKA_WORKSPACES_CONFIG_PATH`, or `workspaces.local.yaml` and always creates a
portable placement below the supplied destination directory.
Production initialization and import require an explicit path or
`METRKA_WORKSPACES_CONFIG_PATH`; they never create a registry in the process
working directory implicitly. The same rule applies to CLI and Python API calls.

The CLI uses the standard core registry. Dataset packages that provide custom
actions or extractors compose their registry explicitly and call
`metrka_core.api.run_pipeline()` from their own entrypoint.


## Operator commands

The installed `metrka operations` command is the supported interface for
metadata administration and Silver governance workflows:

```text
metrka operations metadata check
metrka operations engine-releases list
metrka operations publication-candidates list --dataset-id example.county
metrka operations reconcile-publications --dataset-id example.county
```

Run `metrka operations --help` or the nested command's `--help` for the full
options. The metadata migration command uses the privileged connection selected
by `METRKA_MIGRATION_DSN`. Engine-release decisions, publication-candidate
commands, and publication reconciliation use the dedicated least-privilege
operator connection selected by `METRKA_OPERATIONS_DSN`. This separation keeps
governance workflows outside the schema-owner security boundary.

The command paths above are stable. Their implementation modules remain
internal and may move; external automation must not invoke
`metrka_core.pipeline.silver.manage_*` directly.


## Workspace locations

Applications that only need to locate workspace roots can call
`create_workspace_location_resolver()`. The factory returns the public
`WorkspaceLocationResolver` protocol and keeps the standard YAML adapter an
internal implementation detail. It applies the same explicit-path,
environment-variable, development fallback, and production safeguards as
pipeline execution.

`open_pipeline_context()` accepts either `workspaces_config_path` or a custom
`WorkspaceLocationResolver`. A resolver returns a `WorkspaceLocation` with a
read-oriented `definition_root` and an independently writable `data_root`.
Production runs must pass the path explicitly or set
`METRKA_WORKSPACES_CONFIG_PATH`. Development runs may use `workspaces.local.yaml`
from the current working directory.

The standard YAML adapter requires every workspace to declare one of two
placements. A portable entry declares `workspace_root` and resolves:

```text
<workspace_root>/
├── conf/                  definition_root
└── data/                  data_root
```

A managed entry declares `definition_root` and `data_root`, which may live in
different repositories, disks, mounts, or volumes. Relative paths are resolved
from the placement file. Unknown fields, incomplete placement entries, reused
roots, and missing directories fail before pipeline composition. A custom
resolver remains supported. Pipeline composition treats both placements
identically: code provenance is collected from `definition_root`; mutable
artifacts and integrity-relative paths are owned by `data_root`.

Dataset configuration belongs to the running application. `metrka-core`
never discovers it by walking upward from the installed package directory.

The resolved `definition_root` must be contained in a Git repository. Its
repository identity is collected directly from Git; `data_root` does not need
to be version controlled, and no installed distribution named
`metrka-datasets` is required. A root `pyproject.toml` may provide an optional
project name and version, but a configuration-only repository is supported.

An installed `metrka-core` wheel uses immutable provenance embedded at build
time rather than treating an enclosing consumer repository as the core source
checkout. Missing or inconsistent embedded provenance fails closed.


## Acquisition lifecycle

Acquisition is a mandatory platform-owned lifecycle phase that runs once
before configured pipeline actions.

Extensions may register custom `AssetExtractor` implementations. Capture
management, landing storage, receipts, and the standard `AcquisitionDeps`
remain owned and composed by `metrka-core`.

Extractor-specific collaborators, such as HTTP clients or provider SDKs,
should be injected into the registered extractor implementation rather than
added to `PipelineContext` or the standard `AcquisitionDeps`.
