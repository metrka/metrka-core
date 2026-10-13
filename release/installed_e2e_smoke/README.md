# Installed Metrka end-to-end smoke

This smoke test proves that the built `metrka-core` wheel works without importing
`metrka_core` from the source checkout. It uses the public `metrka` command, a small
workspace kept in this folder, a local HTTP server as its source, and a disposable
PostgreSQL container.

## Preconditions

- `metrka-core` is on a clean local `main` branch.
- Python 3.12 or newer is available as `python`.
- Docker Desktop is running and Linux containers are enabled.
- Internet access is available for Python dependencies and the PostgreSQL image.

Nothing outside this repository is needed. The script creates its build and
installation virtual environments, the workspace copy, and the source server below
`$env:TEMP`. It does not use the active development virtual environment or any
existing database. The Python helpers are separate files so the checks work in both
Windows PowerShell 5.1 and PowerShell 7 without the quoting problems of multiline
`python -c` commands.

## Run

```powershell
& .\release\installed_e2e_smoke\run-installed-e2e-smoke.ps1
```

The default run exercises a portable workspace. Run the identical installed workflow
with definitions and data on separate roots using:

```powershell
& .\release\installed_e2e_smoke\run-installed-e2e-smoke.ps1 -WorkspacePlacement managed
```

`-WorkspaceLayout dataset_folders` runs the same workflow with the workspace as a
source that keeps its dataset in its own folder, as production sources do. Combine it
with either placement:

```powershell
& .\release\installed_e2e_smoke\run-installed-e2e-smoke.ps1 -WorkspacePlacement managed -WorkspaceLayout dataset_folders
```

Options:

- `-WorkspacePlacement` is `portable` (default) or `managed`;
- `-WorkspaceLayout` is `flat` (default) or `dataset_folders`;
- `-PostgresImage` picks the PostgreSQL image, `postgres:17` by default;
- `-KeepDatabase` leaves the container running for inspection and prints how to
  remove it;
- `-KeepFiles` keeps the temporary folder after a passing run. A failing run always
  keeps it and prints its path.

## The smoke workspace

`fixture/smoke_counties` is a flat workspace with one dataset,
`smoke_counties.county_events`:

- `conf/main.yaml` names the template `templates/county_csv.yaml` and gives it the
  source address. The file holds the marker `SMOKE_SOURCE_URL`; the script replaces it
  with its local server's address before the run.
- `conf/contract.yaml` takes its table from the profile `profiles/county_events.yaml`,
  renames `Count` to `event_count`, and takes its source from `profiles/smoke_source.yaml`.
- `glossary.yaml` defines the `[[event]]` term the contract uses.

`fixture/source/county_events.csv` holds made-up counts for three counties and a
`Statewide` total over two years. In 2024 the total is 3 higher than the counties, so
Silver gets one `Unallocated` row; in 2025 they match.

The script copies the workspace below `$env:TEMP` and commits the copy to a new Git
repository, because every run records the revision of its workspace.

With `-WorkspaceLayout dataset_folders` the copy becomes the source `smoke_counties`:
`conf/` moves into its dataset folder `county_events/`, while the profiles, the
template and the glossary stay beside it. The run, validation and publication then
use the dataset ID `smoke_counties.county_events`, and a managed run keeps the data
in `county_events/` under the source's `data_root`. The customer export round trip
is skipped in this layout: a package holds one flat workspace.

## What is verified

1. `metrka-core` is clean and on `main`.
2. Build provenance is generated from the clean checkout.
3. A wheel is built and audited for provenance, Apache-2.0 metadata, the exact
   non-empty license text, and excluded scratch modules.
4. The wheel is installed in a clean virtual environment.
5. `metrka_core` resolves from `site-packages`, not the source checkout.
6. The roles are provisioned with the script shipped inside the wheel, and all
   migrations shipped inside the wheel apply and pass `check`.
7. `metrka workspace validate smoke_counties` succeeds: the template, profiles and
   glossary are filled in.
8. The first `metrka run smoke_counties` downloads the source and creates Bronze and
   Silver state.
9. The Silver table has the profile's columns with the renamed measure, no
   `Statewide` row, and the `Unallocated` row. The contract snapshot keeps the
   expanded profiles and the glossary.
10. The initial publication candidate is approved and published through the installed
    operations CLI.
11. Current and history projections exist and reconciliation succeeds.
12. A second run is idempotent: it creates another successful pipeline receipt but no
    additional marshaled file, Silver build, candidate, or publication.
13. The customer workspace ZIP passes independent manifest, membership, path, size,
    and checksum verification. Steps 13 to 15 run in the flat layout only.
14. `metrka workspace import` verifies the ZIP again, extracts it below a new root,
    and registers the reopened portable workspace using only the installed CLI. The
    reopened workspace validates, so the export carried its template, profiles and
    glossary.
15. Extracted contract definitions, contract snapshots, Silver manifests, latest
    pointers, SQL views, CSV, and Parquet files resolve from their new roots. Recorded
    checksums and table shapes still match, and no text artifact contains the original
    workspace's absolute path.
16. With `-WorkspacePlacement managed`, Bronze, Silver, receipts, snapshots,
    manifests, pointers, and views are created below the detached `data_root`. The
    Git-backed `definition_root` remains clean and contains no runtime `data` or
    `logs` directory.

Successful completion ends with `Installed end-to-end smoke passed` and the tested
placement and layout in parentheses.
