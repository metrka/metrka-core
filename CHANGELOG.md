# Changelog

All notable changes to `metrka-core` are documented in this file.

The project follows [Semantic Versioning](https://semver.org/). Internal
modules remain implementation details unless they are listed in
`PUBLIC_API.md`.


## [1.0.0] - 2026-10-18

First release.

### Pipeline

- Bronze and Silver layers for public source files. Acquisition extractors
  (built-in `http.files`, or a dataset package's own) land each source capture
  with a receipt; Bronze keeps the received files and their fingerprints and
  checks ZIP and XLSX integrity.
- XLSX files of one stream are assembled into one CSV with metadata taken from
  their file names. With `bronze_assembly.group_files_by` they are sorted,
  whole, into one CSV per value of a filename metadata column, each feeding the
  contract table named like it, so one stream can publish tables of different
  shapes. An output written as `skip` leaves that value's files out of Bronze
  before the batch is fingerprinted, so giving the value a CSV later changes the
  batch and Bronze builds it again.
- Silver tables are built from YAML contracts: renames, casts (an `int` cast
  accepts a whole number written as `30.0`), value normalization with reasons,
  and parent-child reconciliation that removes a total row and publishes any
  positive difference as an `Unallocated` row with the table's column types.
  `missing_child_values: allow` reconciles a source that leaves small counts
  blank. Every change is recorded as transformation evidence that names its
  rows.
- Each build has a version period, optional temporal coverage and catalog
  highlights, CSV and Parquet outputs (CSV lines end with `\n` on every
  operating system), a manifest, and an immutable contract snapshot.

### Configuration

- Portable and managed workspaces registered in one YAML registry; a source can
  keep each dataset in its own folder (`layout: dataset_folders`).
- A stream names its contract `<stream>.yaml` and its Bronze file
  `<stream>.csv` unless told otherwise; validation checks every name that
  `main.yaml`, the contract and `quality.yaml` share, including that every
  grouped Bronze output feeds a contract table and every table gets an output.
- Table profiles share a table's structure between contracts, with measures a
  contract may rename; meta profiles share what every dataset of a source says
  about itself. Snapshots keep the expanded contract.
- `main.yaml` templates: a dataset's `main.yaml` names a template with `uses`
  and fills its inputs with `with`, so datasets downloaded and built the same
  way share one configuration.
- A shared glossary: contract texts mark defined words as `[[term]]`, and each
  snapshot records the definitions it used.

### Quality and governance

- `quality.yaml` rules (`not_null`, `unique`, `min`, `max`, `between`,
  `allowed`, `forbidden`, `pattern`) run at the Bronze and Silver quality
  gates with blocking or warning severity; descriptions name the check's own
  settings.
- Publication candidates are approved, rejected and published by their ID;
  Silver engine releases are approved before production builds; published
  assets are verified and reconciled with `metrka operations`.

### Catalog

- The catalog keeps a dataset's tags in the order its contract lists them, with
  the text the contract wrote.

### Operations

- One PostgreSQL metadata schema created by a single initial migration,
  `0001_initial`, with least-privilege roles for the schema owner, migrator,
  pipelines, the web catalog and operators.
- `metrka run`, `metrka workspace validate|init|export|import` and
  `metrka operations` command-line interfaces, and the public Python API in
  `metrka_core.api` described in `PUBLIC_API.md`.
- The installed end-to-end smoke runs for both workspace placements and both
  layouts.
