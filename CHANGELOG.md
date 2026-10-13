# Changelog

All notable changes to `metrka-core` are documented in this file.

The project follows [Semantic Versioning](https://semver.org/). Internal
modules remain implementation details unless they are listed in
`PUBLIC_API.md`.


## [1.0.0] - 2026-10-13

First public release.

### Pipeline

- Bronze and Silver layers for public source files. Acquisition extractors
  (built-in `http.files`, or a dataset package's own) land each source capture
  with a receipt; Bronze keeps the received files and their fingerprints and
  checks ZIP and XLSX integrity; XLSX files of one stream can be assembled into
  one CSV with metadata taken from file names.
- Silver tables are built from YAML contracts: renames, casts, value
  normalization with reasons, and parent-child reconciliation that removes a
  total row and publishes any positive difference as an `Unallocated` row.
  Every change is recorded as transformation evidence that names its rows.
- Each build has a version period, optional temporal coverage and catalog
  highlights, CSV and Parquet outputs, a manifest, and an immutable contract
  snapshot.

### Configuration

- Portable and managed workspaces registered in one YAML registry; a source can
  keep each dataset in its own folder (`layout: dataset_folders`).
- A stream names its contract `<stream>.yaml` and its Bronze file
  `<stream>.csv` unless told otherwise; validation checks every name that
  `main.yaml`, the contract and `quality.yaml` share.
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

### Operations

- One PostgreSQL metadata schema created by a single initial migration, with
  least-privilege roles for the schema owner, migrator, pipelines, the web
  catalog and operators.
- `metrka run`, `metrka workspace validate|init|export|import` and
  `metrka operations` command-line interfaces, and the public Python API in
  `metrka_core.api` described in `PUBLIC_API.md`.
