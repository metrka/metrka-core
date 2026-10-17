# Changelog

All notable changes to `metrka-core` are documented in this file.

The project follows [Semantic Versioning](https://semver.org/). Internal
modules remain implementation details unless they are listed in
`PUBLIC_API.md`.


## [Unreleased]

### Bronze

- A stream's XLSX files can be sorted, whole, into several Bronze CSVs by one
  filename metadata value: `bronze_assembly.group_files_by` names the column and
  `outputs` maps each value to its CSV, for example
  `{crude: lung_cancer_incidence.csv, age_adjusted: lung_cancer_incidence_age_adjusted.csv}`.
  Each CSV feeds the contract table named like it, so one dataset can publish
  tables of different shapes from one stream, as a ZIP stream already could. No
  row is split. A file whose value no output names, an output that gets no file,
  and differing columns within one output are errors, and nothing is written
  when any group fails. Without `group_files_by` assembly is unchanged.
- validate checks that every grouped output feeds a contract table and every
  table gets an output.

### Silver

- `parent_child_reconciliation.missing_child_values: allow` reconciles a source
  that leaves small counts blank. Such rows stay as given, the counts shown are
  summed, and the residual row holds what the rows do not show. A group with no
  value at all, the parent row included, stays as given with no residual. The
  default, `fail`, still stops a build on a missing child value. The
  reconciliation evidence records `missing_child_count`.

### Catalog

- The catalog keeps a dataset's tags in the order its contract lists them, with
  the text the contract wrote: `catalog.dataset_tag_memberships` has
  `tag_position` and `tag_label`. Until now the tags were kept as a set and read
  back alphabetically by slug. Tags that give one slug are still one tag; the
  first keeps its place and text.
- Metadata migration `0002_dataset_tag_positions` adds the two columns. Run
  `python -m metrka_core.metadata.migrations upgrade` before the pipeline. Tags
  already in the catalog keep their alphabetical order and have no label until
  the dataset's next Silver build writes its contract order.

### Fixed

- Silver CSV files end every line with `\n` on every operating system. Built on
  Windows they ended lines with `\r\n`, so the same source capture gave CSV files
  with different checksums on Windows and Linux; the data itself was the same.
  Parquet files were not affected.


## [1.1.0] - 2026-10-14

### Configuration

- A managed workspace can keep each dataset in its own folder
  (`layout: dataset_folders`), as a portable one already could. A dataset's
  definitions are in its folder under `definition_root`, and its data in a folder
  of the same name under `data_root`. The source's `data_root` must exist; each
  dataset's own data folder is created by its first run.

### Release

- The installed end-to-end smoke takes `-WorkspaceLayout flat|dataset_folders`,
  and the release runs it for both layouts of both placements.
- The smoke command in `RELEASING.md` is readable again; two of its path
  separators had been saved as control characters.


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
