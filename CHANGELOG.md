# Changelog

All notable changes to `metrka-core` are documented in this file.

The project follows [Semantic Versioning](https://semver.org/). Internal
modules remain implementation details unless they are listed in
`PUBLIC_API.md`.


## [Unreleased]

### Configuration

- Configuration YAML files (contracts, profiles, `main.yaml` and its templates,
  `quality.yaml`, the glossary, the workspace registry and the metadata
  database config) are refused when a mapping repeats a key. Before, the last
  value silently replaced the earlier ones, so `metrka workspace validate`
  passed a contract with a block written twice.
- A dataset folder may keep streams with `artifact_role: documentation` beside
  its data stream, such as the source's own description of the file, captured
  with every download. The data stream must still be the one named like the
  folder, and a second data stream is still refused.
- A stream of JSON files may list `duplicate_check.ignore_json_keys`, dotted
  keys such as `viewCount` whose values change without the file changing.
  Bronze then finds an earlier copy of the file by a hash that sets those keys
  to null and ignores layout; the file is still kept and verified byte for
  byte, and its record keeps both hashes and the ignored keys.
- `normalize_values` accepts `replace_with: null`, which publishes the value as
  missing, such as a source's `Suppressed` in a count column that is cast to
  `int`. The change is recorded like any other replacement, with its reason
  and an empty published value. Contracts that use it are refused by 1.1.0.
  The Silver engine hash changes, so approve the new engine release; every
  Silver dataset then rebuilds once with unchanged data.
- A contract column of type `string` may be split at a separator into columns
  of their own: `split: {separator: "/", into: {race: {cast_to: string},
  sex: {cast_to: string}}}`. Each part has its own `cast_to` and may have
  `format_in`, `case` and `meta`, and quality rules and total reconciliation
  may name it. The column itself is still published and listed in
  `canonical_order` with its parts, unless `exclude_column: true` leaves it
  out (the default is false). A present value that does not split into
  exactly as many pieces as there are parts stops the table, and the error
  names how many values failed with examples. Pieces are trimmed, an empty
  piece is missing, and a missing value leaves every part missing. The split
  is recorded as `column_split` evidence with its separator, parts and counts.
  Contracts that use it are refused by 1.1.0, and published versions that use
  it need a catalog that knows the split parts and the `column_split`
  evidence kind.

### Pipeline

- A pipeline without a `silver.process` step no longer checks the table rules
  of `quality.yaml` against contracts. Before, a dataset whose `quality.yaml`
  already had rules for its Silver table could not run Bronze alone: with the
  Silver step left out no contract was read, and the run stopped with
  "quality.yaml names table ..., which no contract defines". The rules are
  checked again once the Silver step is back.
- `pipeline.steps: []` runs acquisition only: the source is captured with its
  receipt and the capture is registered in the metadata database, and no
  Bronze or Silver step runs. Before, `steps` had to name at least one action.
  A `steps` value that is not a list is still refused.
- A failed column rule in `quality.yaml` now says what the failing rows do,
  such as "3 row(s) where hospitalizations_count is missing". Before, it
  repeated the rule after "breaks:" ("breaks: has no missing values"), which
  read as the opposite. The check, its `expected` record and a passed check's
  text are unchanged; earlier runs keep their old text.
- The Bronze execution step that assembles a stream's files is recorded as
  `assemble_source_batch` instead of `assemble_xlsx_batch`. Earlier runs keep
  the old name in the execution log.


## [1.1.0] - 2026-10-27

### Configuration

- A table profile may list source columns as `retypable`; a contract table
  that uses the profile may then set `cast_to` on those columns. Other columns
  keep the profile's type, and every retypable column must be in the profile's
  table. Contracts written for 1.0.0 work unchanged; a profile with
  `retypable` is refused by 1.0.0.


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
