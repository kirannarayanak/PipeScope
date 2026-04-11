# Changelog

All notable changes to this project are documented in this file.

## 0.1.1 — 2026-04-10

### Added

- README: CI badge, quickstart, Mermaid architecture, embedded demo GIF; `docs/demo/pipescope-demo.cast` + `scripts/generate_demo_gif.py` (Pillow) for regenerating the GIF; expanded `docs/demo/README.md`.
- CLI: Rich **progress** during terminal `scan`; **`--exclude`** / **`-e`** on `scan`, `diff`, and `ci` (directory-name pruning); **`epilog`** examples on subcommands; **`parse_warnings`** in JSON and a terminal panel when parsers skip files.
- GitHub Action **`exclude`** input.

### Changed

- File discovery uses `os.walk` with pruned directories (hidden dirs skipped; excludes case-insensitive). dbt project parsing respects the same exclude segments under model paths.

## 0.1.0 — 2026-04-10

### Added

- CLI: `pipescope scan` with Rich terminal report, JSON, and HTML (Jinja2 + D3 lineage/trends).
- Parsers: dbt (`dbt_project.yml`, schema YAML), standalone SQL (SQLGlot), Airflow DAGs, Spark job hints, ODCS-style data contracts.
- Analyzers: dead assets, test/documentation coverage, complexity, ownership (CODEOWNERS / dbt `meta.owner` / git), contract compliance, static SQL cost hotspots.
- Snapshots under `.pipescope/snapshots/` for trends; `pipescope diff` vs a git ref; `pipescope ci` with threshold, GitHub Actions annotations, optional PR comment; composite `action.yml`.
- Real-world validation harness: `real-project-tests/run_validation.py` and committed compact summaries in `real-project-tests/results/`.
