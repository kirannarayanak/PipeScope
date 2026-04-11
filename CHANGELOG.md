# Changelog

All notable changes to this project are documented in this file.

## 0.1.2 — 2026-04-11

### Added

- **MkDocs** site under `docs/` (Getting Started, Configuration, Analyzers, CI/CD, Contributing, Changelog via snippet).
- **GitHub Pages** workflow **`.github/workflows/docs.yml`** (build + deploy on `main`).
- **`[docs]`** optional dependency (`mkdocs`, `mkdocs-material`, `pymdown-extensions`).
- **`CONTRIBUTING.md`** pointing to the docs site; **Documentation** URL in `pyproject.toml` and README badge.

### Changed

- Expanded module and public API docstrings (`pipescope`, `scanner`, `graph` helpers, `parse_file`, `parse_dbt_project`, reporters, terminal/HTML helpers).

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
