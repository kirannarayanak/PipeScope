# PipeScope

**PipeScope** is a static analyzer for data pipelines. Point it at a repository to discover **SQL**, **dbt**, **Airflow**, **Apache Spark** jobs, and **Open Data Contract Standard (ODCS)** YAML—without a warehouse or cloud credentials.

## Features

- **Lineage graph** (NetworkX) with cycle detection, orphans, and fan-out.
- **Analyzers**: dead assets, test and documentation coverage, SQL/graph complexity, ownership (CODEOWNERS / dbt `meta.owner` / git), contract compliance, static cost-pattern hints.
- **Outputs**: Rich terminal report, **JSON** for CI, **HTML** with D3 lineage and trends.
- **Workflows**: snapshots, `pipescope diff` vs a git ref, `pipescope ci` with thresholds and GitHub Actions annotations.

## Quick links

- [Getting Started](getting-started.md) — install and first scan.
- [Configuration](configuration.md) — CLI flags, excludes, environment variables.
- [Analyzers reference](analyzers.md) — what each check does.
- [CI/CD integration](ci-cd.md) — `ci`, composite Action, `jq` gates.
- [Contributing](contributing.md) — dev setup and guidelines.

The canonical **changelog** is inlined on the [Changelog](changelog.md) page (sourced from the repo root `CHANGELOG.md`).

Repository: [github.com/kirannarayanak/PipeScope](https://github.com/kirannarayanak/PipeScope).
