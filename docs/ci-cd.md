# CI/CD integration

## `pipescope ci`

Runs a full scan (same pipeline as `scan`), then:

1. Prints an overall blended **score** (average of dimension scores, with complexity inverted).
2. Emits **GitHub Actions workflow commands** for each finding (`::error::`, `::warning::`, `::notice::`).
3. Optionally posts a short **PR comment** when `GITHUB_TOKEN`, `GITHUB_REPOSITORY`, and a PR ref are present.
4. Exits with code **`1`** if overall score **&lt; `--threshold`** (default `70`).

```bash
pipescope ci --threshold 70 --path .
pipescope ci --path ./transforms --dialect bigquery --exclude .venv,venv
```

## Composite GitHub Action

The repo ships **`action.yml`** at the root. Example workflow step:

```yaml
- uses: kirannarayanak/PipeScope@main
  with:
    path: .
    threshold: "70"
    dialect: ""
    exclude: "node_modules,venv"
```

Inputs: `path`, `threshold`, `dialect`, `exclude`.

## JSON gates with `jq`

PipeScope does not fail on findings alone; gate on **`scores`** or **finding categories**:

```bash
pipescope scan . --format json | jq -e '.scores.contracts >= 90'
```

```bash
pipescope scan . --format json | jq -e '[.findings[] | select(.category | test("^contract_"))] | length == 0'
```

Write JSON once, run multiple checks:

```bash
pipescope scan . --format json > pipescope-scan.json
jq -e '.scores.dead_assets >= 80' pipescope-scan.json
```

## Snapshots in CI

Scans write **`.pipescope/snapshots/`** under the scan root. In CI, either **persist** that directory as an artifact for trends or **gitignore** it if you only need pass/fail.

## `pipescope diff`

Use locally or in a job with git history to compare **current** assets/findings vs **`HEAD~1`**, **`main`**, or any ref:

```bash
pipescope diff HEAD~1 --path . --exclude node_modules
```
