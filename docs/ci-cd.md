# CI/CD integration

## `lineagescope ci`

Runs a full scan (same pipeline as `scan`), then:

1. Prints an overall blended **score** (average of dimension scores, with complexity inverted).
2. Emits **GitHub Actions workflow commands** for each finding (`::error::`, `::warning::`, `::notice::`).
3. Optionally posts a short **PR comment** when `GITHUB_TOKEN`, `GITHUB_REPOSITORY`, and a PR ref are present.
4. Exits with code **`1`** if overall score **&lt; `--threshold`** (default `70`).

```bash
lineagescope ci --threshold 70 --path .
lineagescope ci --path ./transforms --dialect bigquery --exclude .venv,venv
```

## Composite GitHub Action

The repo ships **`action.yml`** at the root. Example workflow step:

```yaml
- uses: kirannarayanak/lineagescope@main
  with:
    path: .
    threshold: "70"
    dialect: ""
    exclude: "node_modules,venv"
```

Inputs: `path`, `threshold`, `dialect`, `exclude`.

### GitHub Marketplace

To list a composite action on the [Marketplace](https://github.com/marketplace?type=actions), GitHub currently expects a **thin repository** that contains **only** the action (root `action.yml` plus minimal files such as a README)—**not** a monorepo that also defines workflow files under `.github/workflows/`. This repo keeps CI/docs workflows here, so it is consumed via `uses: owner/Lineagescope@vX` from releases or branches. If you want a Marketplace listing, publish the same `action.yml` (and branding metadata) from a **dedicated** public repository with no workflow files, then [draft a release](https://docs.github.com/en/actions/sharing-automations/creating-actions/publishing-actions-in-github-marketplace) and check **Publish this action to the GitHub Marketplace** (requires the Marketplace developer agreement and 2FA).

## JSON gates with `jq`

LineageScope does not fail on findings alone; gate on **`scores`** or **finding categories**:

```bash
lineagescope scan . --format json | jq -e '.scores.contracts >= 90'
```

```bash
lineagescope scan . --format json | jq -e '[.findings[] | select(.category | test("^contract_"))] | length == 0'
```

Write JSON once, run multiple checks:

```bash
lineagescope scan . --format json > lineagescope-scan.json
jq -e '.scores.dead_assets >= 80' lineagescope-scan.json
```

## Snapshots in CI

Scans write **`.lineagescope/snapshots/`** under the scan root. In CI, either **persist** that directory as an artifact for trends or **gitignore** it if you only need pass/fail.

## `lineagescope diff`

Use locally or in a job with git history to compare **current** assets/findings vs **`HEAD~1`**, **`main`**, or any ref:

```bash
lineagescope diff HEAD~1 --path . --exclude node_modules
```
