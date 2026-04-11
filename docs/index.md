---
hide:
  - navigation.path
---

<div class="lineagescope-hero" markdown="1">

# LineageScope

**Static analysis for data pipelines** — point it at a repo to discover **SQL**, **dbt**, **Airflow**, **Spark**, and **ODCS** contracts. No warehouse or cloud account required.

<ul class="lineagescope-tagline">
  <li>Lineage</li>
  <li>Quality gates</li>
  <li>CI-ready JSON</li>
</ul>

</div>

## Explore { .lineagescope-section }

<div class="lineagescope-features">
  <a class="lineagescope-card" href="getting-started/">
    <span class="lineagescope-card-ico" aria-hidden="true">🚀</span>
    <strong>Getting started</strong>
    <span class="lineagescope-card-sub">Install, first scan, JSON output, and HTML report path.</span>
  </a>
  <a class="lineagescope-card" href="configuration/">
    <span class="lineagescope-card-ico" aria-hidden="true">⚙️</span>
    <strong>Configuration</strong>
    <span class="lineagescope-card-sub">CLI flags, <code>--exclude</code>, env vars, dbt meta, CODEOWNERS.</span>
  </a>
  <a class="lineagescope-card" href="analyzers/">
    <span class="lineagescope-card-ico" aria-hidden="true">📊</span>
    <strong>Analyzers</strong>
    <span class="lineagescope-card-sub">Dead assets, tests, docs, complexity, ownership, contracts, cost hints.</span>
  </a>
  <a class="lineagescope-card" href="ci-cd/">
    <span class="lineagescope-card-ico" aria-hidden="true">✅</span>
    <strong>CI/CD</strong>
    <span class="lineagescope-card-sub"><code>lineagescope ci</code>, GitHub Action, <code>jq</code> gates, <code>diff</code>.</span>
  </a>
  <a class="lineagescope-card" href="contributing/">
    <span class="lineagescope-card-ico" aria-hidden="true">🤝</span>
    <strong>Contributing</strong>
    <span class="lineagescope-card-sub">Dev setup, tests, Ruff, and how to extend the project.</span>
  </a>
  <a class="lineagescope-card" href="changelog/">
    <span class="lineagescope-card-ico" aria-hidden="true">📜</span>
    <strong>Changelog</strong>
    <span class="lineagescope-card-sub">Release history (synced from the repo <code>CHANGELOG.md</code>).</span>
  </a>
</div>

## Features { .lineagescope-section }

- **Lineage graph** (NetworkX) — cycles, orphans, fan-out, critical path.
- **Analyzers** — dead assets, test/doc coverage, SQL/graph complexity, ownership (CODEOWNERS / dbt `meta.owner` / git), ODCS compliance, static cost-pattern hints.
- **Outputs** — Rich terminal report, **JSON** for automation, **HTML** with D3 lineage and score trends.
- **Workflows** — `.lineagescope/snapshots/`, `lineagescope diff` vs any git ref, `lineagescope ci` with thresholds and GitHub Actions annotations.

!!! tip "Repository"

    Source and issues: **[github.com/kirannarayanak/lineagescope](https://github.com/kirannarayanak/lineagescope)** · Docs are built with **MkDocs Material** and deploy to **GitHub Pages**.
