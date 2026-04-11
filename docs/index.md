---
hide:
  - navigation.path
---

<div class="pipescope-hero" markdown="1">

# PipeScope

**Static analysis for data pipelines** — point it at a repo to discover **SQL**, **dbt**, **Airflow**, **Spark**, and **ODCS** contracts. No warehouse or cloud account required.

<ul class="pipescope-tagline">
  <li>Lineage</li>
  <li>Quality gates</li>
  <li>CI-ready JSON</li>
</ul>

</div>

## Explore { .pipescope-section }

<div class="pipescope-features">
  <a class="pipescope-card" href="getting-started/">
    <span class="pipescope-card-ico" aria-hidden="true">🚀</span>
    <strong>Getting started</strong>
    <span class="pipescope-card-sub">Install, first scan, JSON output, and HTML report path.</span>
  </a>
  <a class="pipescope-card" href="configuration/">
    <span class="pipescope-card-ico" aria-hidden="true">⚙️</span>
    <strong>Configuration</strong>
    <span class="pipescope-card-sub">CLI flags, <code>--exclude</code>, env vars, dbt meta, CODEOWNERS.</span>
  </a>
  <a class="pipescope-card" href="analyzers/">
    <span class="pipescope-card-ico" aria-hidden="true">📊</span>
    <strong>Analyzers</strong>
    <span class="pipescope-card-sub">Dead assets, tests, docs, complexity, ownership, contracts, cost hints.</span>
  </a>
  <a class="pipescope-card" href="ci-cd/">
    <span class="pipescope-card-ico" aria-hidden="true">✅</span>
    <strong>CI/CD</strong>
    <span class="pipescope-card-sub"><code>pipescope ci</code>, GitHub Action, <code>jq</code> gates, <code>diff</code>.</span>
  </a>
  <a class="pipescope-card" href="contributing/">
    <span class="pipescope-card-ico" aria-hidden="true">🤝</span>
    <strong>Contributing</strong>
    <span class="pipescope-card-sub">Dev setup, tests, Ruff, and how to extend the project.</span>
  </a>
  <a class="pipescope-card" href="changelog/">
    <span class="pipescope-card-ico" aria-hidden="true">📜</span>
    <strong>Changelog</strong>
    <span class="pipescope-card-sub">Release history (synced from the repo <code>CHANGELOG.md</code>).</span>
  </a>
</div>

## Features { .pipescope-section }

- **Lineage graph** (NetworkX) — cycles, orphans, fan-out, critical path.
- **Analyzers** — dead assets, test/doc coverage, SQL/graph complexity, ownership (CODEOWNERS / dbt `meta.owner` / git), ODCS compliance, static cost-pattern hints.
- **Outputs** — Rich terminal report, **JSON** for automation, **HTML** with D3 lineage and score trends.
- **Workflows** — `.pipescope/snapshots/`, `pipescope diff` vs any git ref, `pipescope ci` with thresholds and GitHub Actions annotations.

!!! tip "Repository"

    Source and issues: **[github.com/kirannarayanak/PipeScope](https://github.com/kirannarayanak/PipeScope)** · Docs are built with **MkDocs Material** and deploy to **GitHub Pages**.
