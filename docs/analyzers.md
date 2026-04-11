# Analyzers reference

Each analyzer contributes **findings** (with severity and category) and a **0–100 score** exposed in JSON as `scores` and inside `analytics` blocks. Higher is better except **`complexity`**, where a higher score means a heavier pipeline (more structural/SQL weight).

## Dead assets

**Intent:** Surface sinks that look unused or weakly connected.

- Uses the lineage graph: nodes with **no downstream** but **some upstream** are candidates.
- Respects **whitelist** CLI and **terminal tag** heuristics for intentional exports/dashboards.

**Categories:** `dead_asset`, and related graph metrics in `analytics.dead_asset_analysis`.

## Test coverage

**Intent:** Flag models/tables that lack tests, with extra weight when many downstream dependents exist.

- **CRITICAL** findings when downstream count exceeds `--test-coverage-critical-deps` (non-staging heuristics apply).

**Categories:** `missing_test`, `weak_test_coverage`.

## Documentation coverage

**Intent:** Compare documented vs undocumented assets (dbt descriptions, SQL leading comments, etc.).

**Categories:** `missing_documentation`.

## Complexity

**Intent:** Summarize SQL size, joins, CTE depth, and graph position (fan-in/fan-out, depth percentiles).

**Score:** Pipeline-oriented; higher = more complex (unlike other “higher is better” scores).

## Ownership

**Resolution order:**

1. **CODEOWNERS** path match (last match wins).
2. **dbt** `meta.owner` on models and source tables.
3. **Git** last commit author on the backing file.

**Categories:** `no_owner`, `stale_asset` (files with last commit older than ~6 months, when git history is available).

## Contract compliance (ODCS)

**Intent:** For each parsed contract table, find a matching asset and compare **columns** and **types**.

**Categories:** `contract_asset_not_found`, `contract_missing_column`, `contract_extra_column`, `contract_type_mismatch`.

**Score:** Compliant contracts over total contracts with column definitions.

## Cost hotspots (static SQL)

**Intent:** Flag static patterns that often correlate with cost or risk: `SELECT *`, cross joins, missing `WHERE` / `LIMIT` on large selects, missing partition filters when `partition_key` meta is set.

**Categories:** `cost_hotspot`.

Downstream node count **weights** impact in analytics rankings.

## Graph metrics (shared)

`analytics` also includes orphan counts, **cycles**, high fan-out lists, and **critical path** length from `compute_scan_analytics` / `PipelineGraph`.
