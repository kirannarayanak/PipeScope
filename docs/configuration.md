# Configuration

## CLI: `pipescope scan`

| Option | Description |
| --- | --- |
| `path` | Directory to scan (default: `.`). |
| `-d`, `--dialect` | SQLGlot dialect (`snowflake`, `bigquery`, `postgres`, `duckdb`, …). |
| `-f`, `--format` | `terminal` (default) or `json`. |
| `-e`, `--exclude` | Comma- or semicolon-separated **directory names** to prune while walking (case-insensitive). Hidden directories (`.*`) are always skipped. |
| `--dead-asset-whitelist` | Comma-separated asset names excluded from dead-asset analysis. |
| `--dead-asset-terminal-tags` | Comma-separated substrings matching tag keys/values for intentional sinks (default behavior uses `exposure`, `dashboard`, `export`; pass empty string to disable). |
| `--test-coverage-critical-deps` | Downstream threshold for CRITICAL missing-test findings (default: `10`). |

`pipescope diff` and `pipescope ci` accept **`--path`**, **`--dialect`**, and **`--exclude`** where applicable.

## Environment variables

| Variable | Used by | Description |
| --- | --- | --- |
| `PIPESCOPE_SNAPSHOT_RETENTION_DAYS` | `scan`, `ci` | Days to keep **timestamped** snapshot files under `.pipescope/snapshots/` (daily `YYYY-MM-DD.json` files are not pruned by age). Invalid values fall back to `30`. |
| `GITHUB_TOKEN` | `ci` | If set in GitHub Actions with `GITHUB_REF` pointing at a PR, PipeScope may post a short score comment (optional). |
| `GITHUB_REF`, `GITHUB_REPOSITORY` | `ci` | Used together with `GITHUB_TOKEN` for PR comment detection. |
| `PYTHONUTF8` | CLI (Windows) | Set to `1` for reliable UTF-8 in consoles (PipeScope also tries to reconfigure stdio on Windows). |

## Parse warnings

If a file cannot be read or a parser raises, PipeScope **skips** that file and records a message in **`parse_warnings`** (JSON) or a yellow panel (terminal). The scan continues.

## dbt ownership and partitions

In **`schema.yml`** (or other dbt schema files PipeScope reads):

- **`meta.owner`** on models and source tables feeds the ownership analyzer.
- **`meta.partition_key`** tags models/tables for partition-aware cost checks.
- Column **`data_type`** improves contract and typing alignment.

## CODEOWNERS

Place **`.github/CODEOWNERS`** or **`CODEOWNERS`** at the repo root (or under the scan root). GitHub-style globs apply; last matching line wins.

## Data contracts (ODCS)

YAML files under the scan tree that look like ODCS (e.g. `dataContractSpecification`, `kind: DataContract`, top-level `schema:` lists) are matched to assets by name and compared for columns/types.
