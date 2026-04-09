# PipeScope

Universal static analyzer for data pipelines. Point it at a Git repository to analyze SQL, dbt, Airflow, Spark, and data contracts without a database or cloud account.

## Requirements

- Python 3.11+

## Setup

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
# source venv/bin/activate

pip install -e ".[dev]"
pre-commit install
```

## Development

Run the test suite and linter locally:

```bash
pytest
ruff check pipescope tests
```

Optional: `pre-commit run --all-files` runs the same hooks as on commit (Ruff, YAML/TOML checks, whitespace).

### Windows terminal

On Windows, PipeScope reconfigures stdout/stderr to UTF-8 when supported so Rich tables and paths render correctly. For best results, use **Windows Terminal** or **PowerShell 7+**; you can also set `PYTHONUTF8=1` in the environment or run `chcp 65001` in legacy consoles.

## Usage

```bash
pipescope --help
pipescope scan .
pipescope scan path/to/repo --dialect postgres
pipescope scan . --format json
```

Analyzer tuning (optional):

```bash
pipescope scan . --dead-asset-whitelist my_export,legacy_sink
pipescope scan . --test-coverage-critical-deps 15
```

### JSON output (`--format json`)

Top-level keys include:

| Key | Purpose |
| --- | --- |
| `assets`, `edges` | Parsed inventory and lineage (`source` → `target`) |
| `graph` | `node_count`, `edge_count`, `is_directed_acyclic` |
| `analytics` | Graph metrics, `dead_asset_analysis`, `test_coverage` / `test_coverage_analysis`, `documentation_coverage` / `documentation_coverage_analysis` |
| `findings` | Combined `dead_asset`, `missing_test`, `weak_test_coverage`, … |
| `scores` | `dead_assets`, `test_coverage`, and `documentation` scores (0–100; higher is better for coverage/docs) |

Use this shape for CI gates and dashboards.

## CI

On GitHub, [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs **Ruff** (`ruff check .`) and **pytest** with coverage (`pytest --cov=pipescope`) on Python **3.11** for every **push** and **pull_request**.

## License

MIT. See [LICENSE](LICENSE).
