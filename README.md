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

## Usage

```bash
pipescope --help
```

## License

MIT. See [LICENSE](LICENSE).
