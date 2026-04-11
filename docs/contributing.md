# Contributing

Thank you for helping improve PipeScope.

## Development setup

```bash
git clone https://github.com/kirannarayanak/PipeScope.git
cd PipeScope
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

## Checks

```bash
ruff check pipescope tests
pytest
```

Optional: `pre-commit run --all-files`.

## Documentation site

Build the MkDocs site locally:

```bash
pip install -e ".[docs]"
mkdocs serve
```

Publish is handled by **GitHub Actions** (see `.github/workflows/docs.yml`) to **GitHub Pages** when `main` updates.

## Pull requests

- Keep changes focused; match existing style (Ruff, types, short docstrings on public APIs).
- Add or update **tests** under `tests/` for behavior changes.
- Update **user-facing docs** in `docs/` when CLI behavior or analyzers change.
- Update **`CHANGELOG.md`** under an appropriate version section.

## Project layout

| Path | Role |
| --- | --- |
| `pipescope/cli.py` | Typer CLI, scan orchestration. |
| `pipescope/scanner.py` | Repository walk and file classification. |
| `pipescope/parsers/` | SQL, dbt, Airflow, Spark, ODCS parsing. |
| `pipescope/analyzers/` | Rules and scores. |
| `pipescope/graph.py` | NetworkX lineage graph. |
| `pipescope/reporters/` | Terminal, JSON, HTML. |
| `tests/` | Pytest suite. |

## License

By contributing, you agree your contributions are licensed under the same terms as the project (**MIT**).
