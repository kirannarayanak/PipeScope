# Contributing

Thank you for helping improve LineageScope.

## Development setup

```bash
git clone https://github.com/kirannarayanak/lineagescope.git
cd lineagescope
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

## Checks

```bash
ruff check lineagescope tests
pytest
```

Optional: `pre-commit run --all-files`.

## Documentation site

Build and preview locally:

```bash
pip install -e ".[docs]"
mkdocs serve
```

`mkdocs serve` **keeps running** until you stop it (**Ctrl+C**). That is normal—it is a live dev server, not a hang.

Because `site_url` targets GitHub **Project** Pages (`…/lineagescope/`), the default preview URL is:

`http://127.0.0.1:8000/lineagescope/`

To preview at the site root instead (`http://127.0.0.1:8000/`):

```powershell
# PowerShell
$env:LINEAGESCOPE_DOCS_SITE_URL = "http://127.0.0.1:8000/"
mkdocs serve
```

```bash
# bash
LINEAGESCOPE_DOCS_SITE_URL=http://127.0.0.1:8000/ mkdocs serve
```

One-off static build: `mkdocs build` (output in `site/`).

**Theming:** global styles live in `docs/stylesheets/extra.css`; the header logo is `docs/assets/logo.svg`. Edit `mkdocs.yml` (`theme.palette`, `theme.font`, `extra_css`) for larger visual changes.

Publish is handled by **GitHub Actions** (see `.github/workflows/docs.yml`) to **GitHub Pages** when `main` updates (CI does **not** set `LINEAGESCOPE_DOCS_SITE_URL`, so production keeps the real `site_url`).

## Pull requests

- Keep changes focused; match existing style (Ruff, types, short docstrings on public APIs).
- Add or update **tests** under `tests/` for behavior changes.
- Update **user-facing docs** in `docs/` when CLI behavior or analyzers change.
- Update **`CHANGELOG.md`** under an appropriate version section.

## Project layout

| Path | Role |
| --- | --- |
| `lineagescope/cli.py` | Typer CLI, scan orchestration. |
| `lineagescope/scanner.py` | Repository walk and file classification. |
| `lineagescope/parsers/` | SQL, dbt, Airflow, Spark, ODCS parsing. |
| `lineagescope/analyzers/` | Rules and scores. |
| `lineagescope/graph.py` | NetworkX lineage graph. |
| `lineagescope/reporters/` | Terminal, JSON, HTML. |
| `tests/` | Pytest suite. |

## License

By contributing, you agree your contributions are licensed under the same terms as the project (**MIT**).
