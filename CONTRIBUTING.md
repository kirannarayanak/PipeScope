# Contributing to LineageScope

See the **[Contributing guide](https://kirannarayanak.github.io/lineagescope/contributing/)** on the documentation site (source: `docs/contributing.md`).

Quick local setup:

```bash
pip install -e ".[dev]"
pytest
ruff check lineagescope tests
```

To preview documentation:

```bash
pip install -e ".[docs]"
mkdocs serve
```
