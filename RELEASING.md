# Releasing PipeScope

## Preconditions

- `pytest` and `ruff check pipescope tests` pass.
- Version bumped in `pyproject.toml` (`[project].version`) and a new section added in `CHANGELOG.md`.
- Git working tree clean; changes merged on `main` (or your release branch).

## Build

Uses [Hatch](https://hatch.pypa.io/) via PEP 517 (`pyproject.toml` already declares `hatchling`).

```bash
python -m pip install --upgrade build hatch
python -m build
```

Artifacts appear under `dist/` (`lineagescope-<version>-py3-none-any.whl` and `.tar.gz`).

## Publish to PyPI

1. Create a [PyPI API token](https://pypi.org/manage/account/token/) with scope for this project (or use Trusted Publishing from GitHub Actions).
2. Upload:

   ```bash
   python -m pip install twine
   python -m twine upload dist/lineagescope-*
   ```

   Or with Hatch: `hatch publish` (configure credentials per [Hatch docs](https://hatch.pypa.io/latest/publish/)).

3. Tag the release:

   ```bash
   git tag -a v0.1.3 -m "Release v0.1.3"
   git push origin v0.1.3
   ```

## After publish

- Confirm `pip install lineagescope` installs the expected version (`pipescope --help` should work).
- Update any marketing or demo docs that reference install instructions.
