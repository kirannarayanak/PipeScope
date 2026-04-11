# Getting started

## Requirements

- Python **3.11+**

## Install

**From PyPI** (after publish):

```bash
pip install lineagescope
```

The PyPI package is **`lineagescope`**; the CLI remains **`pipescope`** (`pipescope scan …`).

**From a git checkout**:

```bash
git clone https://github.com/kirannarayanak/PipeScope.git
cd PipeScope
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## First scan

```bash
pipescope scan /path/to/your/repo
```

Terminal mode prints tables, scores, and top findings. An HTML report path is shown at the end.

**JSON** (for scripts and CI):

```bash
pipescope scan /path/to/your/repo --format json > scan.json
```

**SQL dialect** (optional, improves SQLGlot parsing):

```bash
pipescope scan . --dialect snowflake
```

**Skip heavy directories** (e.g. `node_modules`, virtualenvs):

```bash
pipescope scan . --exclude node_modules,venv,.venv,.git
```

## Other commands

| Command | Purpose |
| --- | --- |
| `pipescope diff HEAD~1` | Compare assets/findings vs another git revision (uses a temporary worktree). |
| `pipescope ci --threshold 70` | Full scan; exit `1` if blended score is below threshold; emits GitHub Actions annotations. |

Use `pipescope <command> --help` for examples on each command.

## HTML report

In terminal mode, PipeScope writes an HTML file (path shown in the footer). Open it in a browser for sortable findings, score cards, and a D3 lineage graph.

## Snapshots

Each successful scan writes under **`<scan_root>/.pipescope/snapshots/`** (daily and timestamped JSON). Set **`PIPESCOPE_SNAPSHOT_RETENTION_DAYS`** to control pruning of old timestamped files (see [Configuration](configuration.md)).
