# Getting started

## Requirements

- Python **3.11+**

## Install

**From PyPI** (after publish):

```bash
pip install lineagescope
```

The PyPI package and CLI are both **`lineagescope`** (`lineagescope scan …`).

**From a git checkout**:

```bash
git clone https://github.com/kirannarayanak/lineagescope.git
cd lineagescope
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## First scan

```bash
lineagescope scan /path/to/your/repo
```

Terminal mode prints tables, scores, and top findings. An HTML report path is shown at the end.

**JSON** (for scripts and CI):

```bash
lineagescope scan /path/to/your/repo --format json > scan.json
```

**SQL dialect** (optional, improves SQLGlot parsing):

```bash
lineagescope scan . --dialect snowflake
```

**Skip heavy directories** (e.g. `node_modules`, virtualenvs):

```bash
lineagescope scan . --exclude node_modules,venv,.venv,.git
```

## Other commands

| Command | Purpose |
| --- | --- |
| `lineagescope diff HEAD~1` | Compare assets/findings vs another git revision (uses a temporary worktree). |
| `lineagescope ci --threshold 70` | Full scan; exit `1` if blended score is below threshold; emits GitHub Actions annotations. |

Use `lineagescope <command> --help` for examples on each command.

## HTML report

In terminal mode, LineageScope writes an HTML file (path shown in the footer). Open it in a browser for sortable findings, score cards, and a D3 lineage graph.

## Snapshots

Each successful scan writes under **`<scan_root>/.lineagescope/snapshots/`** (daily and timestamped JSON). Set **`LINEAGESCOPE_SNAPSHOT_RETENTION_DAYS`** (legacy: `PIPESCOPE_SNAPSHOT_RETENTION_DAYS`) to control pruning of old timestamped files (see [Configuration](configuration.md)).
