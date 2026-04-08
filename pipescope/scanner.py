"""Repository walker and file discovery."""

from __future__ import annotations

from pathlib import Path


def discover_repo(root: Path) -> list[Path]:
    """Walk *root* and return candidate files for parsing (stub).

    Phase 1 will classify SQL, dbt, Airflow, Spark, and contract files.
    """
    root = root.resolve()
    if not root.is_dir():
        return []
    return sorted(p for p in root.rglob("*") if p.is_file())
