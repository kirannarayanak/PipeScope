"""dbt project parsing (manifest, Jinja, schema YAML)."""

from __future__ import annotations

from pathlib import Path


def parse_dbt_project(root: Path) -> None:
    """Discover dbt layout under *root*; Phase 1 fills in manifest handling."""
    _ = root
