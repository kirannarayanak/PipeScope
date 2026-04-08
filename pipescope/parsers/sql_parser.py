"""SQL parsing with SQLGlot (table refs, lineage hooks)."""

from __future__ import annotations

from pathlib import Path


def parse_sql_file(path: Path) -> None:
    """Parse a SQL file; Phase 1 will return AST / lineage structures."""
    _ = path.read_text(encoding="utf-8", errors="replace")
