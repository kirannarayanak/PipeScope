"""Spark job parsing via Python AST."""

from __future__ import annotations

from pathlib import Path


def parse_spark_file(path: Path) -> None:
    """Parse a Python file for Spark read/write and SQL usage."""
    _ = path.read_text(encoding="utf-8", errors="replace")
