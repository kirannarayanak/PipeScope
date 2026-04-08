"""Airflow DAG parsing via Python AST."""

from __future__ import annotations

from pathlib import Path


def parse_airflow_file(path: Path) -> None:
    """Parse a Python file that may contain DAG definitions."""
    _ = path.read_text(encoding="utf-8", errors="replace")
