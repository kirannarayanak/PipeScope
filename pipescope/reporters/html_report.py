"""Interactive HTML report generation."""

from __future__ import annotations

from pathlib import Path


def write_report(path: Path, payload: dict) -> None:
    """Write a single-file HTML report (stub)."""
    path.write_text(
        "<!DOCTYPE html><html><body><p>PipeScope report (stub)</p></body></html>",
        encoding="utf-8",
    )
    _ = payload
