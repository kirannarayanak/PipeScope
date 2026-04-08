"""JSON output for CI/CD."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipescope.models import Asset, Edge


def write_json(path: Path, data: object) -> None:
    """Serialize *data* to JSON."""
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def format_scan_json(
    *,
    version: str,
    scan_root: str,
    discovered_file_count: int,
    parsed_sql_file_count: int,
    parsed_airflow_file_count: int = 0,
    assets: list[Asset],
    edges: list[Edge],
    graph: dict[str, Any],
) -> str:
    """Build a single JSON document for ``pipescope scan --format json``."""
    payload = {
        "version": version,
        "scan_root": scan_root,
        "discovered_file_count": discovered_file_count,
        "parsed_sql_file_count": parsed_sql_file_count,
        "parsed_airflow_file_count": parsed_airflow_file_count,
        "assets": [a.model_dump(mode="json") for a in assets],
        "edges": [e.model_dump(mode="json") for e in edges],
        "graph": graph,
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)
