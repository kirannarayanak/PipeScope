"""JSON output for CI/CD."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lineagescope.models import Asset, Edge, Finding


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
    parsed_spark_file_count: int = 0,
    parsed_dbt_project_count: int = 0,
    assets: list[Asset],
    edges: list[Edge],
    graph: dict[str, Any],
    analytics: dict[str, Any] | None = None,
    findings: list[Finding] | None = None,
    scores: dict[str, int] | None = None,
    parse_warnings: list[str] | None = None,
) -> str:
    """Build a single JSON document for ``lineagescope scan --format json``.

    ``analytics`` may include graph summaries, ``dead_asset_analysis``, test and
    documentation coverage, ``complexity_analysis``, ``ownership_analysis``,
    ``contract_compliance_analysis``, ``cost_hotspot_analysis``, etc.
    ``scores`` maps dimension names (e.g. ``dead_assets``, ``ownership``) to 0–100.
    """
    payload: dict[str, Any] = {
        "version": version,
        "scan_root": scan_root,
        "discovered_file_count": discovered_file_count,
        "parsed_sql_file_count": parsed_sql_file_count,
        "parsed_airflow_file_count": parsed_airflow_file_count,
        "parsed_spark_file_count": parsed_spark_file_count,
        "parsed_dbt_project_count": parsed_dbt_project_count,
        "assets": [a.model_dump(mode="json") for a in assets],
        "edges": [e.model_dump(mode="json") for e in edges],
        "graph": graph,
    }
    if analytics is not None:
        payload["analytics"] = analytics
    if findings is not None:
        payload["findings"] = [f.model_dump(mode="json") for f in findings]
    if scores is not None:
        payload["scores"] = dict(scores)
    if parse_warnings:
        payload["parse_warnings"] = list(parse_warnings)
    return json.dumps(payload, indent=2, ensure_ascii=False)
