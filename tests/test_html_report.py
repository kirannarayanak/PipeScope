"""Tests for HTML report generation."""

from __future__ import annotations

from pathlib import Path

from pipescope.reporters.html_report import write_report


def _payload() -> dict:
    return {
        "scan_root": "repo",
        "scores": {
            "dead_assets": 90,
            "test_coverage": 80,
            "documentation": 70,
            "complexity": 40,
            "ownership": 85,
            "contracts": 75,
            "cost_hotspots": 88,
        },
        "assets": [
            {"name": "a", "asset_type": "dbt_model", "file_path": "models/a.sql"},
            {"name": "b", "asset_type": "table", "file_path": "sql/b.sql"},
        ],
        "edges": [{"source": "a", "target": "b"}],
        "findings": [
            {
                "severity": "warning",
                "category": "cost_hotspot",
                "asset_name": "a",
                "message": "x",
                "file_path": "models/a.sql",
            }
        ],
    }


def test_write_report_renders_sections(tmp_path: Path) -> None:
    report = tmp_path / "report.html"
    write_report(report, _payload())
    html = report.read_text(encoding="utf-8")
    assert "PipeScope Report" in html
    assert "Overall Health" in html
    assert "Category" in html
    assert "Lineage Graph" in html
    assert "Findings" in html
    assert "graph-data" in html
    assert "cdn.jsdelivr.net/npm/d3@7" in html
    assert "node-details" in html


def test_write_report_updates_history(tmp_path: Path) -> None:
    report = tmp_path / "report.html"
    write_report(report, _payload())
    write_report(report, _payload())
    hist = report.with_suffix(".history.jsonl")
    assert hist.is_file()
    lines = [x for x in hist.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) >= 2
