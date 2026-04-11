"""JSON report helpers."""

import json

from lineagescope.models import Asset, AssetType, Edge
from lineagescope.reporters.json_report import format_scan_json


def test_format_scan_json_roundtrip() -> None:
    assets = [
        Asset(
            name="t",
            asset_type=AssetType.TABLE,
            file_path="a.sql",
            columns=["c"],
        )
    ]
    edges = [Edge(source="u", target="t")]
    raw = format_scan_json(
        version="0.1.0",
        scan_root="/repo",
        discovered_file_count=3,
        parsed_sql_file_count=1,
        assets=assets,
        edges=edges,
        graph={"node_count": 2, "edge_count": 1, "is_directed_acyclic": True},
    )
    data = json.loads(raw)
    assert data["version"] == "0.1.0"
    assert data["scan_root"] == "/repo"
    assert data["assets"][0]["name"] == "t"
    assert data["edges"][0]["source"] == "u"
    assert data["graph"]["edge_count"] == 1


def test_format_scan_json_includes_parse_warnings_when_present() -> None:
    assets = [
        Asset(name="t", asset_type=AssetType.TABLE, file_path="a.sql", columns=[]),
    ]
    edges: list = []
    raw = format_scan_json(
        version="0.1.0",
        scan_root="/repo",
        discovered_file_count=1,
        parsed_sql_file_count=1,
        assets=assets,
        edges=edges,
        graph={"node_count": 1, "edge_count": 0, "is_directed_acyclic": True},
        parse_warnings=["x.sql: skipped (Error: boom)"],
    )
    data = json.loads(raw)
    assert data["parse_warnings"] == ["x.sql: skipped (Error: boom)"]