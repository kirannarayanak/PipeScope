"""Tests for cost hotspot analyzer."""

from __future__ import annotations

from pathlib import Path

from pipescope.analyzers.cost_hotspots import analyze_cost_hotspots
from pipescope.graph import build_pipeline_graph
from pipescope.models import Asset, AssetType, Edge


def test_hotspot_weights_downstream_more_than_upstream_only(tmp_path: Path) -> None:
    root = tmp_path
    sql_dir = root / "models"
    sql_dir.mkdir()
    (sql_dir / "heavy.sql").write_text(
        "SELECT * FROM a CROSS JOIN b;\n",
        encoding="utf-8",
    )
    (sql_dir / "other.sql").write_text("SELECT 1\n", encoding="utf-8")
    (sql_dir / "other2.sql").write_text("SELECT 1\n", encoding="utf-8")

    assets = [
        Asset(
            name="sink",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/heavy.sql",
            tags={"partition_key": "dt"},
        ),
        Asset(
            name="down1",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/other.sql",
        ),
        Asset(
            name="down2",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/other2.sql",
        ),
    ]
    edges = [
        Edge(source="sink", target="down1"),
        Edge(source="sink", target="down2"),
    ]

    pg = build_pipeline_graph(assets, edges)
    r = analyze_cost_hotspots(pg, assets, root, dialect="postgres")
    assert r.ranked
    top = r.ranked[0]
    assert top["asset_name"] == "sink"
    assert top["downstream_count"] == 2
    assert top["weighted_impact"] > 0
