"""Dead asset analyzer tests."""

from __future__ import annotations

from pipescope.analyzers.dead_assets import analyze_dead_assets
from pipescope.graph import build_pipeline_graph
from pipescope.models import Asset, AssetType, Edge, Severity


def test_dead_asset_score_and_findings_simple_chain() -> None:
    """Upstream -> dead sink: one dead asset, score = 100 - 1/2*100 = 50."""
    assets = [
        Asset(name="src", asset_type=AssetType.TABLE, file_path="a.sql"),
        Asset(name="sink", asset_type=AssetType.TABLE, file_path="b.sql"),
    ]
    edges = [Edge(source="src", target="sink")]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_dead_assets(pg, assets)
    assert r.dead_count == 1
    assert r.findings[0].asset_name == "sink"
    assert r.score == 50
    assert r.total_count == 2
    d0 = r.details[0]
    assert d0["exclusive_upstream_feeders"] == 1
    assert d0["estimated_wasted_compute"] >= 2


def test_terminal_tag_excludes_from_dead() -> None:
    assets = [
        Asset(name="a", asset_type=AssetType.TABLE, file_path="x.sql"),
        Asset(
            name="dash",
            asset_type=AssetType.TABLE,
            file_path="d.sql",
            tags={"kind": "dashboard"},
        ),
    ]
    pg = build_pipeline_graph(assets, [Edge(source="a", target="dash")])
    r = analyze_dead_assets(pg, assets)
    assert r.dead_count == 0
    assert r.findings == []
    assert r.score == 100


def test_whitelist_excludes() -> None:
    assets = [
        Asset(name="u", asset_type=AssetType.TABLE, file_path="u.sql"),
        Asset(name="v", asset_type=AssetType.TABLE, file_path="v.sql"),
    ]
    pg = build_pipeline_graph(assets, [Edge(source="u", target="v")])
    r = analyze_dead_assets(pg, assets, whitelist=frozenset({"v"}))
    assert r.dead_count == 0


def test_severity_escalates_with_large_upstream_chain() -> None:
    """Long chain increases wasted compute -> at least WARNING."""
    assets = [
        Asset(name=f"n{i}", asset_type=AssetType.TABLE, file_path=f"{i}.sql", columns=["c"])
        for i in range(12)
    ]
    edges = [Edge(source=f"n{i}", target=f"n{i + 1}") for i in range(11)]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_dead_assets(pg, assets)
    assert r.findings[0].severity in (Severity.WARNING, Severity.CRITICAL)
