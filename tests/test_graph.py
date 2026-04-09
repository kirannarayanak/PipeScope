"""Tests for graph utilities."""

from pipescope.graph import (
    build_graph,
    build_pipeline_graph,
    compute_scan_analytics,
    graph_summary,
    new_graph,
)
from pipescope.models import Asset, AssetType, Edge


def test_new_graph_is_empty_digraph() -> None:
    g = new_graph()
    assert g.number_of_nodes() == 0
    assert g.number_of_edges() == 0


def test_build_graph_from_assets_and_edges() -> None:
    assets = [
        Asset(
            name="a.t1",
            asset_type=AssetType.TABLE,
            file_path="x.sql",
            columns=["id"],
        ),
    ]
    edges = [
        Edge(source="b.upstream", target="a.t1"),
    ]
    g = build_graph(assets, edges)
    assert g.has_node("a.t1")
    assert g.has_node("b.upstream")
    assert g.has_edge("b.upstream", "a.t1")
    assert g.nodes["a.t1"]["asset_type"] == "table"
    assert g.nodes["b.upstream"]["kind"] == "reference_only"


def test_graph_summary_empty_and_dag() -> None:
    assert graph_summary(new_graph()) == {
        "node_count": 0,
        "edge_count": 0,
        "is_directed_acyclic": True,
    }

    g = build_graph(
        [
            Asset(
                name="x",
                asset_type=AssetType.TABLE,
                file_path="f.sql",
            )
        ],
        [],
    )
    s = graph_summary(g)
    assert s["node_count"] == 1
    assert s["edge_count"] == 0
    assert s["is_directed_acyclic"] is True


def test_build_pipeline_graph_matches_build_graph_topology() -> None:
    assets = [
        Asset(
            name="a.t1",
            asset_type=AssetType.TABLE,
            file_path="x.sql",
            columns=["id"],
        ),
    ]
    edges = [Edge(source="b.upstream", target="a.t1")]
    g = build_graph(assets, edges)
    pg = build_pipeline_graph(assets, edges)
    assert g.number_of_nodes() == pg.g.number_of_nodes()
    assert g.number_of_edges() == pg.g.number_of_edges()
    assert sorted(g.edges()) == sorted(pg.g.edges())


def test_compute_scan_analytics_reports_dead_assets() -> None:
    assets = [
        Asset(name="x", asset_type=AssetType.TABLE, file_path="a.sql", has_tests=True),
        Asset(name="y", asset_type=AssetType.TABLE, file_path="b.sql", has_tests=False),
    ]
    pg = build_pipeline_graph(assets, [Edge(source="x", target="y")])
    a = compute_scan_analytics(pg, assets)
    assert a["dead_asset_count"] == 1
    assert a["dead_assets"] == ["y"]
