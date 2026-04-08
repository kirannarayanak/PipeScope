"""Tests for graph utilities."""

from pipescope.graph import build_graph, graph_summary, new_graph
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
