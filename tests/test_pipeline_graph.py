"""Tests for PipelineGraph (10-node manual graph: fan-out, dead-end, cycle, orphan)."""

from __future__ import annotations

import networkx as nx

from pipescope.graph import PipelineGraph
from pipescope.models import Asset, AssetType, Edge


def _table(name: str) -> Asset:
    return Asset(name=name, asset_type=AssetType.TABLE, file_path=f"{name}.sql")


def _build_ten_node_graph() -> PipelineGraph:
    """10 nodes: hub fan-out, dead-end chain, 2-node cycle, isolated orphan."""
    pg = PipelineGraph()
    names = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    for n in names:
        pg.add_asset(_table(n))

    # High fan-out from A (5 downstream consumers)
    for t in ["B", "C", "D", "E", "F"]:
        pg.add_edge(Edge(source="A", target=t))
    # Dead-end sink: B -> G (G is a terminal sink; C–F are also sinks from A)
    pg.add_edge(Edge(source="B", target="G"))
    # Cycle: H <-> I
    pg.add_edge(Edge(source="H", target="I"))
    pg.add_edge(Edge(source="I", target="H"))
    # J remains isolated (orphan)
    return pg


def test_pipeline_graph_fanout_dead_cycle_orphan() -> None:
    pg = _build_ten_node_graph()
    g = pg.g
    assert g.number_of_nodes() == 10
    assert g.number_of_edges() == 8

    # Fan-out: A feeds 5 children (threshold 3)
    fan = pg.get_high_fanout(threshold=3)
    assert fan == [("A", 5)]

    # Dead assets: sinks with incoming edges (no downstream consumers)
    dead = set(pg.get_dead_assets())
    assert dead == {"C", "D", "E", "F", "G"}

    # Orphan: no edges at all
    assert pg.get_orphan_assets() == ["J"]

    # Cycle between H and I
    cycles = pg.get_cycles()
    assert len(cycles) == 1
    c0 = set(cycles[0])
    assert c0 == {"H", "I"}

    # Not a DAG → longest path helper returns empty
    assert pg.get_critical_path() == []

    # Depth from roots (in-degree 0): A and J. A→B→G → depth(G)==2; J alone → 0.
    # Cycle H↔I: no path from A or J, so depth is 0 for those nodes.
    assert pg.depth("G") == 2
    assert pg.depth("J") == 0
    assert pg.depth("I") == 0


def test_pipeline_graph_dag_longest_path_and_depth() -> None:
    """Acyclic graph: critical path and depth behave as a DAG."""
    pg = PipelineGraph()
    for n in ("a", "b", "c"):
        pg.add_asset(_table(n))
    pg.add_edge(Edge(source="a", target="b"))
    pg.add_edge(Edge(source="b", target="c"))

    assert nx.is_directed_acyclic_graph(pg.g)
    assert pg.get_critical_path() == ["a", "b", "c"]
    assert pg.depth("c") == 2
    assert pg.depth("a") == 0
