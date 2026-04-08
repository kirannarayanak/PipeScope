"""NetworkX graph builder for lineage and dependencies."""

from __future__ import annotations

from typing import Any

import networkx as nx

from pipescope.models import Asset, Edge


def new_graph() -> nx.DiGraph:
    """Return an empty directed graph for assets and edges."""
    return nx.DiGraph()


def build_graph(assets: list[Asset], edges: list[Edge]) -> nx.DiGraph:
    """Load *assets* and *edges* into a directed lineage graph.

    Nodes are asset names (``Asset.name``). Edges run **upstream → downstream**
    (``source`` → ``target``). Endpoints referenced only in edges get a minimal
    node so the graph stays connected to the parsed edge list.
    """
    g: nx.DiGraph = new_graph()

    for asset in assets:
        g.add_node(
            asset.name,
            asset_type=asset.asset_type.value,
            file_path=asset.file_path,
            columns=asset.columns,
            has_tests=asset.has_tests,
            has_docs=asset.has_docs,
            tags=asset.tags,
        )

    for edge in edges:
        if edge.source not in g:
            g.add_node(edge.source, kind="reference_only")
        if edge.target not in g:
            g.add_node(edge.target, kind="reference_only")
        g.add_edge(
            edge.source,
            edge.target,
            column_mapping=dict(edge.column_mapping),
        )

    return g


def graph_summary(g: nx.DiGraph) -> dict[str, Any]:
    """Lightweight stats for reports and JSON output."""
    n = g.number_of_nodes()
    m = g.number_of_edges()
    out: dict[str, Any] = {
        "node_count": n,
        "edge_count": m,
    }
    if n:
        out["is_directed_acyclic"] = nx.is_directed_acyclic_graph(g)
    else:
        out["is_directed_acyclic"] = True
    return out
