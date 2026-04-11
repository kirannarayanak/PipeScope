"""NetworkX graph builder for lineage and dependencies."""

from __future__ import annotations

from typing import Any

import networkx as nx

from pipescope.models import Asset, Edge

# Asset types treated as intentional sinks (e.g. dashboards) — excluded from "dead asset" lists.
_TERMINAL_ASSET_TYPES: frozenset[str] = frozenset()


class PipelineGraph:
    """Directed lineage graph with analytics helpers (dead assets, cycles, fan-out, depth)."""

    def __init__(self) -> None:
        self.g: nx.DiGraph = nx.DiGraph()

    def add_asset(self, asset: Asset) -> None:
        """Add *asset* as a node; attributes mirror :meth:`Asset.model_dump`."""
        self.g.add_node(asset.name, **asset.model_dump(mode="json"))

    def add_edge(self, edge: Edge) -> None:
        """Add ``edge.source → edge.target`` with remaining edge fields as attributes."""
        data = edge.model_dump(mode="json")
        u = data.pop("source")
        v = data.pop("target")
        self.g.add_edge(u, v, **data)

    def get_dead_assets(self) -> list[str]:
        """Sinks with upstream inputs (``out_degree == 0``, ``in_degree > 0``).

        Skips node types listed in ``_TERMINAL_ASSET_TYPES`` when ``asset_type`` is set.
        """
        dead = [
            n
            for n in self.g.nodes()
            if self.g.out_degree(n) == 0 and self.g.in_degree(n) > 0
        ]
        return [
            n
            for n in dead
            if self.g.nodes[n].get("asset_type") not in _TERMINAL_ASSET_TYPES
        ]

    def get_orphan_assets(self) -> list[str]:
        """Assets with no upstream and no downstream (isolated nodes)."""
        return [n for n in self.g.nodes() if self.g.degree(n) == 0]

    def get_high_fanout(self, threshold: int = 15) -> list[tuple[str, int]]:
        """Assets feeding more than *threshold* downstream consumers."""
        return [
            (n, self.g.out_degree(n))
            for n in self.g.nodes()
            if self.g.out_degree(n) > threshold
        ]

    def get_cycles(self) -> list[list[str]]:
        """Circular dependencies (should be empty in a healthy DAG)."""
        try:
            return list(nx.simple_cycles(self.g))
        except Exception:
            return []

    def get_critical_path(self) -> list[str]:
        """Longest dependency chain (unweighted). Empty if the graph is not a DAG."""
        if not nx.is_directed_acyclic_graph(self.g):
            return []
        try:
            return list(nx.dag_longest_path(self.g))
        except Exception:
            return []

    def depth(self, node: str) -> int:
        """Max shortest-path hop count from any root (in-degree 0) down to *node*."""
        try:
            roots = [n for n in self.g.nodes() if self.g.in_degree(n) == 0]
            if not roots:
                return 0
            lengths: list[int] = []
            for root in roots:
                try:
                    lengths.append(nx.shortest_path_length(self.g, root, node))
                except nx.NetworkXNoPath:
                    pass
            return max(lengths) if lengths else 0
        except Exception:
            return 0


def build_pipeline_graph(assets: list[Asset], edges: list[Edge]) -> PipelineGraph:
    """Return a :class:`PipelineGraph` with the same nodes/edges as :func:`build_graph`."""
    pg = PipelineGraph()
    for asset in assets:
        pg.add_asset(asset)
    for edge in edges:
        if edge.source not in pg.g:
            pg.g.add_node(edge.source, kind="reference_only")
        if edge.target not in pg.g:
            pg.g.add_node(edge.target, kind="reference_only")
        pg.add_edge(edge)
    return pg


def compute_scan_analytics(
    pg: PipelineGraph,
    assets: list[Asset],
    *,
    fanout_threshold: int = 15,
) -> dict[str, Any]:
    """Dead/orphan assets, fan-out, cycles, and critical path (see test-coverage analyzer)."""
    dead = pg.get_dead_assets()
    orphans = pg.get_orphan_assets()
    fanout = pg.get_high_fanout(threshold=fanout_threshold)
    cycles = pg.get_cycles()
    cp = pg.get_critical_path()
    return {
        "dead_asset_count": len(dead),
        "dead_assets": sorted(dead),
        "orphan_asset_count": len(orphans),
        "orphan_assets": sorted(orphans),
        "high_fanout": [
            {"asset": name, "out_degree": deg}
            for name, deg in sorted(fanout, key=lambda x: x[0])
        ],
        "high_fanout_threshold": fanout_threshold,
        "cycle_count": len(cycles),
        "cycles": cycles,
        "critical_path_length": len(cp),
        "critical_path": cp,
    }


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
