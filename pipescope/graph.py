"""NetworkX graph builder for lineage and dependencies."""

from __future__ import annotations

import networkx as nx


def new_graph() -> nx.DiGraph:
    """Return an empty directed graph for assets and edges."""
    return nx.DiGraph()
