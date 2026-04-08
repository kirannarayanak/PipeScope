"""Tests for graph utilities."""

from pipescope.graph import new_graph


def test_new_graph_is_empty_digraph() -> None:
    g = new_graph()
    assert g.number_of_nodes() == 0
    assert g.number_of_edges() == 0
