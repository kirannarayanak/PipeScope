"""Tests for SQLGlot-based SQL parser."""

from __future__ import annotations

from pipescope.models import AssetType
from pipescope.parsers.sql_parser import detect_cost_patterns, parse_sql_file


def _edge_pairs(edges: list) -> set[tuple[str, str]]:
    return {(e.source, e.target) for e in edges}


def test_create_table_as_select_extracts_asset_and_edge() -> None:
    sql = """
    CREATE TABLE public.orders AS
    SELECT id, customer_id FROM raw.events;
    """
    assets, edges = parse_sql_file("models/orders.sql", sql, dialect="postgres")
    names = {a.name for a in assets}
    assert "public.orders" in names
    assert _edge_pairs(edges) == {("raw.events", "public.orders")}
    orders = next(a for a in assets if a.name == "public.orders")
    assert orders.asset_type == AssetType.TABLE


def test_insert_into_select_builds_edge_to_target() -> None:
    sql = """
    INSERT INTO warehouse.facts (id)
    SELECT user_id FROM staging.users;
    """
    _, edges = parse_sql_file("load/facts.sql", sql, dialect="postgres")
    assert _edge_pairs(edges) == {("staging.users", "warehouse.facts")}


def test_create_view_asset_and_upstream_edge() -> None:
    sql = """
    CREATE VIEW analytics.v_revenue AS
    SELECT SUM(amount) AS revenue FROM finance.payments;
    """
    assets, edges = parse_sql_file("views/revenue.sql", sql, dialect="postgres")
    v = next(a for a in assets if a.name == "analytics.v_revenue")
    assert v.asset_type == AssetType.VIEW
    assert _edge_pairs(edges) == {("finance.payments", "analytics.v_revenue")}


def test_select_with_joins_yields_query_asset_and_source_edges() -> None:
    sql = """
    SELECT a.id, b.name
    FROM schema1.table_a AS a
    JOIN schema2.table_b AS b ON a.id = b.id;
    """
    assets, edges = parse_sql_file("adhoc/join.sql", sql, dialect="postgres")
    qname = "adhoc/join.sql#select0"
    assert any(a.name == qname and a.asset_type == AssetType.VIEW for a in assets)
    assert _edge_pairs(edges) == {
        ("schema1.table_a", qname),
        ("schema2.table_b", qname),
    }


def test_select_star_emits_cost_pattern() -> None:
    sql = "SELECT * FROM big_table WHERE id = 1"
    assert "SELECT_STAR" in detect_cost_patterns(sql, dialect="postgres")
