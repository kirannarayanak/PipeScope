"""Tests for dbt parser."""

from __future__ import annotations

from pathlib import Path

from lineagescope.models import AssetType
from lineagescope.parsers.dbt_parser import parse_dbt_project

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_dbt_sample_extracts_models_sources_and_edges() -> None:
    assets, edges = parse_dbt_project(FIXTURES / "dbt_sample")

    asset_by_name = {a.name: a for a in assets}
    assert "stg_events" in asset_by_name
    assert "fct_sessions" in asset_by_name
    assert "raw.events" in asset_by_name
    assert asset_by_name["stg_events"].asset_type == AssetType.DBT_MODEL
    assert asset_by_name["raw.events"].asset_type == AssetType.DBT_SOURCE
    assert asset_by_name["stg_events"].has_docs is True
    assert asset_by_name["stg_events"].has_tests is True
    assert "id" in asset_by_name["stg_events"].columns
    assert asset_by_name["fct_sessions"].tags.get("test_richness") == "low"
    assert asset_by_name["fct_sessions"].owner == "analytics-team"
    assert asset_by_name["raw.events"].owner == "ingest-team"

    edge_pairs = {(e.source, e.target) for e in edges}
    assert ("raw.events", "stg_events") in edge_pairs
    assert ("stg_events", "fct_sessions") in edge_pairs


def test_parse_jaffle_shop_finds_models_refs_and_schema_tests() -> None:
    assets, edges = parse_dbt_project(FIXTURES / "jaffle_shop")
    model_assets = [a for a in assets if a.asset_type == AssetType.DBT_MODEL]
    model_names = {a.name for a in model_assets}
    assert model_names.issuperset(
        {"customers", "orders", "stg_customers", "stg_orders", "stg_payments"}
    )

    edge_pairs = {(e.source, e.target) for e in edges}
    expected = {
        ("raw_customers", "stg_customers"),
        ("raw_orders", "stg_orders"),
        ("raw_payments", "stg_payments"),
        ("stg_orders", "orders"),
        ("stg_payments", "orders"),
        ("stg_customers", "customers"),
        ("stg_orders", "customers"),
        ("stg_payments", "customers"),
    }
    assert expected.issubset(edge_pairs)

    orders = next(a for a in model_assets if a.name == "orders")
    assert orders.has_docs is True
    assert orders.has_tests is True
    assert "order_id" in orders.columns
