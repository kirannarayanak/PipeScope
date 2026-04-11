"""Tests for contract compliance analyzer."""

from __future__ import annotations

from lineagescope.analyzers.contracts import (
    analyze_contract_compliance,
    find_asset_for_contract,
)
from lineagescope.graph import build_pipeline_graph
from lineagescope.models import Asset, AssetType
from lineagescope.parsers.odcs_parser import ParsedContract


def test_compliant_when_columns_and_types_match() -> None:
    assets = [
        Asset(
            name="orders",
            asset_type=AssetType.DBT_MODEL,
            file_path="m.sql",
            columns=["id", "amt"],
            column_types={"id": "INT", "amt": "DECIMAL(10,2)"},
        ),
    ]
    contracts = [
        ParsedContract(
            file_path="c.yaml",
            dataset_name="orders",
            columns={"id": "integer", "amt": "numeric"},
        ),
    ]
    pg = build_pipeline_graph(assets, [])
    r = analyze_contract_compliance(pg, assets, contracts)
    assert r.total_contracts == 1
    assert r.compliant_contracts == 1
    assert r.score == 100
    assert not r.findings


def test_missing_column_finding() -> None:
    assets = [
        Asset(
            name="t",
            asset_type=AssetType.TABLE,
            file_path="t.sql",
            columns=["a"],
            column_types={"a": "int"},
        ),
    ]
    contracts = [
        ParsedContract(
            file_path="c.yaml",
            dataset_name="t",
            columns={"a": "int", "b": "string"},
        ),
    ]
    pg = build_pipeline_graph(assets, [])
    r = analyze_contract_compliance(pg, assets, contracts)
    assert r.compliant_contracts == 0
    assert any(f.category == "contract_missing_column" for f in r.findings)


def test_asset_not_found() -> None:
    assets = [
        Asset(name="other", asset_type=AssetType.TABLE, file_path="o.sql", columns=[]),
    ]
    contracts = [
        ParsedContract(
            file_path="c.yaml",
            dataset_name="missing",
            columns={"x": "string"},
        ),
    ]
    pg = build_pipeline_graph(assets, [])
    r = analyze_contract_compliance(pg, assets, contracts)
    assert any(f.category == "contract_asset_not_found" for f in r.findings)


def test_find_asset_stem_match() -> None:
    a = Asset(name="raw.orders", asset_type=AssetType.DBT_SOURCE, file_path="s.yml", columns=[])
    assert find_asset_for_contract("orders", [a]) is a


def test_query_blocks_excluded_from_match() -> None:
    q = Asset(
        name="q#select0",
        asset_type=AssetType.VIEW,
        file_path="f.sql",
        tags={"role": "query_block"},
    )
    t = Asset(name="x", asset_type=AssetType.TABLE, file_path="t.sql", columns=["a"])
    assert find_asset_for_contract("x", [q, t]) is t
