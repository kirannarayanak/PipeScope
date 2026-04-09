"""Tests for test coverage analyzer."""

from __future__ import annotations

from pipescope.analyzers.test_coverage import analyze_test_coverage
from pipescope.graph import build_pipeline_graph
from pipescope.models import Asset, AssetType, Edge, Severity


def test_coverage_score_and_missing_test_severity() -> None:
    assets = [
        Asset(name="a", asset_type=AssetType.DBT_MODEL, file_path="models/a.sql", has_tests=True),
        Asset(name="b", asset_type=AssetType.DBT_MODEL, file_path="models/b.sql", has_tests=False),
        Asset(name="c", asset_type=AssetType.DBT_MODEL, file_path="models/c.sql", has_tests=True),
    ]
    edges = [Edge(source="a", target="b"), Edge(source="b", target="c")]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_test_coverage(pg, assets)
    assert r.total_count == 3
    assert r.score == 67
    assert r.tested_count == 2
    assert r.coverage_ratio is not None
    assert abs(r.coverage_ratio - 2 / 3) < 1e-5
    missing = [f for f in r.findings if f.category == "missing_test"]
    assert len(missing) == 1
    assert missing[0].asset_name == "b"
    assert missing[0].severity == Severity.WARNING


def test_critical_many_dependents_without_tests() -> None:
    assets = [
        Asset(
            name="risky",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/marts/risky.sql",
            has_tests=False,
        ),
    ] + [
        Asset(
            name=f"c{i}",
            asset_type=AssetType.DBT_MODEL,
            file_path=f"models/marts/c{i}.sql",
            has_tests=True,
        )
        for i in range(12)
    ]
    edges = [Edge(source="risky", target=f"c{i}") for i in range(12)]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_test_coverage(pg, assets)
    crit = [f for f in r.findings if f.category == "missing_test" and f.asset_name == "risky"]
    assert len(crit) == 1
    assert crit[0].severity == Severity.CRITICAL


def test_staging_downgrades_severity() -> None:
    assets = [
        Asset(
            name="stg_x",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/staging/stg_x.sql",
            has_tests=False,
        ),
        Asset(
            name="downstream",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/marts/d.sql",
            has_tests=True,
        ),
    ]
    edges = [Edge(source="stg_x", target="downstream")]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_test_coverage(pg, assets)
    m = [f for f in r.findings if f.asset_name == "stg_x"][0]
    assert m.severity == Severity.INFO


def test_weak_test_richness_info_on_mart() -> None:
    assets = [
        Asset(
            name="fct_orders",
            asset_type=AssetType.DBT_MODEL,
            file_path="models/marts/fct_orders.sql",
            has_tests=True,
            tags={"project": "p", "test_richness": "low"},
        ),
    ]
    pg = build_pipeline_graph(assets, [])
    r = analyze_test_coverage(pg, assets)
    weak = [f for f in r.findings if f.category == "weak_test_coverage"]
    assert len(weak) == 1
    assert weak[0].severity == Severity.INFO
