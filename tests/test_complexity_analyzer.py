"""Complexity analyzer tests."""

from __future__ import annotations

from pathlib import Path

from pipescope.analyzers.complexity import analyze_complexity
from pipescope.graph import build_pipeline_graph
from pipescope.models import Asset, AssetType, Edge, Severity


def test_complexity_pipeline_score_and_percentile_flag(tmp_path: Path) -> None:
    """Five assets with increasing SQL surface; top tier flagged at 80th percentile."""
    models = tmp_path / "models"
    models.mkdir(parents=True)
    simple = "SELECT 1 AS x"
    heavy = """
    WITH cte1 AS (SELECT 1 a), cte2 AS (SELECT 2 b)
    SELECT CASE WHEN 1=1 THEN 1 ELSE 0 END AS c
    FROM t1
    JOIN t2 ON t1.id = t2.id
    JOIN t3 ON t2.id = t3.id
    JOIN (SELECT id FROM u) s ON s.id = t3.id
    """
    for i, sql in enumerate([simple, simple, simple, simple, heavy]):
        (models / f"m{i}.sql").write_text(sql, encoding="utf-8")

    assets = [
        Asset(
            name=f"n{i}",
            asset_type=AssetType.DBT_MODEL,
            file_path=f"models/m{i}.sql",
        )
        for i in range(5)
    ]
    edges = [Edge(source=f"n{i}", target=f"n{i + 1}") for i in range(4)]
    pg = build_pipeline_graph(assets, edges)
    r = analyze_complexity(pg, assets, tmp_path, dialect="postgres")

    assert r.pipeline_score >= 0
    assert r.percentile_80_threshold > 0
    warned = [f for f in r.findings if f.category == "high_complexity"]
    assert len(warned) >= 1
    assert any(f.asset_name == "n4" for f in warned)
    assert warned[0].severity == Severity.WARNING


def test_all_trivial_scores_no_findings(tmp_path: Path) -> None:
    models = tmp_path / "models"
    models.mkdir(parents=True)
    (models / "a.sql").write_text("SELECT 1", encoding="utf-8")
    assets = [
        Asset(name="a", asset_type=AssetType.DBT_MODEL, file_path="models/a.sql"),
    ]
    pg = build_pipeline_graph(assets, [])
    r = analyze_complexity(pg, assets, tmp_path, dialect="postgres")
    assert r.findings == []
