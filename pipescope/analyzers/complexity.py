"""Pipeline complexity scoring from graph topology and SQL structure."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from pipescope.graph import PipelineGraph
from pipescope.models import Asset, Finding, Severity

WEIGHT_DEPTH = 0.25
WEIGHT_FANOUT = 0.25
WEIGHT_FANIN = 0.25
WEIGHT_SQL = 0.25

PERCENTILE_WARNING = 80.0


def _scope_assets(assets: list[Asset]) -> list[Asset]:
    return [a for a in assets if a.tags.get("role") != "query_block"]


def _read_sql_file(asset: Asset, scan_root: Path) -> str | None:
    fp = (asset.file_path or "").lower()
    if not fp.endswith(".sql"):
        return None
    path = scan_root / asset.file_path
    if not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None


def _count_sql_features(content: str, dialect: str | None) -> dict[str, int]:
    """Counts JOINs, subqueries, CTEs, and CASE expressions (sqlglot AST)."""
    try:
        stmts = sqlglot.parse(
            content,
            read=dialect,
            error_level=ErrorLevel.IGNORE,
        )
    except Exception:
        return {"joins": 0, "subqueries": 0, "ctes": 0, "cases": 0, "total": 0}

    joins = subqueries = ctes = cases = 0
    for stmt in stmts:
        if stmt is None:
            continue
        joins += len(list(stmt.find_all(exp.Join)))
        subqueries += len(list(stmt.find_all(exp.Subquery)))
        ctes += len(list(stmt.find_all(exp.CTE)))
        cases += len(list(stmt.find_all(exp.Case)))

    total = joins + subqueries + ctes + cases
    return {
        "joins": joins,
        "subqueries": subqueries,
        "ctes": ctes,
        "cases": cases,
        "total": total,
    }


def _percentile_linear(values: list[float], p: float) -> float:
    """Linear interpolation percentile in [0, 100]."""
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n == 1:
        return s[0]
    k = (n - 1) * (p / 100.0)
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return s[lo]
    return s[lo] + (k - lo) * (s[hi] - s[lo])


def _normalize(values: list[int], x: int) -> float:
    m = max(values) if values else 0
    if m <= 0:
        return 0.0
    return min(100.0, (x / m) * 100.0)


@dataclass
class ComplexityAnalysisResult:
    findings: list[Finding] = field(default_factory=list)
    pipeline_score: int = 0
    percentile_80_threshold: float = 0.0
    assets_above_percentile: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "complexity_pipeline_score": self.pipeline_score,
            "percentile_80_threshold": self.percentile_80_threshold,
            "assets_above_percentile": self.assets_above_percentile,
            "weights": {
                "depth": WEIGHT_DEPTH,
                "fanout": WEIGHT_FANOUT,
                "fanin": WEIGHT_FANIN,
                "sql": WEIGHT_SQL,
            },
            "details": self.details,
        }


def analyze_complexity(
    pg: PipelineGraph,
    assets: list[Asset],
    scan_root: Path,
    dialect: str | None = None,
) -> ComplexityAnalysisResult:
    """Per-asset weighted complexity, pipeline average, WARNING above 80th percentile."""
    scope = _scope_assets(assets)
    if not scope:
        return ComplexityAnalysisResult()

    g = pg.g
    depths: list[int] = []
    fanouts: list[int] = []
    fanins: list[int] = []
    sql_totals: list[int] = []

    raw_rows: list[dict[str, Any]] = []

    for asset in scope:
        name = asset.name
        d = pg.depth(name) if name in g else 0
        fo = int(g.out_degree(name)) if name in g else 0
        fi = int(g.in_degree(name)) if name in g else 0
        sql_text = _read_sql_file(asset, scan_root)
        if sql_text is not None:
            feats = _count_sql_features(sql_text, dialect)
            st = feats["total"]
        else:
            feats = {"joins": 0, "subqueries": 0, "ctes": 0, "cases": 0, "total": 0}
            st = 0

        depths.append(d)
        fanouts.append(fo)
        fanins.append(fi)
        sql_totals.append(st)

        raw_rows.append(
            {
                "name": name,
                "depth": d,
                "fanout": fo,
                "fanin": fi,
                "sql_features": feats,
            }
        )

    max_d = max(depths) if depths else 0
    max_fo = max(fanouts) if fanouts else 0
    max_fi = max(fanins) if fanins else 0
    max_sql = max(sql_totals) if sql_totals else 0

    combined_scores: list[float] = []
    details: list[dict[str, Any]] = []

    for i, asset in enumerate(scope):
        row = raw_rows[i]
        nd = _normalize(depths, row["depth"]) if max_d > 0 else 0.0
        nfo = _normalize(fanouts, row["fanout"]) if max_fo > 0 else 0.0
        nfi = _normalize(fanins, row["fanin"]) if max_fi > 0 else 0.0
        nsql = _normalize(sql_totals, sql_totals[i]) if max_sql > 0 else 0.0

        combined = (
            WEIGHT_DEPTH * nd
            + WEIGHT_FANOUT * nfo
            + WEIGHT_FANIN * nfi
            + WEIGHT_SQL * nsql
        )
        combined_scores.append(combined)

        details.append(
            {
                "asset_name": asset.name,
                "file_path": asset.file_path,
                "depth_score": round(nd, 4),
                "fanout_score": round(nfo, 4),
                "fanin_score": round(nfi, 4),
                "sql_complexity_score": round(nsql, 4),
                "sql_counts": row["sql_features"],
                "combined_score": round(combined, 4),
            }
        )

    pipeline = (
        int(round(sum(combined_scores) / len(combined_scores)))
        if combined_scores
        else 0
    )
    p80 = _percentile_linear(combined_scores, PERCENTILE_WARNING)

    findings: list[Finding] = []
    above = 0
    for i, asset in enumerate(scope):
        sc = combined_scores[i]
        # Skip noise when the whole pipeline is effectively trivial (all zeros).
        if sc < p80 - 1e-9 or sc < 1.0:
            continue
        above += 1
        findings.append(
            Finding(
                severity=Severity.WARNING,
                category="high_complexity",
                asset_name=asset.name,
                message=(
                    f"Complexity {sc:.1f} at/above {PERCENTILE_WARNING:.0f}th percentile "
                    f"(threshold {p80:.1f})"
                ),
                file_path=asset.file_path or None,
            )
        )

    return ComplexityAnalysisResult(
        findings=findings,
        pipeline_score=pipeline,
        percentile_80_threshold=round(p80, 6),
        assets_above_percentile=above,
        details=details,
    )


def analyze() -> list[Finding]:
    """Stub without graph; use :func:`analyze_complexity`."""
    return []
