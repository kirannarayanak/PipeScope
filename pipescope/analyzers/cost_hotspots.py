"""Cost hotspot analysis: static SQL patterns × downstream lineage impact."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

from pipescope.graph import PipelineGraph
from pipescope.models import Asset, AssetType, Finding, Severity
from pipescope.parsers.sql_parser import (
    detect_cost_patterns,
    detect_partition_filter_issues,
)


def _partition_map_from_assets(assets: list[Asset]) -> dict[str, str]:
    out: dict[str, str] = {}
    for a in assets:
        pk = a.tags.get("partition_key")
        if isinstance(pk, str) and pk.strip():
            out[a.name] = pk.strip()
    return out


def _downstream_count(g: nx.DiGraph, node: str) -> int:
    if node not in g:
        return 0
    return len(nx.descendants(g, node))


def _eligible_sql_assets(assets: list[Asset]) -> list[Asset]:
    return [
        a
        for a in assets
        if a.file_path.lower().endswith(".sql")
        and a.tags.get("role") != "query_block"
        and a.asset_type
        in (AssetType.DBT_MODEL, AssetType.TABLE, AssetType.VIEW)
    ]


def _weighted_hotspot(pattern_count: int, downstream: int) -> float:
    """More downstream consumers amplify the same static risk."""
    if pattern_count <= 0:
        return 0.0
    cap = 50.0
    d = min(cap, float(downstream))
    return float(pattern_count) * (1.0 + 0.12 * d)


@dataclass
class CostHotspotResult:
    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    ranked: list[dict[str, Any]] = field(default_factory=list)
    total_pattern_count: int = 0
    max_weighted: float = 0.0

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "cost_hotspot_score": self.score,
            "total_pattern_instances": self.total_pattern_count,
            "max_weighted_impact": round(self.max_weighted, 4),
            "ranked": self.ranked,
        }


def analyze_cost_hotspots(
    pg: PipelineGraph,
    assets: list[Asset],
    scan_root: Path,
    dialect: str | None,
) -> CostHotspotResult:
    """Rank SQL assets by static cost patterns weighted by downstream graph reach."""
    partition_map = _partition_map_from_assets(assets)
    eligible = _eligible_sql_assets(assets)
    if not eligible:
        return CostHotspotResult()

    scan_root = scan_root.resolve()
    # file_path -> file contents (one read per path)
    content_cache: dict[str, str] = {}
    ranked_rows: list[dict[str, Any]] = []
    findings: list[Finding] = []
    total_patterns = 0
    max_w = 0.0

    by_file: dict[str, list[Asset]] = defaultdict(list)
    for a in eligible:
        by_file[a.file_path].append(a)

    for rel, group in by_file.items():
        path = scan_root / rel
        if not path.is_file():
            if Path(rel).is_absolute() and Path(rel).is_file():
                path = Path(rel)
            else:
                continue
        key = str(path.resolve())
        if key not in content_cache:
            try:
                content_cache[key] = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
        content = content_cache[key]
        patterns = list(dict.fromkeys(detect_cost_patterns(content, dialect)))
        patterns.extend(detect_partition_filter_issues(content, dialect, partition_map))
        patterns = list(dict.fromkeys(patterns))
        if not patterns:
            continue

        for a in group:
            down = _downstream_count(pg.g, a.name)
            w = _weighted_hotspot(len(patterns), down)
            total_patterns += len(patterns)
            max_w = max(max_w, w)

            ranked_rows.append(
                {
                    "asset_name": a.name,
                    "file_path": a.file_path,
                    "patterns": patterns,
                    "pattern_count": len(patterns),
                    "downstream_count": down,
                    "weighted_impact": round(w, 4),
                }
            )

            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    category="cost_hotspot",
                    asset_name=a.name,
                    message=(
                        f"Static cost signals: {', '.join(patterns)} "
                        f"(downstream nodes: {down}, weighted impact: {w:.2f})"
                    ),
                    file_path=a.file_path or None,
                )
            )

    ranked_rows.sort(key=lambda r: r["weighted_impact"], reverse=True)

    # Score: higher is "healthier" (fewer / lighter hotspots)
    if not ranked_rows:
        return CostHotspotResult(ranked=[], score=100)

    sum_w = sum(float(r["weighted_impact"]) for r in ranked_rows)
    n = len(ranked_rows)
    # Penalize average weighted burden; cap contribution
    penalty = min(100.0, (sum_w / max(1.0, n)) * 8.0)
    score = int(max(0, min(100, round(100.0 - penalty))))

    return CostHotspotResult(
        findings=findings,
        score=score,
        ranked=ranked_rows[:50],
        total_pattern_count=total_patterns,
        max_weighted=max_w,
    )


def analyze() -> list[Finding]:
    """Stub without scan context; use :func:`analyze_cost_hotspots`."""
    return []
