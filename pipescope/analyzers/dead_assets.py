"""Dead / unused asset detection with impact scoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import networkx as nx

from pipescope.graph import PipelineGraph
from pipescope.models import Asset, Finding, Severity

# Tag substrings (keys or values, case-insensitive) that mark intentional terminal sinks.
DEFAULT_TERMINAL_TAG_MARKERS: frozenset[str] = frozenset(
    ("exposure", "dashboard", "export"),
)


def parse_dead_asset_whitelist_cli(raw: str | None) -> frozenset[str]:
    """Parse ``--dead-asset-whitelist`` (comma-separated asset names)."""
    if raw is None or not str(raw).strip():
        return frozenset()
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def parse_dead_asset_terminal_tags_cli(raw: str | None) -> frozenset[str]:
    """Parse ``--dead-asset-terminal-tags``.

    * ``None`` — use :data:`DEFAULT_TERMINAL_TAG_MARKERS`.
    * Empty or whitespace-only — no tag-based exclusion (empty marker set).
    * Otherwise — comma-separated substrings matched against tag keys/values.
    """
    if raw is None:
        return DEFAULT_TERMINAL_TAG_MARKERS
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    return frozenset(parts)


def _tags_indicate_terminal(tags: dict[str, str], markers: frozenset[str]) -> bool:
    for k, v in tags.items():
        lk = (k or "").lower()
        lv = (v or "").lower()
        for m in markers:
            if m in lk or m in lv:
                return True
    return False


def _ancestors(g: nx.DiGraph, node: str) -> set[str]:
    """All nodes upstream of *node* (cycle-safe BFS backward)."""
    seen: set[str] = set()
    stack: list[str] = list(g.predecessors(node))
    while stack:
        p = stack.pop()
        if p in seen:
            continue
        seen.add(p)
        stack.extend(g.predecessors(p))
    return seen


def _node_waste_unit(g: nx.DiGraph, n: str) -> int:
    """Rough complexity: 1 + number of columns when known."""
    cols = g.nodes[n].get("columns")
    if isinstance(cols, list):
        return 1 + len(cols)
    return 1


def _estimated_wasted_compute(g: nx.DiGraph, dead: str) -> int:
    """Sum complexity units over the dead node and all ancestors."""
    anc = _ancestors(g, dead)
    total = _node_waste_unit(g, dead)
    for u in anc:
        total += _node_waste_unit(g, u)
    return total


def _exclusive_feeder_count(g: nx.DiGraph, dead: str) -> int:
    """Direct predecessors whose only downstream edge targets *dead*."""
    n = 0
    for pred in g.predecessors(dead):
        if g.out_degree(pred) == 1:
            n += 1
    return n


def _severity_for_impact(waste_score: int, exclusive_feeders: int) -> Severity:
    if waste_score >= 50 or exclusive_feeders >= 5:
        return Severity.CRITICAL
    if waste_score >= 20 or exclusive_feeders >= 2:
        return Severity.WARNING
    return Severity.INFO


@dataclass
class DeadAssetAnalysisResult:
    """Outputs from :func:`analyze_dead_assets`."""

    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    dead_count: int = 0
    total_count: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "dead_asset_score": self.score,
            "dead_count": self.dead_count,
            "total_count": self.total_count,
            "details": self.details,
        }


def analyze_dead_assets(
    pg: PipelineGraph,
    assets: list[Asset],
    *,
    terminal_tag_markers: frozenset[str] = DEFAULT_TERMINAL_TAG_MARKERS,
    whitelist: frozenset[str] = frozenset(),
) -> DeadAssetAnalysisResult:
    """Flag pipeline sinks with no downstream consumers (excluding terminals / whitelist).

    1. Candidates: ``out_degree == 0`` and ``in_degree > 0``.
    2. Exclude nodes on *whitelist* and assets whose *tags* match *terminal_tag_markers*.
    3. Per asset: exclusive feeder count, estimated wasted compute on upstream chain.
    4. Score: ``100 - (dead_count / total_count * 100)`` (0--100, higher is better).
    5. One :class:`Finding` per dead asset with severity from upstream impact.
    """
    g = pg.g
    n_assets = len(assets)
    candidates: list[str] = []
    asset_by_name = {a.name: a for a in assets}

    for n in g.nodes():
        if g.out_degree(n) != 0 or g.in_degree(n) == 0:
            continue
        if n in whitelist:
            continue
        attrs = g.nodes[n]
        if attrs.get("kind") == "reference_only":
            continue
        a = asset_by_name.get(n)
        if a is not None and _tags_indicate_terminal(a.tags, terminal_tag_markers):
            continue
        if a is None:
            tags = attrs.get("tags")
            if isinstance(tags, dict):
                tdict = {str(k): str(v) for k, v in tags.items()}
                if _tags_indicate_terminal(tdict, terminal_tag_markers):
                    continue

        candidates.append(n)

    dead_count = len(candidates)
    if n_assets == 0:
        score = 100
    else:
        score = max(0, min(100, 100 - int(round(dead_count / n_assets * 100))))

    findings: list[Finding] = []
    details: list[dict[str, Any]] = []

    for name in sorted(candidates):
        exclusive = _exclusive_feeder_count(g, name)
        waste = _estimated_wasted_compute(g, name)
        sev = _severity_for_impact(waste, exclusive)
        fp = None
        if name in asset_by_name:
            fp = asset_by_name[name].file_path
        else:
            fp = g.nodes[name].get("file_path")

        details.append(
            {
                "asset_name": name,
                "exclusive_upstream_feeders": exclusive,
                "estimated_wasted_compute": waste,
                "severity": sev.value,
            }
        )

        findings.append(
            Finding(
                severity=sev,
                category="dead_asset",
                asset_name=name,
                message=(
                    f"No downstream consumers; {exclusive} exclusive upstream feeder(s); "
                    f"estimated wasted compute (chain complexity)={waste}"
                ),
                file_path=fp if isinstance(fp, str) else None,
            )
        )

    return DeadAssetAnalysisResult(
        findings=findings,
        score=score,
        dead_count=dead_count,
        total_count=len(assets),
        details=details,
    )


def analyze() -> list[Finding]:
    """Stub for callers without graph context; use :func:`analyze_dead_assets`."""
    return []
