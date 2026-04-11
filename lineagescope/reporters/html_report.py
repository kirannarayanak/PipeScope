"""Interactive single-file HTML report generation via Jinja2."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape


def _overall_health(scores: dict[str, int]) -> int:
    good_dims = (
        "dead_assets",
        "test_coverage",
        "documentation",
        "ownership",
        "contracts",
        "cost_hotspots",
    )
    vals = [scores[k] for k in good_dims if k in scores]
    if "complexity" in scores:
        vals.append(max(0, min(100, 100 - int(scores["complexity"]))))
    if not vals:
        return 100
    return int(round(sum(vals) / len(vals)))


def _tone(score: int) -> str:
    if score >= 85:
        return "good"
    if score >= 65:
        return "warn"
    return "bad"


def _score_cards(scores: dict[str, int], overall: int) -> list[dict[str, Any]]:
    cards = [
        {"key": "overall", "label": "Overall", "score": overall},
        {"key": "dead_assets", "label": "Dead Assets", "score": scores.get("dead_assets", 0)},
        {"key": "test_coverage", "label": "Test Coverage", "score": scores.get("test_coverage", 0)},
        {"key": "documentation", "label": "Documentation", "score": scores.get("documentation", 0)},
        {"key": "complexity", "label": "Complexity", "score": scores.get("complexity", 0)},
        {"key": "ownership", "label": "Ownership", "score": scores.get("ownership", 0)},
        {"key": "contracts", "label": "Contracts", "score": scores.get("contracts", 0)},
        {"key": "cost_hotspots", "label": "Cost Hotspots", "score": scores.get("cost_hotspots", 0)},
    ]
    for c in cards:
        c["tone"] = _tone(int(c["score"]))
    return cards


def _build_graph_data(payload: dict[str, Any]) -> dict[str, Any]:
    assets = payload.get("assets") or []
    edges = payload.get("edges") or []
    node_by_id: dict[str, dict[str, Any]] = {}
    nodes: list[dict[str, Any]] = []
    for a in assets:
        if not isinstance(a, dict):
            continue
        nid = str(a.get("name", ""))
        if not nid:
            continue
        row = {
            "id": nid,
            "type": str(a.get("asset_type", "")),
            "file": str(a.get("file_path", "")),
        }
        nodes.append(row)
        node_by_id[nid] = row
    links: list[dict[str, Any]] = []
    adjacency: dict[str, set[str]] = {n["id"]: set() for n in nodes}
    for e in edges:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if not src or not tgt:
            continue
        if src not in node_by_id:
            node_by_id[src] = {"id": src, "type": "external", "file": ""}
            nodes.append(node_by_id[src])
            adjacency[src] = set()
        if tgt not in node_by_id:
            node_by_id[tgt] = {"id": tgt, "type": "external", "file": ""}
            nodes.append(node_by_id[tgt])
            adjacency[tgt] = set()
        adjacency[src].add(tgt)
        links.append(
            {
                "source": src,
                "target": tgt,
            }
        )

    def downstream_count(start: str) -> int:
        seen: set[str] = set()
        stack = list(adjacency.get(start, set()))
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(adjacency.get(cur, set()))
        return len(seen)

    for n in nodes:
        n["importance"] = downstream_count(n["id"])
    return {"nodes": nodes, "links": links}


def _read_history(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            out.append(item)
    return out


def _append_history(path: Path, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    existing = _read_history(path)
    existing.append(snapshot)
    path.write_text(
        "\n".join(json.dumps(x, ensure_ascii=False) for x in existing[-30:]) + "\n",
        encoding="utf-8",
    )
    return existing[-30:]


def write_report(path: Path, payload: dict[str, Any]) -> None:
    """Render ``payload`` (scan dict) to *path* as a single HTML file.

    Embeds CSS/JS, D3 lineage, sortable findings, score cards, and appends a
    small history series next to *path* (``.history.jsonl``) for sparkline trends.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(tz=UTC)

    scores = payload.get("scores", {}) or {}
    findings = payload.get("findings", []) or []
    overall = _overall_health(scores)
    graph_data = _build_graph_data(payload)

    snapshot = {
        "scan_date": now.isoformat(),
        "overall": overall,
        "scores": scores,
    }
    hist_file = path.with_suffix(".history.jsonl")
    history = _append_history(hist_file, snapshot)

    template_dir = Path(__file__).resolve().parent.parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("report.html.j2")
    html = template.render(
        project_name=Path(str(payload.get("scan_root", "LineageScope"))).name or "LineageScope",
        scan_root=str(payload.get("scan_root", "")),
        scan_date=now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        overall_score=overall,
        overall_tone=_tone(overall),
        score_cards=_score_cards(scores, overall),
        findings=findings,
        graph_json=json.dumps(graph_data, ensure_ascii=False),
        trend_json=json.dumps(history, ensure_ascii=False),
    )
    path.write_text(html, encoding="utf-8")
