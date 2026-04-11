"""Rich terminal renderer for scan output."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from lineagescope.models import Asset, Edge, Finding, Severity


def _overall_health_score(scores: dict[str, int]) -> int:
    positive_dims = [
        "dead_assets",
        "test_coverage",
        "documentation",
        "ownership",
        "contracts",
        "cost_hotspots",
    ]
    vals: list[int] = [scores[k] for k in positive_dims if k in scores]
    if "complexity" in scores:
        vals.append(max(0, min(100, 100 - int(scores["complexity"]))))
    if not vals:
        return 100
    return int(round(sum(vals) / len(vals)))


def _score_color(score: int) -> str:
    if score >= 85:
        return "green"
    if score >= 65:
        return "yellow"
    return "red"


def _severity_color(severity: Severity) -> str:
    if severity == Severity.CRITICAL:
        return "red"
    if severity == Severity.WARNING:
        return "yellow"
    return "blue"


def print_terminal_report(
    console: Console,
    *,
    scan_root: str,
    discovered_file_count: int,
    assets: list[Asset],
    edges: list[Edge],
    summary: dict[str, object],
    analytics: dict[str, object],
    scores: dict[str, int],
    findings: list[Finding],
    html_report_path: str,
    parse_warnings: list[str] | None = None,
) -> None:
    """Render a Rich report: assets, edges, graph summary, scores, findings, footer.

    When *parse_warnings* is non-empty, shows a yellow panel listing skipped files.
    """
    console.print(f"[bold purple]Scanning {scan_root}...[/]")
    console.print(f"Found {discovered_file_count} data files")

    if parse_warnings:
        warn_text = "\n".join(f"• {w}" for w in parse_warnings[:25])
        if len(parse_warnings) > 25:
            warn_text += f"\n• … +{len(parse_warnings) - 25} more"
        console.print(
            Panel(
                warn_text,
                title="Parse warnings (skipped files)",
                border_style="yellow",
            )
        )

    asset_table = Table(
        title="Discovered Assets",
        expand=True,
        show_header=True,
        header_style="bold",
    )
    asset_table.add_column("Name", style="cyan", overflow="fold", min_width=24)
    asset_table.add_column("Type", style="green", min_width=8)
    asset_table.add_column("File", overflow="fold", min_width=28)
    asset_table.add_column("Columns", justify="right", min_width=7)
    for asset in assets:
        asset_table.add_row(
            asset.name,
            asset.asset_type.value,
            asset.file_path,
            str(len(asset.columns)),
        )
    console.print(asset_table)

    console.print(f"\n[bold]Edges (dependencies): {len(edges)}[/]")
    for edge in edges[:20]:
        console.print(f"  {edge.source} -> {edge.target}")

    console.print("\n[bold]Graph & scores[/]")
    graph_panel = Panel(
        (
            f"[cyan]Nodes[/] {summary.get('node_count', 0)}   "
            f"[cyan]Edges[/] {summary.get('edge_count', 0)}\n"
            f"[cyan]DAG[/] {summary.get('is_directed_acyclic', False)}   "
            f"[cyan]Orphans[/] {analytics.get('orphan_asset_count', 0)}   "
            f"[cyan]Cycles[/] {analytics.get('cycle_count', 0)}"
        ),
        title="Lineage graph",
        border_style="blue",
    )
    console.print(graph_panel)

    health = _overall_health_score(scores)
    color = _score_color(health)
    health_panel = Panel(
        Text(f"{health}/100", style=f"bold {color}", justify="center"),
        title="Overall health score",
        border_style=color,
    )
    console.print(health_panel)

    score_table = Table(title="Category scores", expand=True, show_header=True, header_style="bold")
    score_table.add_column("Category", style="cyan")
    score_table.add_column("Score", justify="right", style="bold")
    score_table.add_column("Progress", min_width=24)

    labels = {
        "dead_assets": "Dead asset score",
        "test_coverage": "Test coverage score",
        "documentation": "Documentation score",
        "complexity": "Complexity score",
        "ownership": "Ownership score",
        "contracts": "Contract compliance",
        "cost_hotspots": "Cost hotspots",
    }
    for key, label in labels.items():
        if key not in scores:
            continue
        value = int(scores[key])
        bar = ProgressBar(total=100, completed=value)
        style = _score_color(value if key != "complexity" else 100 - value)
        score_table.add_row(label, f"[{style}]{value}[/{style}]/100", bar)
    console.print(score_table)

    findings_panel_title = f"Top findings ({len(findings)} total)"
    if not findings:
        console.print(Panel("No findings.", title=findings_panel_title, border_style="green"))
    else:
        sev_rank = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
        top = sorted(findings, key=lambda f: sev_rank.get(f.severity, 9))[:12]
        lines = Text()
        for f in top:
            bullet = f"- [{f.severity.value.upper()}] {f.asset_name}: {f.message}\n"
            lines.append(bullet, style=_severity_color(f.severity))
        console.print(Panel(lines, title=findings_panel_title, border_style="blue"))

    footer = Text()
    footer.append("HTML report: ", style="bold")
    footer.append(html_report_path, style="cyan underline")
    console.print(Panel(footer, title="Footer", border_style="dim"))
