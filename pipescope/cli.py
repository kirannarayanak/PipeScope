"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from pipescope import __version__
from pipescope.graph import build_graph, graph_summary
from pipescope.models import Asset, Edge
from pipescope.parsers.sql_parser import parse_sql_file
from pipescope.reporters.json_report import format_scan_json
from pipescope.scanner import DiscoveredFile, scan_directory

app = typer.Typer(
    name="pipescope",
    help="PipeScope: Universal static analyzer for data pipelines.",
    no_args_is_help=True,
)


def _ensure_utf8_stdio() -> None:
    """Prefer UTF-8 on Windows so Rich tables and paths print reliably."""
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8")
            except (OSError, ValueError, AttributeError):
                pass


def _make_console() -> Console:
    """Terminal-aware width: full width when interactive, wider default when captured."""
    width: int | None = None
    try:
        if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
            width = shutil.get_terminal_size().columns
        else:
            width = 120
    except OSError:
        width = 120
    return Console(
        width=width,
        legacy_windows=False,
    )


@app.callback()
def main() -> None:
    """PipeScope CLI."""


def _relative_file_path(file_path: Path, root: Path) -> str:
    try:
        return str(file_path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(file_path)


def collect_sql_scan(
    root: Path,
    dialect: str | None,
) -> tuple[list[DiscoveredFile], list[Asset], list[Edge]]:
    """Discover files and parse SQL / dbt model SQL."""
    files = scan_directory(root)
    all_assets: list[Asset] = []
    all_edges: list[Edge] = []
    for f in files:
        if f.file_type not in ("sql", "dbt_model"):
            continue
        content = f.path.read_text(encoding="utf-8", errors="ignore")
        display_path = _relative_file_path(f.path, root)
        assets, edges = parse_sql_file(display_path, content, dialect)
        all_assets.extend(assets)
        all_edges.extend(edges)
    return files, all_assets, all_edges


def _parsed_sql_file_count(files: list[DiscoveredFile]) -> int:
    return sum(1 for f in files if f.file_type in ("sql", "dbt_model"))


@app.command()
def scan(
    path: str = typer.Argument(".", help="Path to scan."),
    dialect: str | None = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (snowflake, bigquery, postgres, etc.).",
    ),
    format_: str = typer.Option(
        "terminal",
        "--format",
        "-f",
        help="Output: terminal (Rich) or json (machine-readable).",
    ),
) -> None:
    """Scan a directory and analyze data pipeline health."""
    fmt = format_.strip().lower()
    if fmt not in ("terminal", "json"):
        raise typer.BadParameter("format must be 'terminal' or 'json'")

    root = Path(path).resolve()
    files, all_assets, all_edges = collect_sql_scan(root, dialect)

    g = build_graph(all_assets, all_edges)
    summary = graph_summary(g)

    if fmt == "json":
        _ensure_utf8_stdio()
        payload = format_scan_json(
            version=__version__,
            scan_root=str(root),
            discovered_file_count=len(files),
            parsed_sql_file_count=_parsed_sql_file_count(files),
            assets=all_assets,
            edges=all_edges,
            graph=summary,
        )
        sys.stdout.write(payload)
        if not payload.endswith("\n"):
            sys.stdout.write("\n")
        return

    _ensure_utf8_stdio()
    console = _make_console()

    console.print(f"[bold purple]Scanning {root}...[/]")
    console.print(f"Found {len(files)} data files")

    table = Table(
        title="Discovered Assets",
        expand=True,
        show_header=True,
        header_style="bold",
    )
    table.add_column("Name", style="cyan", overflow="fold", min_width=24)
    table.add_column("Type", style="green", min_width=8)
    table.add_column("File", overflow="fold", min_width=28)
    table.add_column("Columns", justify="right", min_width=7)

    for asset in all_assets:
        table.add_row(
            asset.name,
            asset.asset_type.value,
            asset.file_path,
            str(len(asset.columns)),
        )
    console.print(table)

    console.print(f"\n[bold]Edges (dependencies): {len(all_edges)}[/]")
    for edge in all_edges[:20]:
        console.print(f"  {edge.source} -> {edge.target}")

    console.print(
        f"\n[bold]Graph:[/] {summary['node_count']} nodes, "
        f"{summary['edge_count']} edges, "
        f"DAG={summary['is_directed_acyclic']}",
    )


if __name__ == "__main__":
    app()
