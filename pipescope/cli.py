"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from pipescope.parsers.sql_parser import parse_sql_file
from pipescope.scanner import scan_directory

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


@app.command()
def scan(
    path: str = typer.Argument(".", help="Path to scan."),
    dialect: str | None = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (snowflake, bigquery, postgres, etc.).",
    ),
) -> None:
    """Scan a directory and analyze data pipeline health."""
    _ensure_utf8_stdio()
    console = _make_console()

    root = Path(path).resolve()
    console.print(f"[bold purple]Scanning {root}...[/]")

    files = scan_directory(root)
    console.print(f"Found {len(files)} data files")

    all_assets: list = []
    all_edges: list = []
    for f in files:
        if f.file_type not in ("sql", "dbt_model"):
            continue
        content = f.path.read_text(encoding="utf-8", errors="ignore")
        display_path = _relative_file_path(f.path, root)
        assets, edges = parse_sql_file(display_path, content, dialect)
        all_assets.extend(assets)
        all_edges.extend(edges)

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


if __name__ == "__main__":
    app()
