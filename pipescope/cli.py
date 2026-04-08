"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

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
console = Console()


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

    table = Table(title="Discovered Assets")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("File")
    table.add_column("Columns", justify="right")

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
