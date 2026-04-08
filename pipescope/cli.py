"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer(
    name="pipescope",
    help="Universal static analyzer for data pipelines.",
    no_args_is_help=True,
)
console = Console()


@app.callback()
def main() -> None:
    """PipeScope CLI."""


@app.command()
def scan(
    path: str = typer.Argument(".", help="Repository or directory to scan."),
    format_: str = typer.Option(
        "html",
        "--format",
        "-f",
        help="Output format: html, json, terminal.",
    ),
) -> None:
    """Run full analysis and generate a report."""
    console.print(f"[bold]pipescope scan[/bold] — path={path!r}, format={format_!r}")
    console.print("[dim]Implementation coming in Phase 1.[/dim]")


if __name__ == "__main__":
    app()
