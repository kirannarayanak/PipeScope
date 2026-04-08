"""Rich terminal output."""

from __future__ import annotations

from rich.console import Console

console = Console()


def print_banner(title: str) -> None:
    """Print a section header."""
    console.print(f"[bold]{title}[/bold]")
