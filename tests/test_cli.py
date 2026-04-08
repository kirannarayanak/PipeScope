"""CLI smoke tests."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from pipescope.cli import app

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_scan_prints_assets_and_edges_table() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["scan", str(FIXTURES.resolve()), "--dialect", "postgres"],
        color=False,
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    out = result.stdout
    assert "Scanning" in out
    assert "data files" in out
    assert "Discovered Assets" in out
    assert "Edges (dependencies):" in out
