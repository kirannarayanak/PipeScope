"""CLI smoke tests."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

from pipescope.cli import app

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _invoke_scan(
    runner: CliRunner,
    *extra: str,
    path: str | None = None,
) -> Result:
    args = ["scan", path or str(FIXTURES.resolve()), *extra]
    return runner.invoke(
        app,
        args,
        color=False,
        env={"PYTHONUTF8": "1"},
    )


def test_scan_prints_assets_and_edges_table(runner: CliRunner) -> None:
    result = _invoke_scan(runner, "--dialect", "postgres")
    assert result.exit_code == 0, result.stdout + result.stderr
    out = result.stdout
    assert "Scanning" in out
    assert "data files" in out
    assert "Discovered Assets" in out
    assert "Edges (dependencies):" in out
    assert "Graph:" in out
    assert "Analytics:" in out


def test_scan_reports_nonzero_edge_count_when_fixtures_have_sql(runner: CliRunner) -> None:
    result = _invoke_scan(runner, "--dialect", "postgres")
    assert result.exit_code == 0
    m = re.search(r"Edges \(dependencies\):\s*(\d+)", result.stdout)
    assert m is not None
    assert int(m.group(1)) >= 1


def test_scan_works_without_dialect_option(runner: CliRunner) -> None:
    result = _invoke_scan(runner)
    assert result.exit_code == 0
    assert "Discovered Assets" in result.stdout


def test_root_help_lists_scan_subcommand(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"], color=False)
    assert result.exit_code == 0
    assert "scan" in result.stdout.lower()


def test_scan_help_shows_path_and_dialect(runner: CliRunner) -> None:
    result = runner.invoke(app, ["scan", "--help"], color=False)
    assert result.exit_code == 0
    assert "dialect" in result.stdout.lower() or "--dialect" in result.stdout


def test_scan_format_json_is_valid_json(runner: CliRunner) -> None:
    result = _invoke_scan(runner, "--format", "json", "--dialect", "postgres")
    assert result.exit_code == 0, result.stderr
    data = json.loads(result.stdout)
    assert "version" in data
    assert "assets" in data and "edges" in data
    assert "graph" in data
    assert "analytics" in data
    assert data["graph"]["node_count"] >= 1
    assert "findings" in data and isinstance(data["findings"], list)
    assert "scores" in data and "dead_assets" in data["scores"]
    assert "dead_asset_analysis" in data["analytics"]
    tc = data["analytics"]["test_coverage"]
    assert "asset_count" in tc and "assets_with_tests" in tc and "coverage_ratio" in tc
    assert "Scanning" not in result.stdout


def test_scan_rejects_invalid_format(runner: CliRunner) -> None:
    result = _invoke_scan(runner, "--format", "xml")
    assert result.exit_code != 0


def test_scan_includes_airflow_dag_from_fixtures(runner: CliRunner) -> None:
    """tests/fixtures contains airflow_sample_dag.py classified as airflow_dag."""
    result = _invoke_scan(runner, "--dialect", "postgres")
    assert result.exit_code == 0
    out = result.stdout
    assert "sample_pipeline" in out
    assert "airflow_dag" in out or "airflow_task" in out


def test_scan_json_includes_airflow_parse_count(runner: CliRunner) -> None:
    result = _invoke_scan(runner, "--format", "json", "--dialect", "postgres")
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data.get("parsed_airflow_file_count", 0) >= 1
    assert data.get("parsed_spark_file_count", 0) >= 1
    assert data.get("parsed_dbt_project_count", 0) >= 1
    types = {a["asset_type"] for a in data["assets"]}
    assert "airflow_dag" in types or "airflow_task" in types
    assert "dbt_model" in types or "dbt_source" in types


def test_scan_dbt_sample_lineage_from_dbt_parser(runner: CliRunner) -> None:
    """dbt_sample: ref + source edges come from parse_dbt_project, not plain SQL."""
    dbt_root = str((FIXTURES / "dbt_sample").resolve())
    result = runner.invoke(
        app,
        ["scan", dbt_root, "--format", "json", "--dialect", "postgres"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0, result.stderr
    data = json.loads(result.stdout)
    assert data.get("parsed_dbt_project_count") == 1
    edges = {(e["source"], e["target"]) for e in data["edges"]}
    assert ("stg_events", "fct_sessions") in edges
    assert ("raw.events", "stg_events") in edges
