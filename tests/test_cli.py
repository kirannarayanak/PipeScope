"""CLI smoke tests."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import pytest
from click.testing import Result
from typer.testing import CliRunner

import pipescope.cli as cli_mod
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
    assert "Graph & scores" in out
    assert "Lineage graph" in out
    assert "Dead asset score" in out
    assert "Test coverage score" in out
    assert "Documentation score" in out
    assert "Complexity score" in out
    assert "Ownership score" in out
    assert "Contract compliance" in out
    assert "Cost hotspots" in out


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
    result = runner.invoke(
        app,
        ["scan", "--help"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0
    assert "dialect" in result.stdout.lower() or "--dialect" in result.stdout
    assert "test-coverage-critical-deps" in result.stdout
    assert "--exclude" in result.stdout or "-e" in result.stdout
    assert "pipescope scan" in result.stdout and "json" in result.stdout.lower()


def test_diff_help_shows_exclude(runner: CliRunner) -> None:
    result = runner.invoke(app, ["diff", "--help"], color=False)
    assert result.exit_code == 0
    assert "exclude" in result.stdout.lower()


def test_ci_help_shows_exclude(runner: CliRunner) -> None:
    result = runner.invoke(app, ["ci", "--help"], color=False)
    assert result.exit_code == 0
    assert "exclude" in result.stdout.lower()


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
    assert "test_coverage" in data["scores"]
    assert "documentation" in data["scores"]
    assert "complexity" in data["scores"]
    assert "ownership" in data["scores"]
    assert "contracts" in data["scores"]
    assert "cost_hotspots" in data["scores"]
    assert "dead_asset_analysis" in data["analytics"]
    assert "ownership_analysis" in data["analytics"]
    assert "contract_compliance_analysis" in data["analytics"]
    assert "cost_hotspot_analysis" in data["analytics"]
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


def test_scan_writes_snapshot_file(runner: CliRunner, tmp_path: Path) -> None:
    src = FIXTURES / "scanner_sample"
    scan_root = tmp_path / "scan_root"
    shutil.copytree(src, scan_root)

    result = runner.invoke(
        app,
        ["scan", str(scan_root), "--format", "json", "--dialect", "postgres"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0, result.stderr

    day = datetime.now().strftime("%Y-%m-%d")
    snap = scan_root / ".pipescope" / "snapshots" / f"{day}.json"
    assert snap.is_file()
    snapshots_dir = scan_root / ".pipescope" / "snapshots"
    stamped = [
        p
        for p in snapshots_dir.glob("*.json")
        if p.name != f"{day}.json"
    ]
    assert stamped, "expected timestamped snapshot files in addition to daily snapshot"
    data = json.loads(snap.read_text(encoding="utf-8"))
    assert "scores" in data and "graph" in data and "findings" in data


def test_scan_prunes_old_timestamped_snapshots(runner: CliRunner, tmp_path: Path) -> None:
    src = FIXTURES / "scanner_sample"
    scan_root = tmp_path / "scan_root_prune"
    shutil.copytree(src, scan_root)
    snapshots_dir = scan_root / ".pipescope" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    old_file = snapshots_dir / "2000-01-01T000000_000000Z.json"
    old_file.write_text("{}", encoding="utf-8")

    result = runner.invoke(
        app,
        ["scan", str(scan_root), "--format", "json", "--dialect", "postgres"],
        color=False,
        env={
            "PYTHONUTF8": "1",
            "PIPESCOPE_SNAPSHOT_RETENTION_DAYS": "0",
        },
    )
    assert result.exit_code == 0, result.stderr
    assert not old_file.exists()


def test_scan_dead_asset_whitelist_cli(runner: CliRunner) -> None:
    """Whitelisting a sink removes it from dead-asset findings."""
    dbt_root = str((FIXTURES / "dbt_sample").resolve())
    result = runner.invoke(
        app,
        [
            "scan",
            dbt_root,
            "--format",
            "json",
            "--dead-asset-whitelist",
            "fct_sessions",
        ],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0, result.stderr
    data = json.loads(result.stdout)
    names = {f["asset_name"] for f in data["findings"] if f["category"] == "dead_asset"}
    assert "fct_sessions" not in names


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


def test_diff_command_outputs_clean_tables(runner: CliRunner, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    sql_file = repo / "model.sql"
    sql_file.write_text("CREATE TABLE a AS SELECT 1 AS id;", encoding="utf-8")
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    sql_file.write_text("CREATE TABLE a AS SELECT 2 AS id;", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "second",
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    sql_file.write_text("CREATE TABLE a AS SELECT * FROM t;", encoding="utf-8")

    result = runner.invoke(
        app,
        ["diff", "HEAD~1", "--path", str(repo), "--dialect", "postgres"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    out = result.stdout
    assert "Diff summary" in out
    assert "New findings" in out
    assert "Resolved findings" in out


def test_ci_command_emits_annotations_and_passes(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_mod, "_post_pr_comment_if_possible", lambda *_args: False)
    monkeypatch.setattr(cli_mod, "_overall_health_score", lambda _scores: 90)
    result = runner.invoke(
        app,
        ["ci", "--path", str(FIXTURES.resolve()), "--threshold", "70", "--dialect", "postgres"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    out = result.stdout
    assert "PipeScope overall score: 90" in out
    assert "::warning title=PipeScope::" in out or "::notice title=PipeScope::" in out


def test_ci_command_fails_build_below_threshold(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_mod, "_post_pr_comment_if_possible", lambda *_args: False)
    monkeypatch.setattr(cli_mod, "_overall_health_score", lambda _scores: 10)
    result = runner.invoke(
        app,
        ["ci", "--path", str(FIXTURES.resolve()), "--threshold", "70", "--dialect", "postgres"],
        color=False,
        env={"PYTHONUTF8": "1"},
    )
    assert result.exit_code == 1
