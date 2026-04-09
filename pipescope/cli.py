"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

import typer
from rich.console import Console

from pipescope import __version__
from pipescope.analyzers.complexity import analyze_complexity
from pipescope.analyzers.contracts import analyze_contract_compliance
from pipescope.analyzers.cost_hotspots import analyze_cost_hotspots
from pipescope.analyzers.dead_assets import (
    analyze_dead_assets,
    parse_dead_asset_terminal_tags_cli,
    parse_dead_asset_whitelist_cli,
)
from pipescope.analyzers.doc_coverage import analyze_documentation_coverage
from pipescope.analyzers.ownership import analyze_ownership
from pipescope.analyzers.test_coverage import analyze_test_coverage
from pipescope.graph import build_pipeline_graph, compute_scan_analytics, graph_summary
from pipescope.models import Asset, Edge
from pipescope.parsers import parse_dbt_project, parse_file, parse_odcs_file
from pipescope.parsers.odcs_parser import ParsedContract
from pipescope.reporters.html_report import write_report
from pipescope.reporters.json_report import format_scan_json
from pipescope.reporters.terminal import print_terminal_report
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


def _collect_dbt_project_roots(scan_root: Path, files: list[DiscoveredFile]) -> list[Path]:
    """Directory paths that contain ``dbt_project.yml`` (one dbt project each)."""
    roots: set[Path] = set()
    for f in files:
        if f.file_type == "dbt_project":
            roots.add(f.path.resolve().parent)
    if not roots:
        for yml in scan_root.rglob("dbt_project.yml"):
            if yml.is_file():
                roots.add(yml.resolve().parent)
    return sorted(roots)


def _path_under_any_project(path: Path, project_roots: set[Path]) -> bool:
    rp = path.resolve()
    for pr in project_roots:
        try:
            rp.relative_to(pr)
            return True
        except ValueError:
            continue
    return False


def _remap_asset_paths_to_scan_root(
    assets: list,
    project_root: Path,
    scan_root: Path,
) -> None:
    """Rewrite ``file_path`` from dbt-project-relative to scan-root-relative."""
    scan_root = scan_root.resolve()
    project_root = project_root.resolve()
    for asset in assets:
        rel = Path(asset.file_path)
        if rel.is_absolute():
            full = rel
        else:
            full = (project_root / rel).resolve()
        try:
            asset.file_path = str(full.relative_to(scan_root))
        except ValueError:
            asset.file_path = str(full)


def _dedupe_edges(edges: list[Edge]) -> list[Edge]:
    seen: set[tuple[str, str]] = set()
    out: list[Edge] = []
    for e in edges:
        k = (e.source, e.target)
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    return out


def collect_scan(
    root: Path,
    dialect: str | None,
) -> tuple[list[DiscoveredFile], list[Asset], list[Edge], list[ParsedContract]]:
    """Discover files and parse SQL, dbt projects, Airflow DAGs, Spark jobs, and ODCS contracts."""
    scan_root = root.resolve()
    files = scan_directory(scan_root)
    dbt_roots_list = _collect_dbt_project_roots(scan_root, files)
    dbt_roots = set(dbt_roots_list)

    all_assets: list[Asset] = []
    all_edges: list[Edge] = []
    parsed_contracts: list[ParsedContract] = []

    for project_root in dbt_roots_list:
        dbt_assets, dbt_edges = parse_dbt_project(project_root)
        _remap_asset_paths_to_scan_root(dbt_assets, project_root, scan_root)
        all_assets.extend(dbt_assets)
        all_edges.extend(dbt_edges)

    for f in files:
        if f.file_type == "dbt_model" and _path_under_any_project(f.path, dbt_roots):
            continue
        if f.file_type == "dbt_schema" and _path_under_any_project(f.path, dbt_roots):
            continue
        if f.file_type not in (
            "sql",
            "dbt_model",
            "dbt_schema",
            "airflow_dag",
            "spark_job",
        ):
            continue
        assets, edges = parse_file(f, dialect, scan_root=scan_root)
        all_assets.extend(assets)
        all_edges.extend(edges)

    for f in files:
        if f.file_type != "data_contract":
            continue
        try:
            content = f.path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        rel = _relative_file_path(f.path, scan_root)
        parsed_contracts.extend(parse_odcs_file(rel, content))

    all_edges = _dedupe_edges(all_edges)
    return files, all_assets, all_edges, parsed_contracts


def _parsed_sql_file_count(files: list[DiscoveredFile], dbt_roots: set[Path]) -> int:
    n = 0
    for f in files:
        if f.file_type not in ("sql", "dbt_model"):
            continue
        if f.file_type == "dbt_model" and _path_under_any_project(f.path, dbt_roots):
            continue
        n += 1
    return n


def _parsed_airflow_file_count(files: list[DiscoveredFile]) -> int:
    return sum(1 for f in files if f.file_type == "airflow_dag")


def _parsed_spark_file_count(files: list[DiscoveredFile]) -> int:
    return sum(1 for f in files if f.file_type == "spark_job")


def _parsed_dbt_project_count(dbt_roots: list[Path]) -> int:
    return len(dbt_roots)


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
    dead_asset_whitelist: str | None = typer.Option(
        None,
        "--dead-asset-whitelist",
        help="Comma-separated asset names excluded from dead-asset analysis.",
    ),
    dead_asset_terminal_tags: str | None = typer.Option(
        None,
        "--dead-asset-terminal-tags",
        help=(
            "Comma-separated substrings for intentional terminal sinks "
            "(tag keys/values; default: exposure,dashboard,export). "
            "Pass empty string to disable tag-based exclusion."
        ),
    ),
    test_coverage_critical_deps: int = typer.Option(
        10,
        "--test-coverage-critical-deps",
        help=(
            "Emit CRITICAL missing-test findings when downstream dependents exceed this "
            "(non-staging models only; default: 10)."
        ),
    ),
) -> None:
    """Scan a directory and analyze data pipeline health."""
    fmt = format_.strip().lower()
    if fmt not in ("terminal", "json"):
        raise typer.BadParameter("format must be 'terminal' or 'json'")
    if test_coverage_critical_deps < 0:
        raise typer.BadParameter("--test-coverage-critical-deps must be >= 0")

    root = Path(path).resolve()
    files, all_assets, all_edges, parsed_contracts = collect_scan(root, dialect)
    dbt_roots = set(_collect_dbt_project_roots(root, files))

    pg = build_pipeline_graph(all_assets, all_edges)
    summary = graph_summary(pg.g)
    analytics = compute_scan_analytics(pg, all_assets)
    dead_analysis = analyze_dead_assets(
        pg,
        all_assets,
        whitelist=parse_dead_asset_whitelist_cli(dead_asset_whitelist),
        terminal_tag_markers=parse_dead_asset_terminal_tags_cli(dead_asset_terminal_tags),
    )
    tc_analysis = analyze_test_coverage(
        pg,
        all_assets,
        critical_downstream_threshold=test_coverage_critical_deps,
    )
    doc_analysis = analyze_documentation_coverage(all_assets)
    cx_analysis = analyze_complexity(pg, all_assets, root, dialect)
    own_analysis = analyze_ownership(all_assets, root)
    cc_analysis = analyze_contract_compliance(pg, all_assets, parsed_contracts)
    ch_analysis = analyze_cost_hotspots(pg, all_assets, root, dialect)
    analytics["dead_asset_analysis"] = dead_analysis.to_analytics_dict()
    analytics["test_coverage"] = {
        "asset_count": tc_analysis.total_count,
        "assets_with_tests": tc_analysis.tested_count,
        "coverage_ratio": tc_analysis.coverage_ratio,
    }
    analytics["test_coverage_analysis"] = tc_analysis.to_analytics_dict()
    analytics["documentation_coverage"] = {
        "asset_count": doc_analysis.total_count,
        "documented_count": doc_analysis.documented_count,
        "coverage_ratio": doc_analysis.coverage_ratio,
    }
    analytics["documentation_coverage_analysis"] = doc_analysis.to_analytics_dict()
    analytics["complexity_analysis"] = cx_analysis.to_analytics_dict()
    analytics["ownership_analysis"] = own_analysis.to_analytics_dict()
    analytics["contract_compliance_analysis"] = cc_analysis.to_analytics_dict()
    analytics["cost_hotspot_analysis"] = ch_analysis.to_analytics_dict()
    findings = (
        list(dead_analysis.findings)
        + list(tc_analysis.findings)
        + list(doc_analysis.findings)
        + list(cx_analysis.findings)
        + list(own_analysis.findings)
        + list(cc_analysis.findings)
        + list(ch_analysis.findings)
    )
    scores = {
        "dead_assets": dead_analysis.score,
        "test_coverage": tc_analysis.score,
        "documentation": doc_analysis.score,
        "complexity": cx_analysis.pipeline_score,
        "ownership": own_analysis.score,
        "contracts": cc_analysis.score,
        "cost_hotspots": ch_analysis.score,
    }

    if fmt == "json":
        _ensure_utf8_stdio()
        payload = format_scan_json(
            version=__version__,
            scan_root=str(root),
            discovered_file_count=len(files),
            parsed_sql_file_count=_parsed_sql_file_count(files, dbt_roots),
            parsed_airflow_file_count=_parsed_airflow_file_count(files),
            parsed_spark_file_count=_parsed_spark_file_count(files),
            parsed_dbt_project_count=_parsed_dbt_project_count(sorted(dbt_roots)),
            assets=all_assets,
            edges=all_edges,
            graph=summary,
            analytics=analytics,
            findings=findings,
            scores=scores,
        )
        sys.stdout.write(payload)
        if not payload.endswith("\n"):
            sys.stdout.write("\n")
        return

    _ensure_utf8_stdio()
    console = _make_console()
    html_report_path = Path(tempfile.gettempdir()) / "pipescope-report.html"
    write_report(
        html_report_path,
        {
            "version": __version__,
            "scan_root": str(root),
            "graph": summary,
            "scores": scores,
            "analytics": analytics,
            "findings": [f.model_dump(mode="json") for f in findings],
        },
    )
    print_terminal_report(
        console,
        scan_root=str(root),
        discovered_file_count=len(files),
        assets=all_assets,
        edges=all_edges,
        summary=summary,
        analytics=analytics,
        scores=scores,
        findings=findings,
        html_report_path=str(html_report_path),
    )


if __name__ == "__main__":
    app()
